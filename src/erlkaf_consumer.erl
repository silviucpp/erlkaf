-module(erlkaf_consumer).

-include("erlkaf.hrl").
-include("erlkaf_private.hrl").

-define(DEFAULT_POLL_IDLE_MS, 1000).
-define(DEFAULT_BATCH_SIZE, 100).

-behaviour(gen_server).

-export([
    start_link/6,
    stop/1,

    % gen_server

    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    client_ref,
    topic_name,
    partition,
    queue_ref,
    cb_module,
    cb_state,
    poll_batch_size,
    poll_idle_ms,
    max_retries,
    dispatch_mode,
    messages = [],
    last_offset = -1
}).

start_link(ClientRef, TopicName, Partition, Offset, QueueRef, TopicSettings) ->
    gen_server:start_link(?MODULE, [ClientRef, TopicName, Partition, Offset, QueueRef, TopicSettings], []).

stop(Pid) ->
    case erlang:is_process_alive(Pid) of
        true ->
            Tag = make_ref(),
            Pid ! {stop, self(), Tag},

            receive
                {stopped, Tag} ->
                    ok
            after 5000 ->
                exit(Pid, kill)
            end;
        _ ->
            {error, not_alive}
    end.

init([ClientRef, TopicName, Partition, Offset, QueueRef, TopicSettings]) ->
    ?LOG_INFO("start consumer for: ~p partition: ~p offset: ~p", [TopicName, Partition, Offset]),

    CbModule = erlkaf_utils:lookup(callback_module, TopicSettings),
    CbArgs = erlkaf_utils:lookup(callback_args, TopicSettings, []),
    DispatchMode = erlkaf_utils:lookup(dispatch_mode, TopicSettings, one_by_one),
    PollIdleMs = erlkaf_utils:lookup(poll_idle_ms, TopicSettings, ?DEFAULT_POLL_IDLE_MS),
    MaxRetires = erlkaf_utils:lookup(max_retries, TopicSettings, infinity),

    case catch CbModule:init(TopicName, Partition, Offset, CbArgs) of
        {ok, CbState} ->
            schedule_poll(0),

            {DpMode, PollBatchSize} = dispatch_mode_parse(DispatchMode),

            {ok, #state{
                client_ref = ClientRef,
                topic_name = TopicName,
                partition = Partition,
                queue_ref = QueueRef,
                cb_module = CbModule,
                cb_state = CbState,
                poll_batch_size = PollBatchSize,
                poll_idle_ms = PollIdleMs,
                max_retries = MaxRetires,
                dispatch_mode = DpMode
            }};
        Error ->
            ?LOG_ERROR("~p:init for topic: ~p failed with: ~p", [CbModule, TopicName, Error]),
            {stop, Error}
    end.

handle_call(Request, _From, State) ->
    ?LOG_ERROR("handle_call unexpected message: ~p", [Request]),
    {reply, ok, State}.

handle_cast(Request, State) ->
    ?LOG_ERROR("handle_cast unexpected message: ~p", [Request]),
    {noreply, State}.

handle_info(poll_events, #state{queue_ref = Queue, poll_batch_size = PollBatchSize, poll_idle_ms = PollIdleMs} = State) ->
    case erlkaf_nif:consumer_queue_poll(Queue, PollBatchSize) of
        {ok, Events, LastOffset} ->
            case Events of
                [] ->
                    schedule_poll(PollIdleMs),
                    {noreply, State};
                _ ->
                    schedule_message_process(0),
                    {noreply, State#state{messages = Events, last_offset = LastOffset}}
            end;
        Error ->
            ?LOG_INFO("~p poll events error: ~p", [?MODULE, Error]),
            throw({error, Error})
    end;

handle_info(process_messages, #state{
    messages = Msgs,
    dispatch_mode = DispatchMode,
    client_ref = ClientRef,
    max_retries = MaxRetries,
    cb_module = CbModule,
    cb_state = CbState} = State) ->

    case process_events(DispatchMode, Msgs, batch_offset(DispatchMode, State), ClientRef, CbModule, CbState, MaxRetries) of
        {ok, NewCbState} ->
            schedule_poll(0),
            {noreply, State#state{messages = [], last_offset = -1, cb_state = NewCbState}};
        {stop, From, Tag} ->
            handle_stop(From, Tag, State),
            {stop, normal, State};
        Error ->
            ?LOG_ERROR("unexpected response: ~p", [Error]),
            {stop, Error, State}
    end;

handle_info({stop, From, Tag}, State) ->
    handle_stop(From, Tag, State),
    {stop, normal, State};

handle_info(Info, State) ->
    ?LOG_ERROR("handle_info unexpected message: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%internals

batch_offset(batch, #state{topic_name = T, partition = P, last_offset = O}) ->
    {T, P, O};
batch_offset(_, _) ->
    null.

dispatch_mode_parse(one_by_one) ->
    {one_by_one, ?DEFAULT_BATCH_SIZE};
dispatch_mode_parse({batch, MaxBatchSize}) ->
    {batch, MaxBatchSize}.

schedule_poll(Timeout) ->
    erlang:send_after(Timeout, self(), poll_events).

schedule_message_process(Timeout) ->
    erlang:send_after(Timeout, self(), process_messages).

commit_offset(ClientRef, #erlkaf_msg{topic = Topic, partition = Partition, offset = Offset}) ->
    erlkaf_nif:consumer_offset_store(ClientRef, Topic, Partition, Offset).

process_events(one_by_one, Msgs, _LastBatchOffset, ClientRef, CbModule, CbState, MaxRetries) ->
    process_events_one_by_one(Msgs, ClientRef, 0, CbModule, CbState, MaxRetries, MaxRetries);
process_events(batch, Msgs, LastBatchOffset, ClientRef, CbModule, CbState, MaxRetries) ->
    process_events_batch(Msgs, LastBatchOffset, ClientRef, 0, CbModule, CbState, MaxRetries).

process_events_batch(Msgs, LastBatchOffset, ClientRef, Backoff, CbModule, CbState, MaxRetries) ->
    case catch CbModule:handle_message(Msgs, CbState) of
        {ok, NewCbState} ->
            {Topic, Partition, Offset} = LastBatchOffset,
            ok = erlkaf_nif:consumer_offset_store(ClientRef, Topic, Partition, Offset),
            {ok, NewCbState};
        Error ->
            {Reason, NewCbState} = new_error_state(Error, CbState),
            ?LOG_ERROR("~p:handle_message for batch error: ~p", [CbModule, Reason]),
            case recv_stop() of
                false ->
                    case MaxRetries == infinity orelse MaxRetries > 0 of
                        true ->
                            process_events_batch(Msgs, LastBatchOffset, ClientRef, exponential_backoff(Backoff), CbModule, NewCbState, max_retry(MaxRetries));
                        _ ->
                            case handle_failed_message(CbModule, NewCbState, Msgs, 0, MaxRetries) of
                                {ok, NewCbState2} ->
                                    {Topic, Partition, Offset} = LastBatchOffset,
                                    ok = erlkaf_nif:consumer_offset_store(ClientRef, Topic, Partition, Offset),
                                    {ok, NewCbState2};
                                Other ->
                                    ?LOG_ERROR("~p:handle_failed_message for batch failed with: ~p", [CbModule, Error]),
                                    Other
                            end
                    end;
                StopMsg ->
                    StopMsg
            end
    end.

process_events_one_by_one([H|T] = Msgs, ClientRef, Backoff, CbModule, CbState, MaxRetries, CurrentMaxRetries) ->
    case recv_stop() of
        false ->
            case catch CbModule:handle_message(H, CbState) of
                {ok, NewCbState} ->
                    ok = commit_offset(ClientRef, H),
                    process_events_one_by_one(T, ClientRef, 0, CbModule, NewCbState, MaxRetries, MaxRetries);
                Error ->
                    {Reason, NewCbState} = new_error_state(Error, CbState),
                    ?LOG_ERROR("~p:handle_message for: ~p error: ~p", [CbModule, H, Reason]),

                    case CurrentMaxRetries == infinity orelse CurrentMaxRetries > 0 of
                        true ->
                            process_events_one_by_one(Msgs, ClientRef, exponential_backoff(Backoff), CbModule, NewCbState, MaxRetries, max_retry(CurrentMaxRetries));
                        _ ->
                            case handle_failed_message(CbModule, NewCbState, H, 0, MaxRetries) of
                                {ok, NewCbState2} ->
                                    ok = commit_offset(ClientRef, H),
                                    process_events_one_by_one(T, ClientRef, 0, CbModule, NewCbState2, MaxRetries, MaxRetries);
                                Other ->
                                    ?LOG_ERROR("~p:handle_failed_message for batch failed with: ~p", [CbModule, Error]),
                                    Other
                            end
                    end
            end;
        StopMsg ->
            StopMsg
    end;
process_events_one_by_one([], _ClientRef, _Backoff, _CbModule, CbState, _MaxRetries, _CurrentMaxRetries) ->
    {ok, CbState}.

recv_stop() ->
    receive {stop, _From, _Tag} = Msg -> Msg after 0 -> false end.

handle_stop(From, Tag, #state{topic_name = TopicName, partition = Partition}) ->
    ?LOG_INFO("stop consumer for: ~p partition: ~p", [TopicName, Partition]),
    From ! {stopped, Tag}.

exponential_backoff(0) ->
    500;
exponential_backoff(4000) ->
    timer:sleep(4000),
    4000;
exponential_backoff(Backoff) ->
    timer:sleep(Backoff),
    Backoff * 2.

new_error_state({error, Reason, NewCbState}, _State) ->
    {Reason, NewCbState};
new_error_state(Error, State) ->
    {Error, State}.

max_retry(infinity) ->
    infinity;
max_retry(N) ->
    N-1.

handle_failed_message(CbModule, CbState, Message, Backoff, MaxRetry) ->
    case erlang:function_exported(CbModule, handle_failed_message, 2) of
        true ->
            case catch CbModule:handle_failed_message(Message, CbState) of
                {ok, _NewCbState} = R ->
                    R;
                Error ->
                    case recv_stop() of
                        false ->
                            case MaxRetry == infinity orelse MaxRetry > 0 of
                                true ->
                                    handle_failed_message(CbModule, CbState, Message, exponential_backoff(Backoff), max_retry(MaxRetry));
                                _ ->
                                    Error
                            end;
                        StopMsg ->
                            StopMsg
                    end
            end;
        false ->
            {ok, CbState}
    end.
