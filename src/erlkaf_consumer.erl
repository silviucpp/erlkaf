-module(erlkaf_consumer).

-include("erlkaf.hrl").
-include("erlkaf_private.hrl").

-define(POLL_IDLE_MS, 1000).
-define(DEFAULT_BATCH_SIZE, 100).

-behaviour(gen_server).

-export([
    start_link/8,
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
    dispatch_mode,
    messages = [],
    backoff = 0,
    last_offset = -1
}).

start_link(ClientRef, TopicName, DispatchMode, Partition, Offset, QueueRef, CbModule, CbArgs) ->
    gen_server:start_link(?MODULE, [ClientRef, TopicName, DispatchMode, Partition, Offset, QueueRef, CbModule, CbArgs], []).

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

init([ClientRef, TopicName, DispatchMode, Partition, Offset, QueueRef, CbModule, CbArgs]) ->
    ?LOG_INFO("start consumer for: ~p partition: ~p offset: ~p", [TopicName, Partition, Offset]),

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
                dispatch_mode = DpMode
            }};
        Error ->
            {stop, Error}
    end.

handle_call(Request, _From, State) ->
    ?LOG_ERROR("handle_call unexpected message: ~p", [Request]),
    {reply, ok, State}.

handle_cast(Request, State) ->
    ?LOG_ERROR("handle_cast unexpected message: ~p", [Request]),
    {noreply, State}.

handle_info(poll_events, #state{queue_ref = Queue, poll_batch_size = PollBatchSize} = State) ->
    case erlkaf_nif:consumer_queue_poll(Queue, PollBatchSize) of
        {ok, Events, LastOffset} ->
            case Events of
                [] ->
                    schedule_poll(?POLL_IDLE_MS),
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
    cb_module = CbModule,
    cb_state = CbState} = State) ->

    case process_events(DispatchMode, Msgs, batch_offset(DispatchMode, State), ClientRef, CbModule, CbState) of
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

process_events(one_by_one, Msgs, _LastBatchOffset, ClientRef, CbModule, CbState) ->
    process_events_one_by_one(Msgs, ClientRef, CbModule, CbState);
process_events(batch, Msgs, LastBatchOffset, ClientRef, CbModule, CbState) ->
    process_events_batch(Msgs, LastBatchOffset, ClientRef, CbModule, CbState).

process_events_batch(Msgs, LastBatchOffset, ClientRef, CbModule, #state{backoff = Backoff} = CbState) ->
    case catch CbModule:handle_message(Msgs, CbState) of
        {ok, NewCbState} ->
            {Topic, Partition, Offset} = LastBatchOffset,
            ok = erlkaf_nif:consumer_offset_store(ClientRef, Topic, Partition, Offset),
            {ok, NewCbState#state{backoff = 0}};
        {error, Reason, NewCbState} ->
            ?LOG_ERROR("~p:handle_message for batch error: ~p", [CbModule, Reason]),
            case recv_stop() of
                false ->
                    process_events_batch(Msgs, LastBatchOffset, ClientRef, CbModule, NewCbState#state{backoff = exponential_backoff(Backoff)});
                StopMsg ->
                    StopMsg
            end;
        Error ->
            ?LOG_ERROR("~p:handle_message for batch error: ~p", [CbModule, Error]),
            case recv_stop() of
                false ->
                    process_events_batch(Msgs, LastBatchOffset, ClientRef, CbModule, CbState#state{backoff = exponential_backoff(Backoff)});
                StopMsg ->
                    StopMsg
            end
    end.

process_events_one_by_one([H|T] = Msgs, ClientRef, CbModule, #state{backoff = Backoff} = CbState) ->
    case recv_stop() of
        false ->
            case catch CbModule:handle_message(H, CbState) of
                {ok, NewCbState} ->
                    ok = commit_offset(ClientRef, H),
                    process_events_one_by_one(T, ClientRef, CbModule, NewCbState#state{backoff = 0});
                {error, Reason, NewCbState} ->
                    ?LOG_ERROR("~p:handle_message for: ~p error: ~p", [CbModule, H, Reason]),
                    process_events_one_by_one(Msgs, ClientRef, CbModule, NewCbState#state{backoff = exponential_backoff(Backoff)});
                Error ->
                    ?LOG_ERROR("~p:handle_message for: ~p error: ~p", [CbModule, H, Error]),
                    process_events_one_by_one(Msgs, ClientRef, CbModule, CbState#state{backoff = exponential_backoff(Backoff)})
            end;
        StopMsg ->
            StopMsg
    end;
process_events_one_by_one([], _ClientRef, _CbModule, CbState) ->
    {ok, CbState}.

recv_stop() ->
    receive {stop, _From, _Tag} = Msg -> Msg after 0 -> false end.

handle_stop(From, Tag, #state{topic_name = TopicName, partition = Partition, queue_ref = Queue}) ->
    ?LOG_INFO("stop consumer for: ~p partition: ~p", [TopicName, Partition]),
    ok = erlkaf_nif:consumer_queue_cleanup(Queue),
    From ! {stopped, Tag}.

exponential_backoff(0) ->
    1000;
exponential_backoff(4000) ->
    timer:sleep(4000),
    4000;
exponential_backoff(Backoff) ->
    timer:sleep(Backoff),
    Backoff * 2.
