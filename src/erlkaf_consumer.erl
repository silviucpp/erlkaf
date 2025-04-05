-module(erlkaf_consumer).

-include("erlkaf.hrl").
-include("erlkaf_private.hrl").

-define(DEFAULT_POLL_IDLE_MS, 1000).
-define(DEFAULT_BATCH_SIZE, 100).

-behaviour(gen_statem).

-export([
    %API
    start_link/6,
    stop/1,
    pause/1,
    resume/1,


    % gen_statem 
    init/1,
    terminate/2,
    code_change/3,
    callback_mode/0,

    %states
    polling/3,
    paused/3
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
    dispatch_mode,
    last_offset = -1
}).
callback_mode() -> [state_functions, state_enter].
start_link(ClientRef, TopicName, Partition, Offset, QueueRef, TopicSettings) ->
    gen_statem:start_link(?MODULE, [ClientRef, TopicName, Partition, Offset, QueueRef, TopicSettings], []).

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

pause(Pid) ->
    gen_statem:call(Pid, pause).
resume(Pid) ->
    gen_statem:call(Pid, resume).

init([ClientRef, TopicName, Partition, Offset, QueueRef, TopicSettings]) ->
    ?LOG_INFO("start consumer for: ~p partition: ~p offset: ~p", [TopicName, Partition, Offset]),

    CbModule = erlkaf_utils:lookup(callback_module, TopicSettings),
    CbArgs = erlkaf_utils:lookup(callback_args, TopicSettings, []),
    DispatchMode = erlkaf_utils:lookup(dispatch_mode, TopicSettings, one_by_one),
    PollIdleMs = erlkaf_utils:lookup(poll_idle_ms, TopicSettings, ?DEFAULT_POLL_IDLE_MS),

    case catch CbModule:init(TopicName, Partition, Offset, CbArgs) of
        {ok, CbState} ->
            {DpMode, PollBatchSize} = dispatch_mode_parse(DispatchMode),
            %once we start up go to the polling state 
            {ok, polling, #state{
                client_ref = ClientRef,
                topic_name = TopicName,
                partition = Partition,
                queue_ref = QueueRef,
                cb_module = CbModule,
                cb_state = CbState,
                poll_batch_size = PollBatchSize,
                poll_idle_ms = PollIdleMs,
                dispatch_mode = DpMode
            }};
        Error ->
            ?LOG_ERROR("~p:init for topic: ~p failed with: ~p", [CbModule, TopicName, Error]),
            {stop, Error}
    end.


%Polling state
polling(enter, _OldState, #state{topic_name = TopicName, 
                                 partition = Partition}) -> 
    ?LOG_INFO("starting polling for: topic ~p, ~p partition", [TopicName, Partition]),
    {keep_state_and_data, {state_timeout, 0, poll_events}}; 
polling(state_timeout, poll_events, #state{queue_ref = Queue, 
                                            poll_batch_size = PollBatchSize, 
                                            poll_idle_ms = PollIdleMs} = State) ->
    case erlkaf_nif:consumer_queue_poll(Queue, PollBatchSize) of
        {ok, Events, LastOffset} ->
            case Events of
                [] ->
                    {keep_state_and_data, {state_timeout, PollIdleMs, poll_events}};
                _ ->
                    process(Events, State#state{last_offset=LastOffset})
            end;
        Error ->
            ?LOG_INFO("~p poll events error: ~p", [?MODULE, Error]),
            throw({error, Error})
    end;
%Calls    
polling({call, From}, pause, Data) ->
    {next_state, paused, Data, {reply, From, ok}};
polling({call, From}, resume, _Data) ->
    {keep_state_and_data, {reply, From, ok}} ; %Dont need resume when we are already polling 
polling({call, _From}, Request, _Data) ->
    ?LOG_ERROR("unexpected call message: ~p, in ~p state", [Request, ?FUNCTION_NAME]),
    keep_state_and_data;    
%Casts
polling(cast, Request, _Data) ->
    ?LOG_ERROR("unexpected call message: ~p, in ~p state", [Request, ?FUNCTION_NAME]),
    keep_state_and_data;   
%Infos
polling(info, {stop, From, Tag}, Data) ->
    handle_stop(From, Tag, Data),
    {stop, normal, Data};
polling(info, Request, _Data) ->
    ?LOG_ERROR("unexpected info message: ~p, in ~p state", [Request, ?FUNCTION_NAME]),
    keep_state_and_data.  

%Pasued State
paused(enter, _OldState, #state{topic_name = TopicName, 
                                 partition = Partition}) -> 
    ?LOG_INFO("pasuing polling for: topic ~p, ~p partition", [TopicName, Partition]),
    keep_state_and_data;
%Calls
paused({call, From}, resume, Data) ->
    {next_state, polling, Data, {reply, From, ok}};
paused({call, From}, pause, _Data) ->
    {keep_state_and_data,  {reply, From, ok}}; %Dont need pause when we are already pasued 
paused({call, _From}, Request, _Data) ->
    ?LOG_ERROR("unexpected call message: ~p, in ~p state", [Request, ?FUNCTION_NAME]),
    keep_state_and_data;  
%Casts  
paused(cast, Request, _Data) ->
    ?LOG_ERROR("unexpected call message: ~p, in ~p state", [Request, ?FUNCTION_NAME]),
    keep_state_and_data;   
%Infos
paused(info, {stop, From, Tag}, Data) ->
    handle_stop(From, Tag, Data),
    {stop, normal, Data};
paused(info, Request, _Data) ->
    ?LOG_ERROR("unexpected info message: ~p, in ~p state", [Request, ?FUNCTION_NAME]),
    keep_state_and_data.

process(Events, #state{dispatch_mode = DispatchMode,
                     client_ref = ClientRef,
                     cb_module = CbModule,
                     cb_state = CbState} = State) ->

    case process_events(DispatchMode, Events, batch_offset(DispatchMode, State), ClientRef, CbModule, CbState) of
        {ok, NewCbState} ->
            {keep_state, State#state{last_offset = -1, cb_state = NewCbState}, {state_timeout, 0, poll_events}};
        {stop, From, Tag} ->
            handle_stop(From, Tag, State),
            {stop, normal, State};
        Error ->
            ?LOG_ERROR("unexpected response: ~p", [Error]),
            {stop, Error, State}
    end.

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

commit_offset(ClientRef, #erlkaf_msg{topic = Topic, partition = Partition, offset = Offset}) ->
    erlkaf_nif:consumer_offset_store(ClientRef, Topic, Partition, Offset).

process_events(one_by_one, Msgs, _LastBatchOffset, ClientRef, CbModule, CbState) ->
    process_events_one_by_one(Msgs, ClientRef, 0, CbModule, CbState);
process_events(batch, Msgs, LastBatchOffset, ClientRef, CbModule, CbState) ->
    process_events_batch(Msgs, LastBatchOffset, ClientRef, 0, CbModule, CbState).

process_events_batch(Msgs, LastBatchOffset, ClientRef, Backoff, CbModule, CbState) ->
    case catch CbModule:handle_message(Msgs, CbState) of
        {ok, NewCbState} ->
            {Topic, Partition, Offset} = LastBatchOffset,
            ok = erlkaf_nif:consumer_offset_store(ClientRef, Topic, Partition, Offset),
            {ok, NewCbState};
        {error, Reason, NewCbState} ->
            ?LOG_ERROR("~p:handle_message for batch error: ~p", [CbModule, Reason]),
            case recv_stop() of
                false ->
                    process_events_batch(Msgs, LastBatchOffset, ClientRef, exponential_backoff(Backoff), CbModule, NewCbState);
                StopMsg ->
                    StopMsg
            end;
        Error ->
            ?LOG_ERROR("~p:handle_message for batch error: ~p", [CbModule, Error]),
            case recv_stop() of
                false ->
                    process_events_batch(Msgs, LastBatchOffset, ClientRef, exponential_backoff(Backoff), CbModule, CbState);
                StopMsg ->
                    StopMsg
            end
    end.

process_events_one_by_one([H|T] = Msgs, ClientRef, Backoff, CbModule, CbState) ->
    case recv_stop() of
        false ->
            case catch CbModule:handle_message(H, CbState) of
                {ok, NewCbState} ->
                    ok = commit_offset(ClientRef, H),
                    process_events_one_by_one(T, ClientRef, 0, CbModule, NewCbState);
                {error, Reason, NewCbState} ->
                    ?LOG_ERROR("~p:handle_message for: ~p error: ~p", [CbModule, H, Reason]),
                    process_events_one_by_one(Msgs, ClientRef, exponential_backoff(Backoff), CbModule, NewCbState);
                Error ->
                    ?LOG_ERROR("~p:handle_message for: ~p error: ~p", [CbModule, H, Error]),
                    process_events_one_by_one(Msgs, ClientRef, exponential_backoff(Backoff), CbModule, CbState)
            end;
        StopMsg ->
            StopMsg
    end;
process_events_one_by_one([], _ClientRef, _Backoff, _CbModule, CbState) ->
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
