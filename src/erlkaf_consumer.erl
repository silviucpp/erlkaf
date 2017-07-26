-module(erlkaf_consumer).

-include("erlkaf.hrl").
-include("erlkaf_private.hrl").

-define(POLL_IDLE_MS, 1000).

-behaviour(gen_server).

-export([start_link/7, stop/1, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    client_ref,
    topic_name,
    partition,
    queue_ref,
    cb_module,
    cb_state,
    messages = []
}).

start_link(ClientRef, TopicName, Partition, Offset, QueueRef, CbModule, CbArgs) ->
    gen_server:start_link(?MODULE, [ClientRef, TopicName, Partition, Offset, QueueRef, CbModule, CbArgs], []).

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

init([ClientRef, TopicName, Partition, Offset, QueueRef, CbModule, CbArgs]) ->
    ?INFO_MSG("start consumer for: ~p partition: ~p offset: ~p", [TopicName, Partition, Offset]),

    case catch CbModule:init(TopicName, Partition, Offset, CbArgs) of
        {ok, CbState} ->
            schedule_poll(0),
            {ok, #state{client_ref = ClientRef, topic_name = TopicName, partition = Partition, queue_ref = QueueRef, cb_module = CbModule, cb_state = CbState}};
        Error ->
            {stop, Error}
    end.

handle_call(Request, _From, State) ->
    ?ERROR_MSG("handle_call unexpected message: ~p", [Request]),
    {reply, ok, State}.

handle_cast(Request, State) ->
    ?ERROR_MSG("handle_cast unexpected message: ~p", [Request]),
    {noreply, State}.

handle_info(poll_events, #state{queue_ref = Queue} = State) ->
    case erlkaf_nif:consumer_queue_poll(Queue) of
        {ok, Events} ->
            case Events of
                [] ->
                    schedule_poll(?POLL_IDLE_MS),
                    {noreply, State};
                _ ->
                    schedule_message_process(0),
                    {noreply, State#state{messages = Events}}
            end;
        Error ->
            ?INFO_MSG("~p poll events error: ~p", [?MODULE, Error]),
            throw({error, Error})
    end;

handle_info(process_messages, #state{messages = Msg, client_ref = ClientRef, cb_module = CbModule, cb_state = CbState} = State) ->
    case process_events(Msg, ClientRef, CbModule, CbState) of
        {ok, NewCbState} ->
            schedule_poll(0),
            {noreply, State#state{messages = [], cb_state = NewCbState}};
        {stop, From, Tag} ->
            handle_stop(From, Tag, State),
            {stop, normal, State};
        Error ->
            ?ERROR_MSG("unexpected response: ~p", [Error]),
            {stop, Error, State}
    end;

handle_info({stop, From, Tag}, State) ->
    handle_stop(From, Tag, State),
    {stop, normal, State};

handle_info(Info, State) ->
    ?ERROR_MSG("handle_info unexpected message: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%internals

schedule_poll(Timeout) ->
    %?INFO_MSG("schedule_poll:~p timeout: ~p", [self(), Timeout]),
    erlang:send_after(Timeout, self(), poll_events).

schedule_message_process(Timeout) ->
    %?INFO_MSG("schedule_message_process:~p timeout: ~p", [self(), Timeout]),
    erlang:send_after(Timeout, self(), process_messages).

process_events([H|T] = Msgs, ClientRef, CbModule, CbState) ->
    case recv_stop() of
        false ->
            case catch CbModule:handle_message(H, CbState) of
                {ok, NewCbState} ->
                    ok = commit_offset(ClientRef, H),
                    process_events(T, ClientRef, CbModule, NewCbState);
                Error ->
                    ?ERROR_MSG("~p:handle_message for: ~p error: ~p", [CbModule, H, Error]),
                    process_events(Msgs, ClientRef, CbModule, CbState)
            end;
        StopMsg ->
            StopMsg
    end;
process_events([], _ClientRef, _CbModule, CbState) ->
    {ok, CbState}.

recv_stop() ->
    receive {stop, _From, _Tag} = Msg -> Msg after 0 -> false end.

handle_stop(From, Tag, #state{topic_name = TopicName, partition = Partition, queue_ref = Queue}) ->
    ?INFO_MSG("stop consumer for: ~p partition: ~p", [TopicName, Partition]),
    ok = erlkaf_nif:consumer_queue_cleanup(Queue),
    From ! {stopped, Tag}.

commit_offset(ClientRef, #erlkaf_msg{topic = Topic, partition = Partition, offset = Offset}) ->
    erlkaf_nif:consumer_offset_store(ClientRef, Topic, Partition, Offset).