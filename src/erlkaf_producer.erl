-module(erlkaf_producer).

-include("erlkaf_private.hrl").
-include("erlkaf.hrl").

-define(MAX_QUEUE_PROCESS_MSG, 5000).

-behaviour(gen_server).

-export([
    % api

    start_link/4,
    queue_event/5,

    % gen_server

    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    client_id,
    ref,
    dr_cb,
    stats_cb,
    stats = [],
    overflow_method,
    pqueue,
    pqueue_sch = true
}).

start_link(ClientId, DrCallback, ErlkafConfig, ProducerRef) ->
    gen_server:start_link(?MODULE, [ClientId, DrCallback, ErlkafConfig, ProducerRef], []).

queue_event(Pid, TopicName, Partition, Key, Value) ->
    erlkaf_utils:safe_call(Pid, {queue_event, TopicName, Partition, Key, Value}).

init([ClientId, DrCallback, ErlkafConfig, ProducerRef]) ->
    Pid = self(),
    OverflowStrategy = erlkaf_utils:lookup(queue_buffering_overflow_strategy, ErlkafConfig, local_disk_queue),
    StatsCallback =  erlkaf_utils:lookup(stats_callback, ErlkafConfig),
    ok = erlkaf_nif:producer_set_owner(ProducerRef, Pid),
    ok = erlkaf_cache_client:set(ClientId, ProducerRef, Pid),
    {ok, Queue} = erlkaf_local_queue:new(ClientId),
    process_flag(trap_exit, true),

    case OverflowStrategy of
        local_disk_queue ->
            schedule_consume_queue(0);
        _ ->
            ok
    end,

    {ok, #state{client_id = ClientId, ref = ProducerRef, dr_cb = DrCallback, stats_cb = StatsCallback, overflow_method = OverflowStrategy, pqueue = Queue}}.

handle_call({queue_event, TopicName, Partition, Key, Value}, _From, State) ->
    #state{pqueue = Queue, pqueue_sch = QueueScheduled, overflow_method = OverflowMethod} = State,

    case OverflowMethod of
        local_disk_queue ->
            schedule_consume_queue(QueueScheduled, 1000),
            ok = erlkaf_local_queue:enq(Queue, TopicName, Partition, Key, Value),
            {reply, ok, State#state{pqueue_sch = true}};
        _ ->
            {reply, OverflowMethod, State}
    end;

handle_call(get_stats, _From, #state{stats = Stats} = State) ->
    {reply, {ok, Stats}, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(consume_queue, #state{ref = ClientRef, pqueue = Queue} = State) ->
    case consume_queue(ClientRef, Queue, ?MAX_QUEUE_PROCESS_MSG) of
        completed ->
            {noreply, State#state{pqueue_sch = false}};
        ok ->
            schedule_consume_queue(1000),
            {noreply, State#state{pqueue_sch = true}}
    end;

handle_info({delivery_report, DeliveryStatus, Message}, #state{dr_cb = Callback} = State) ->
    case catch call_callback(Callback, DeliveryStatus, Message) of
        ok ->
            ok;
        Error ->
            ?ERROR_MSG("~p:delivery_report error: ~p", [Callback, Error])
    end,
    {noreply, State};

handle_info({stats, Stats0}, #state{stats_cb = StatsCb, client_id = ClientId} = State) ->
    Stats = erlkaf_json:decode(Stats0),

    case catch erlkaf_utils:call_stats_callback(StatsCb, ClientId, Stats) of
        ok ->
            ok;
        Error ->
            ?ERROR_MSG("~p:stats_callback client_id: ~p error: ~p", [StatsCb, ClientId, Error])
    end,
    {noreply, State#state{stats = Stats}};

handle_info(Info, State) ->
    ?ERROR_MSG("received unknown message: ~p", [Info]),
    {noreply, State}.

terminate(Reason, #state{client_id = ClientId, ref = ClientRef, pqueue = Queue}) ->
    erlkaf_local_queue:free(Queue),
    case Reason of
        shutdown ->
            ok = erlkaf_nif:producer_cleanup(ClientRef),
            ?INFO_MSG("wait for producer client ~p to stop...", [ClientId]),
            receive
                client_stopped ->
                    ?INFO_MSG("producer client ~p stopped", [ClientId])
            end;
        _ ->
            ok
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%internals

call_callback(undefined, _DeliveryStatus, _Message) ->
    ok;
call_callback(C, DeliveryStatus, Message) when is_function(C, 2) ->
    C(DeliveryStatus, Message);
call_callback(C, DeliveryStatus, Message) ->
    C:delivery_report(DeliveryStatus, Message).

schedule_consume_queue(false, Timeout) ->
    erlang:send_after(Timeout, self(), consume_queue);
schedule_consume_queue(_, _) ->
    ok.

schedule_consume_queue(Timeout) ->
    erlang:send_after(Timeout, self(), consume_queue).

%todo:
% * we need support in case we shutdown the producer to get back the pending messages from librdkafka and
%   write them in the local queue. this is not supported now by librdkafka
%   more details: https://github.com/edenhill/librdkafka/issues/990

consume_queue(_ClientRef, _Q, 0) ->
    log_completed(0),
    ok;
consume_queue(ClientRef, Q, N) ->

    case erlkaf_local_queue:head(Q) of
        undefined ->
            log_completed(N),
            completed;
        #{payload := Msg} ->
            {TopicName, Partition, Key, Value} = Msg,
            case erlkaf_nif:produce(ClientRef, TopicName, Partition, Key, Value) of
                ok ->
                    [#{payload := Msg}] = erlkaf_local_queue:deq(Q),
                    consume_queue(ClientRef, Q, N-1);
                {error, ?RD_KAFKA_RESP_ERR_QUEUE_FULL} ->
                    log_completed(N),
                    ok;
                Error ->
                    ?ERROR_MSG("message ~p skipped because of error: ~p", [Msg, Error]),
                    [#{payload := Msg}] = erlkaf_local_queue:deq(Q),
                    consume_queue(ClientRef, Q, N-1)
            end
    end.

log_completed(N) ->
    case N =/= ?MAX_QUEUE_PROCESS_MSG of
        true ->
            ?INFO_MSG("pushed ~p events from local queue cache", [?MAX_QUEUE_PROCESS_MSG - N]);
        _ ->
            ok
    end.