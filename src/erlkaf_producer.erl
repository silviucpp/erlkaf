-module(erlkaf_producer).

-include("erlkaf_private.hrl").
-include("erlkaf.hrl").

-behaviour(gen_server).

-export([start_link/4, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    client_id,
    ref,
    dr_cb,
    stats_cb,
    stats = []
}).

start_link(ClientId, DrCallback, ErlkafConfig, ProducerRef) ->
    gen_server:start_link(?MODULE, [ClientId, DrCallback, ErlkafConfig, ProducerRef], []).

init([ClientId, DrCallback, ErlkafConfig, ProducerRef]) ->
    Pid = self(),
    StatsCallback =  erlkaf_utils:lookup(stats_callback, ErlkafConfig),
    ok = erlkaf_nif:producer_set_owner(ProducerRef, Pid),
    ok = erlkaf_cache_client:set(ClientId, ProducerRef, Pid),
    process_flag(trap_exit, true),
    {ok, #state{client_id = ClientId, ref = ProducerRef, dr_cb = DrCallback, stats_cb = StatsCallback}}.

handle_call(get_stats, _From, #state{stats = Stats} = State) ->
    {reply, {ok, Stats}, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

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

terminate(Reason, #state{client_id = ClientId, ref = ClientRef}) ->
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
