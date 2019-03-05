-module(erlkaf_consumer_group).

-include("erlkaf_private.hrl").

-behaviour(gen_server).

-export([
    start_link/9,

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
    client_ref,
    cb_module,
    cb_args,
    dispatch_mode,
    topics_map =#{},
    stats_cb,
    stats = []
}).

start_link(ClientId, GroupId, Topics, EkClientConfig, RdkClientConfig, EkTopicConfig, RdkTopicConfig, CbModule, CbArgs) ->
    gen_server:start_link(?MODULE, [ClientId, GroupId, Topics, EkClientConfig, RdkClientConfig, EkTopicConfig, RdkTopicConfig, CbModule, CbArgs], []).

init([ClientId, GroupId, Topics, EkClientConfig, RdkClientConfig, EkTopicConfig, RdkTopicConfig, CbModule, CbArgs]) ->
    process_flag(trap_exit, true),

    case erlkaf_nif:consumer_new(GroupId, Topics, RdkClientConfig, RdkTopicConfig) of
        {ok, ClientRef} ->
            ok = erlkaf_cache_client:set(ClientId, undefined, self()),

            {ok, #state{
                client_id = ClientId,
                client_ref = ClientRef,
                cb_module = CbModule,
                cb_args = CbArgs,
                dispatch_mode = erlkaf_utils:lookup(dispatch_mode, EkTopicConfig, one_by_one),
                stats_cb = erlkaf_utils:lookup(stats_callback, EkClientConfig)
            }};
        Error ->
            {stop, Error}
    end.

handle_call(get_stats, _From, #state{stats = Stats} = State) ->
    {reply, {ok, Stats}, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({stats, Stats0}, #state{stats_cb = StatsCb, client_id = ClientId} = State) ->
    Stats = erlkaf_json:decode(Stats0),

    case catch erlkaf_utils:call_stats_callback(StatsCb, ClientId, Stats) of
        ok ->
            ok;
        Error ->
            ?ERROR_MSG("~p:stats_callback client_id: ~p error: ~p", [StatsCb, ClientId, Error])
    end,
    {noreply, State#state{stats = Stats}};

handle_info({assign_partitions, Partitions}, #state{
    client_ref = Crf,
    cb_module = CbModule,
    cb_args = CbState,
    dispatch_mode = DispatchMode,
    topics_map = TopicsMap} = State) ->

    ?INFO_MSG("assign partitions: ~p", [Partitions]),

    PartFun = fun({TopicName, Partition, Offset, QueueRef}, Tmap) ->
        {ok, Pid} = erlkaf_consumer:start_link(Crf, TopicName, DispatchMode, Partition, Offset, QueueRef, CbModule, CbState),
        maps:put({TopicName, Partition}, Pid, Tmap)
    end,

    NewTopicMap = lists:foldl(PartFun, TopicsMap, Partitions),
    {noreply, State#state{topics_map = NewTopicMap}};

handle_info({revoke_partitions, Partitions}, #state{client_ref = ClientRef, topics_map = TopicsMap} = State) ->
    ?INFO_MSG("revoke partitions: ~p", [Partitions]),
    Pids = get_pids(TopicsMap, Partitions),
    ok = stop_consumers(Pids),
    ok = erlkaf_nif:consumer_partition_revoke_completed(ClientRef),
    {noreply, State#state{topics_map = #{}}};

handle_info({'EXIT', FromPid, Reason} , State) when Reason =/= normal ->
    ?WARNING_MSG("consumer ~p died with reason: ~p. restart consumer group ...", [FromPid, Reason]),
    {stop, {error, Reason}, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{topics_map = TopicsMap, client_ref = ClientRef, client_id = ClientId}) ->
    stop_consumers(maps:values(TopicsMap)),
    ok = erlkaf_nif:consumer_cleanup(ClientRef),

    ?INFO_MSG("wait for consumer client ~p to stop...", [ClientId]),

    receive
        client_stopped ->
            ?INFO_MSG("client ~p stopped", [ClientId])
        after 180000 ->
            ?ERROR_MSG("wait for client ~p stop timeout", [ClientId])
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

get_pids(TopicsMap, Partitions) ->
    lists:map(fun(P) -> maps:get(P, TopicsMap) end, Partitions).

stop_consumers(Pids) ->
    erlkaf_utils:parralel_exec(fun(Pid) -> erlkaf_consumer:stop(Pid) end, Pids).

