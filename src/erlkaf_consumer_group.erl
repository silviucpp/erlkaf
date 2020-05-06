-module(erlkaf_consumer_group).

-include("erlkaf_private.hrl").

-behaviour(gen_server).

-export([
    start_link/7,

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
    topics_settings = #{},
    active_topics_map = #{},
    stats_cb,
    stats = []
}).

start_link(ClientId, GroupId, Topics, EkClientConfig, RdkClientConfig, EkTopicConfig, RdkTopicConfig) ->
    gen_server:start_link(?MODULE, [ClientId, GroupId, Topics, EkClientConfig, RdkClientConfig, EkTopicConfig, RdkTopicConfig], []).

init([ClientId, GroupId, Topics, EkClientConfig, RdkClientConfig, _EkTopicConfig, RdkTopicConfig]) ->
    process_flag(trap_exit, true),

    TopicsNames = lists:map(fun({K, _}) -> K end, Topics),

    case erlkaf_nif:consumer_new(GroupId, TopicsNames, RdkClientConfig, RdkTopicConfig) of
        {ok, ClientRef} ->
            ok = erlkaf_cache_client:set(ClientId, undefined, self()),

            {ok, #state{
                client_id = ClientId,
                client_ref = ClientRef,
                topics_settings = maps:from_list(Topics),
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
            ?LOG_ERROR("~p:stats_callback client_id: ~p error: ~p", [StatsCb, ClientId, Error])
    end,
    {noreply, State#state{stats = Stats}};

handle_info({assign_partitions, Partitions}, #state{
    client_ref = ClientRef,
    topics_settings = TopicsSettingsMap,
    active_topics_map = ActiveTopicsMap} = State) ->

    ?LOG_INFO("assign partitions: ~p", [Partitions]),

    PartFun = fun({TopicName, Partition, Offset, QueueRef}, Tmap) ->
        TopicSettings = maps:get(TopicName, TopicsSettingsMap),
        {ok, Pid} = erlkaf_consumer:start_link(ClientRef, TopicName, Partition, Offset, QueueRef, TopicSettings),
        maps:put({TopicName, Partition}, Pid, Tmap)
    end,

    {noreply, State#state{active_topics_map = lists:foldl(PartFun, ActiveTopicsMap, Partitions)}};

handle_info({revoke_partitions, Partitions}, #state{
    client_ref = ClientRef,
    active_topics_map = ActiveTopicsMap} = State) ->

    ?LOG_INFO("revoke partitions: ~p", [Partitions]),
    Pids = get_pids(ActiveTopicsMap, Partitions),
    ok = stop_consumers(Pids),
    ok = erlkaf_nif:consumer_partition_revoke_completed(ClientRef),
    {noreply, State#state{active_topics_map = #{}}};

handle_info({'EXIT', FromPid, Reason} , State) when Reason =/= normal ->
    ?LOG_WARNING("consumer ~p died with reason: ~p. restart consumer group ...", [FromPid, Reason]),
    {stop, {error, Reason}, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{active_topics_map = TopicsMap, client_ref = ClientRef, client_id = ClientId}) ->
    stop_consumers(maps:values(TopicsMap)),
    ok = erlkaf_nif:consumer_cleanup(ClientRef),

    ?LOG_INFO("wait for consumer client ~p to stop...", [ClientId]),

    receive
        client_stopped ->
            ?LOG_INFO("client ~p stopped", [ClientId])
        after 180000 ->
            ?LOG_ERROR("wait for client ~p stop timeout", [ClientId])
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

get_pids(TopicsMap, Partitions) ->
    lists:map(fun(P) -> maps:get(P, TopicsMap) end, Partitions).

stop_consumers(Pids) ->
    erlkaf_utils:parralel_exec(fun(Pid) -> erlkaf_consumer:stop(Pid) end, Pids).

