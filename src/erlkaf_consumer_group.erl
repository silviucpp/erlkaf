-module(erlkaf_consumer_group).

-include("erlkaf_private.hrl").

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export([start_link/9]).

-record(state, {
    client_id,
    client_ref,
    cb_module,
    cb_args,
    topics_map =#{},
    stats_cb,
    stats = []
}).

start_link(ClientId, GroupId, Topics, EkClientConfig, RdkClientConfig, EkTopicConfig, RdkTopicConfig, CbModule, CbArgs) ->
    gen_server:start_link(?MODULE, [ClientId, GroupId, Topics, EkClientConfig, RdkClientConfig, EkTopicConfig, RdkTopicConfig, CbModule, CbArgs], []).

init([ClientId, GroupId, Topics, EkClientConfig, RdkClientConfig, _EkTopicConfig, RdkTopicConfig, CbModule, CbArgs]) ->
    process_flag(trap_exit, true),

    case erlkaf_nif:consumer_new(GroupId, Topics, RdkClientConfig, RdkTopicConfig) of
        {ok, ClientRef} ->
            ok = erlkaf_cache_client:set(ClientId, undefined, self()),
            StatsCb =  erlkaf_utils:lookup(stats_callback, EkClientConfig),
            {ok, #state{client_id = ClientId, client_ref = ClientRef, cb_module = CbModule, cb_args = CbArgs, stats_cb = StatsCb}};
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

handle_info({assign_partitions, Partitions}, #state{client_ref = Crf, cb_module = CbModule, cb_args = CbState, topics_map = TopicsMap} = State) ->
    ?INFO_MSG("assign partitions: ~p", [Partitions]),

    PartFun = fun({TopicName, Partition, Offset, QueueRef}, Tmap) ->
        {ok, Pid} = erlkaf_consumer:start_link(Crf, TopicName, Partition, Offset, QueueRef, CbModule, CbState),
        maps:put({TopicName, Partition}, Pid, Tmap)
    end,

    NewTopicMap = lists:foldl(PartFun, TopicsMap, Partitions),
    {noreply, State#state{topics_map = NewTopicMap}};

handle_info({revoke_partitions, Partitions}, #state{client_ref = ClientRef, topics_map = TopicsMap} = State) ->
    ?INFO_MSG("revoke partitions: ~p", [Partitions]),

    PartFun = fun(Key, Tmap) ->
        {Pid, NewTmap} = maps:take(Key, Tmap),
        erlkaf_consumer:stop(Pid),
        NewTmap
    end,

    NewTopicMap = lists:foldl(PartFun, TopicsMap, Partitions),
    ok = erlkaf_nif:consumer_partition_revoke_completed(ClientRef),
    {noreply, State#state{topics_map = NewTopicMap}};

handle_info({'EXIT', FromPid, Reason} , State) when Reason =/= normal ->
    ?WARNING_MSG("consumer ~p died with reason: ~p. restart consumer ...", [FromPid, Reason]),
    {stop, {error, Reason}, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
