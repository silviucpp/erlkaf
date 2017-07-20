-module(erlkaf_consumer_group).

-include("erlkaf_private.hrl").

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export([start_link/8]).

-record(state, {
    client_ref,
    cb_module,
    cb_args,
    topics_map =#{}
}).

start_link(GroupId, Topics, EkClientConfig, RdkClientConfig, EkTopicConfig, RdkTopicConfig, CbModule, CbArgs) ->
    gen_server:start_link(?MODULE, [GroupId, Topics, EkClientConfig, RdkClientConfig, EkTopicConfig, RdkTopicConfig, CbModule, CbArgs], []).

init([GroupId, Topics, _EkClientConfig, RdkClientConfig, _EkTopicConfig, RdkTopicConfig, CbModule, CbArgs]) ->
    process_flag(trap_exit, true),

    case erlkaf_nif:consumer_new(GroupId, Topics, RdkClientConfig, RdkTopicConfig) of
        {ok, ClientRef} ->
            {ok, #state{client_ref = ClientRef, cb_module = CbModule, cb_args = CbArgs}};
        Error ->
            {stop, Error}
    end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

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