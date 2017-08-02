-module(erlkaf_app).

-include("erlkaf_private.hrl").

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = erlkaf_cache_client:create(),
    {ok, Pid} = erlkaf_sup:start_link(),
    ok = start_clients(),
    {ok, Pid}.

stop(_State) ->
    ok.

start_clients() ->
    case erlkaf_utils:get_env(clients) of
        undefied ->
            ok;
        Value ->
            ok = lists:foreach(fun(Client) -> start_client(Client) end, Value)
    end.

start_client({ClientId, C}) ->
    GlobalClientOpts = erlkaf_utils:get_env(global_client_options, []),

    Type = erlkaf_utils:lookup(type, C),
    LocalClientOpts = erlkaf_utils:lookup(client_options, C, []),
    Topics = erlkaf_utils:lookup(topics, C, []),

    ClientOpts = append_props(LocalClientOpts, GlobalClientOpts),

    case Type of
        producer ->
            ok = erlkaf:create_producer(ClientId, ClientOpts),
            ?INFO_MSG("producer ~p created", [ClientId]),
            ok = create_topics(ClientId, Topics);
        consumer ->
            GroupId = erlkaf_utils:lookup(group_id, C),
            CbModule = erlkaf_utils:lookup(callback_module, C),
            CbArgs = erlkaf_utils:lookup(callback_args, C, []),
            TopicConfig = erlkaf_utils:lookup(topic_options, C, []),
            ok = erlkaf:create_consumer_group(ClientId, GroupId, Topics, ClientOpts, TopicConfig, CbModule, CbArgs),
            ?INFO_MSG("consumer ~p created", [ClientId])
    end.

create_topics(ClientId, [H|T]) ->
    case H of
        {TopicName, TopicOpts} ->
            ok = erlkaf:create_topic(ClientId, TopicName, TopicOpts),
            ?INFO_MSG("topic ~p created over client: ~p", [TopicName, ClientId]);
        TopicName when is_binary(TopicName) ->
            ok = erlkaf:create_topic(ClientId, TopicName),
            ?INFO_MSG("topic ~p created over client: ~p", [TopicName, ClientId])
    end,
    create_topics(ClientId, T);
create_topics(_ClientId, []) ->
    ok.

append_props(L1, [{K, _} = H|T]) ->
    case erlkaf_utils:lookup(K, L1) of
        undefined ->
            append_props([H|L1], T);
        _ ->
            append_props(L1, T)
    end;
append_props(L1, []) ->
    L1.