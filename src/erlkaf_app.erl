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
        {ok, Value} ->
            ok = lists:foreach(fun(Client) -> start_client(Client) end, Value);
        _ ->
            ok
    end.

start_client({ClientId, C}) ->
    Type = erlkaf_utils:lookup(type, C),
    Topics = erlkaf_utils:lookup(topics, C, []),
    Options = erlkaf_utils:lookup(client_options, C, []),

    case Type of
        producer ->
            ok = erlkaf:create_producer(ClientId, Options),
            ?INFO_MSG("client ~p created", [ClientId]),
            ok = create_topics(ClientId, Topics);
        consumer ->
            throw(not_implemented)
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
