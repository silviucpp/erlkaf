-module(erlkaf_cache_client).

-export([
    create/0,
    set/3,
    get/1,
    del/1
]).

-define(ETS_TOPIC_CACHE, erlkaf_client_cache_tab).
-define(GET_KEY(ClientId, TopicName), {ClientId, TopicName}).

create() ->
    ?ETS_TOPIC_CACHE = ets:new(?ETS_TOPIC_CACHE, [set, named_table, public, {read_concurrency, true}]),
    ok.

set(ClientId, ClientRef, ClientPid) ->
    true = ets:insert(?ETS_TOPIC_CACHE, {ClientId, {ClientRef, ClientPid}}),
    ok.

get(ClientId) ->
    case ets:lookup(?ETS_TOPIC_CACHE, ClientId) of
        [{ClientId, {ClientRef, ClientPid}}] ->
            {ok, ClientRef, ClientPid};
        [] ->
            undefined
    end.

del(ClientId) ->
    ets:delete(ClientId).
