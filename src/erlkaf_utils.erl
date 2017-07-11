-module(erlkaf_utils).

-export([
    get_env/1,
    lookup/2,
    lookup/3,
    to_binary/1,
    topicid2bin/1
]).

get_env(Key) ->
    application:get_env(erlkaf, Key).

lookup(Key, List) ->
    lookup(Key, List, undefined).

lookup(Key, List, Default) ->
    case lists:keyfind(Key, 1, List) of
        {Key, Result}
            -> Result;
        false ->
            Default
    end.

to_binary(V) when is_binary(V) ->
    V;
to_binary(V) when is_list(V) ->
    list_to_binary(V);
to_binary(V) when is_atom(V) ->
    atom_to_binary(V, utf8);
to_binary(V) when is_integer(V) ->
    integer_to_binary(V);
to_binary(V) when is_float(V) ->
    float_to_bin(V).

float_to_bin(Value) ->
    float_to_binary(Value, [{decimals, 8}, compact]).

topicid2bin(TopicId) when is_binary(TopicId) ->
    TopicId;
topicid2bin(TopicId) ->
    term_to_binary(TopicId).