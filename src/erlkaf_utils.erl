-module(erlkaf_utils).

-export([
    get_env/1,
    lookup/2,
    lookup/3,
    to_binary/1,
    safe_call/2,
    safe_call/3,
    call_stats_callback/3
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

safe_call(Receiver, Message) ->
    safe_call(Receiver, Message, 5000).

safe_call(Receiver, Message, Timeout) ->
    try
        gen_server:call(Receiver, Message, Timeout)
    catch
        exit:{noproc, _} ->
            {error, not_started};
        _: Exception ->
            {error, Exception}
    end.

call_stats_callback(undefined, _ClientId, _Stats) ->
    ok;
call_stats_callback(C, ClientId, Stats) when is_function(C, 2) ->
    C(ClientId, Stats);
call_stats_callback(C, ClientId, Stats) ->
    C:stats_callback(ClientId, Stats).