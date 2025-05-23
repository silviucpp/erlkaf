-module(erlkaf_utils).

-export([
    get_priv_path/1,
    get_local_queue_path/1,
    get_env/1,
    get_env/2,
    lookup/2,
    lookup/3,
    append_props/2,
    to_binary/1,
    safe_call/2,
    safe_call/3,
    safe_cast/2,
    call_stats_callback/3,
    call_oauthbearer_token_refresh_callback/2,
    parralel_exec/2
]).

-spec get_local_queue_path(string() | atom()) -> string() | undefined.
get_local_queue_path(File) ->
    case application:get_env(erlkaf, global_client_options) of
        undefined -> 
            get_priv_path(File);
        {ok, Val} -> 
            case lookup(local_queue_path, Val) of
                undefined -> 
                    get_priv_path(File);
                Path ->
                    filename:join(Path, File)
            end
    end.
    
get_priv_path(File) ->
    case code:priv_dir(erlkaf) of
        {error, bad_name} ->
            Ebin = filename:dirname(code:which(?MODULE)),
            filename:join([filename:dirname(Ebin), "priv", File]);
        Dir ->
            filename:join(Dir, File)
    end.

get_env(Key) ->
    get_env(Key, undefined).

get_env(Key, Default) ->
    case application:get_env(erlkaf, Key) of
        undefined ->
            Default;
        {ok, Val} ->
            Val
    end.

lookup(Key, List) ->
    lookup(Key, List, undefined).

lookup(Key, List, Default) ->
    case lists:keyfind(Key, 1, List) of
        {Key, Result}
            -> Result;
        false ->
            Default
    end.

append_props(L1, [{K, _} = H|T]) ->
    case lookup(K, L1) of
        undefined ->
            append_props([H|L1], T);
        _ ->
            append_props(L1, T)
    end;
append_props(L1, []) ->
    L1.

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

safe_cast(Receiver, Message) ->
    try
        gen_server:cast(Receiver, Message)
    catch
        exit:{noproc, _} ->
            {error, not_started};
        _: Exception ->
            {error, Exception}
    end.

call_stats_callback(undefined, _ClientId, _Stats) ->
    ok;
call_stats_callback(C, OAuthBearerConfig, Stats) when is_function(C, 2) ->
    C(OAuthBearerConfig, Stats);
call_stats_callback(C, OAuthBearerConfig, Stats) ->
    C:stats_callback(OAuthBearerConfig, Stats).

call_oauthbearer_token_refresh_callback(undefined, _OauthbearerConfig) ->
    ok;
call_oauthbearer_token_refresh_callback(C, OauthbearerConfig) when is_function(C, 1) ->
    C(OauthbearerConfig);
call_oauthbearer_token_refresh_callback(C, OauthbearerConfig) ->
    C:oauthbearer_token_refresh_callback(OauthbearerConfig).

parralel_exec(Fun, List) ->
    Parent = self(),
    Pids = [spawn_link(fun() -> Fun(E), Parent ! {self(), done} end) || E <- List],
    [receive {Pid, done} -> ok end || Pid <- Pids],
    ok.
