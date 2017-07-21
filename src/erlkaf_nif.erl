-module(erlkaf_nif).

-define(NOT_LOADED, not_loaded(?LINE)).

-on_load(load_nif/0).

-export([

    set_log_process/1,

    producer_new/2,
    producer_set_owner/2,
    producer_topic_new/3,
    produce/5,

    consumer_new/4,
    consumer_partition_revoke_completed/1,
    consumer_queue_poll/1,
    consumer_offset_store/4
]).

%% nif functions

load_nif() ->
    SoName = get_nif_library_path(),
    io:format(<<"Loading library: ~p ~n">>, [SoName]),
    ok = erlang:load_nif(SoName, 0).

get_nif_library_path() ->
    case code:priv_dir(erlkaf) of
        {error, bad_name} ->
            case filelib:is_dir(filename:join(["..", priv])) of
                true ->
                    filename:join(["..", priv, ?MODULE]);
                false ->
                    filename:join([priv, ?MODULE])
            end;
        Dir ->
            filename:join(Dir, ?MODULE)
    end.

not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).

set_log_process(_Pid) ->
    ?NOT_LOADED.

producer_new(_HasDrCallback, _Config) ->
    ?NOT_LOADED.

producer_set_owner(_ClientRef, _Pid) ->
    ?NOT_LOADED.

producer_topic_new(_ClientRef, _TopicName, _TopicConfig) ->
    ?NOT_LOADED.

produce(_ClientRef, _TopicRef, _Partition, _Key, _Value) ->
    ?NOT_LOADED.

consumer_new(_GroupId, _Topics, _ClientConfig, _TopicsConfig) ->
    ?NOT_LOADED.

consumer_partition_revoke_completed(_ClientRef) ->
    ?NOT_LOADED.

consumer_queue_poll(_Queue) ->
    ?NOT_LOADED.

consumer_offset_store(_ClientRef, _TopicName, _Partition, _Offset) ->
    ?NOT_LOADED.
