-module(erlkaf_nif).

-define(NOT_LOADED, not_loaded(?LINE)).

-on_load(load_nif/0).

-export([

    set_log_process/1,

    producer_new/2,
    producer_cleanup/1,
    producer_set_owner/2,
    producer_topic_new/3,
    produce/7,
    get_metadata/1,

    consumer_new/4,
    consumer_partition_revoke_completed/1,
    consumer_queue_poll/2,
    consumer_queue_cleanup/1,
    consumer_offset_store/4,
    consumer_cleanup/1
]).

%% nif functions

load_nif() ->
    SoName = erlkaf_utils:get_priv_path(?MODULE),
    io:format(<<"Loading library: ~p ~n">>, [SoName]),
    ok = erlang:load_nif(SoName, 0).

not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).

set_log_process(_Pid) ->
    ?NOT_LOADED.

producer_new(_HasDrCallback, _Config) ->
    ?NOT_LOADED.

producer_cleanup(_ClientRef) ->
    ?NOT_LOADED.

producer_set_owner(_ClientRef, _Pid) ->
    ?NOT_LOADED.

producer_topic_new(_ClientRef, _TopicName, _TopicConfig) ->
    ?NOT_LOADED.

produce(_ClientRef, _TopicRef, _Partition, _Key, _Value, _Headers, _Timestamp) ->
    ?NOT_LOADED.

get_metadata(_ClientRef) ->
    ?NOT_LOADED.

consumer_new(_GroupId, _Topics, _ClientConfig, _TopicsConfig) ->
    ?NOT_LOADED.

consumer_partition_revoke_completed(_ClientRef) ->
    ?NOT_LOADED.

consumer_queue_poll(_Queue, _BatchSize) ->
    ?NOT_LOADED.

consumer_queue_cleanup(_Queue) ->
    ?NOT_LOADED.

consumer_offset_store(_ClientRef, _TopicName, _Partition, _Offset) ->
    ?NOT_LOADED.

consumer_cleanup(_ClientRef) ->
    ?NOT_LOADED.
