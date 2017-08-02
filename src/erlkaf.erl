-module(erlkaf).

-include("erlkaf.hrl").
-include("erlkaf_private.hrl").

-export([
    start/0,
    start/1,
    stop/0,

    create_producer/2,
    create_consumer_group/7,
    stop_client/1,
    get_stats/1,

    create_topic/2,
    create_topic/3,

    produce/4,
    produce/5
]).

-spec start() ->
    ok  | {error, reason()}.

start() ->
    start(temporary).

-spec start(permanent | transient | temporary) ->
    ok | {error, reason()}.

start(Type) ->
    case application:ensure_all_started(erlkaf, Type) of
        {ok, _} ->
            ok;
        Other ->
            Other
    end.

-spec stop() ->
    ok.

stop() ->
    application:stop(erlkaf).

-spec create_producer(client_id(), [client_option()]) ->
    ok | {error, reason()}.

create_producer(ClientId, ClientConfig) ->
    GlobalClientOpts = erlkaf_utils:get_env(global_client_options, []),
    Config = erlkaf_utils:append_props(ClientConfig, GlobalClientOpts),

    case erlkaf_config:convert_kafka_config(Config) of
        {ok, ErlkafConfig, LibRdkafkaConfig} ->
            erlkaf_manager:start_producer(ClientId, ErlkafConfig, LibRdkafkaConfig);
        Error ->
            Error
    end.

-spec create_consumer_group(client_id(), binary(), [binary()], [client_option()], [topic_option()], atom(), any()) ->
    ok | {error, reason()}.

create_consumer_group(ClientId, GroupId, Topics, ClientConfig, TopicConfig, CbModule, CbArgs) ->
    GlobalClientOpts = erlkaf_utils:get_env(global_client_options, []),
    Config = erlkaf_utils:append_props(ClientConfig, GlobalClientOpts),
    erlkaf_manager:start_consumer_group(ClientId, GroupId, Topics, Config, TopicConfig, CbModule, CbArgs).

-spec stop_client(client_id()) ->
    ok | {error, reason()}.

stop_client(ClientId) ->
    erlkaf_manager:stop_client(ClientId).

-spec get_stats(client_id()) ->
    {ok, list()} | {error, reason()}.

get_stats(ClientId) ->
    case erlkaf_cache_client:get(ClientId) of
        {ok, _ClientRef, ClientPid} ->
            erlkaf_utils:safe_call(ClientPid, get_stats);
        _ ->
            {error, ?ERR_UNDEFINED_CLIENT}
    end.

-spec create_topic(client_id(), binary()) ->
    ok | {error, reason()}.

create_topic(ClientId, TopicName) ->
    create_topic(ClientId, TopicName, []).

-spec create_topic(client_id(), binary(), [topic_option()]) ->
    ok | {error, reason()}.

create_topic(ClientId, TopicName, TopicConfig) ->
    case erlkaf_cache_client:get(ClientId) of
        {ok, ClientRef, _ClientPid} ->
            case erlkaf_config:convert_topic_config(TopicConfig) of
                {ok, _ErlkafConfig, LibRdkafkaConfig} ->
                    erlkaf_manager:create_topic(ClientRef, TopicName, LibRdkafkaConfig);
                Error ->
                    Error
            end;
        _ ->
            {error, ?ERR_UNDEFINED_CLIENT}
    end.

-spec produce(client_id(), binary(), key(), binary()) ->
    ok | {error, reason()}.

produce(ClientId, TopicName, Key, Value) ->
    produce(ClientId, TopicName, ?DEFULT_PARTITIONER, Key, Value).

-spec produce(client_id(), binary(), partition(), key(), binary()) ->
    ok | {error, reason()}.

produce(ClientId, TopicName, Partition, Key, Value) ->
    case erlkaf_cache_client:get(ClientId) of
        {ok, ClientRef, ClientPid} ->
            case erlkaf_nif:produce(ClientRef, TopicName, Partition, Key, Value) of
                ok ->
                    ok;
                {error, ?RD_KAFKA_RESP_ERR_QUEUE_FULL} ->
                    case erlkaf_producer:queue_event(ClientPid, TopicName, Partition, Key, Value) of
                        ok ->
                            ok;
                        drop_records ->
                            ?WARNING_MSG("message: ~p dropped", [{TopicName, Partition, Key, Value}]),
                            ok;
                        block_calling_process ->
                            produce_blocking(ClientRef, TopicName, Partition, Key, Value);
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end;
        undefined ->
            {error, ?ERR_UNDEFINED_CLIENT};
        Error ->
            Error
    end.

%internals

produce_blocking(ClientRef, TopicName, Partition, Key, Value) ->
    case erlkaf_nif:produce(ClientRef, TopicName, Partition, Key, Value) of
        ok ->
            ok;
        {error, ?RD_KAFKA_RESP_ERR_QUEUE_FULL} ->
            timer:sleep(100),
            produce_blocking(ClientRef, TopicName, Partition, Key, Value);
        Error ->
            Error
    end.
