-module(erlkaf).

-include("erlkaf.hrl").
-include("erlkaf_private.hrl").

-export([
    start/0,
    start/1,
    stop/0,

    create_producer/2,
    stop_producer/1,

    create_topic/2,
    create_topic/3,
    create_topic/4,

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

create_producer(ClientId, Config) ->
    case erlkaf_config:convert_kafka_config(Config) of
        {ok, ErlkafConfig, LibRdkafkaConfig} ->
            erlkaf_manager:start_producer(ClientId, ErlkafConfig, LibRdkafkaConfig);
        Error ->
            Error
    end.

-spec stop_producer(client_id()) ->
    ok | {error, reason()}.

stop_producer(ClientId) ->
    erlkaf_manager:stop_producer(ClientId).

-spec create_topic(client_id(), binary()) ->
    ok | {error, reason()}.

create_topic(ClientId, TopicName) ->
    create_topic(ClientId, TopicName, TopicName, []).

-spec create_topic(client_id(), binary(), [topic_option()]) ->
    ok | {error, reason()}.

create_topic(ClientId, TopicName, TopicConfig) ->
    create_topic(ClientId, TopicName, TopicName, TopicConfig).

-spec create_topic(client_id(), topic_id(), binary(), [topic_option()]) ->
    ok | {error, reason()}.

create_topic(ClientId, TopicId, TopicName, TopicConfig) ->
    case erlkaf_cache_client:get(ClientId) of
        {ok, ClientRef, _ClientPid} ->
            case erlkaf_config:convert_topic_config(TopicConfig) of
                {ok, _ErlkafConfig, LibRdkafkaConfig} ->
                    erlkaf_manager:create_topic(ClientRef, TopicId, TopicName, LibRdkafkaConfig);
                Error ->
                    Error
            end;
        _ ->
            {error, ?ERR_UNDEFINED_CLIENT}
    end.

-spec produce(client_id(), topic_id(), key(), binary()) ->
    ok | {error, reason()}.

produce(ClientId, TopicId, Key, Value) ->
    produce(ClientId, TopicId, ?DEFULT_PARTITIONER, Key, Value).

-spec produce(client_id(), topic_id(), partition(), key(), binary()) ->
    ok | {error, reason()}.

produce(ClientId, TopicId, Partition, Key, Value) ->
    case erlkaf_cache_client:get(ClientId) of
        {ok, ClientRef, _ClientPid} ->
            case erlkaf_nif:produce(ClientRef, erlkaf_utils:topicid2bin(TopicId), Partition, Key, Value) of
                {error, ?RD_KAFKA_RESP_ERR_QUEUE_FULL} ->
                    %todo: investigate something smarter
                    produce(ClientId, TopicId, Partition, Key, Value);
                Resp ->
                    Resp
            end;
        undefined ->
            {error, ?ERR_UNDEFINED_CLIENT};
        Error ->
            Error
    end.