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

create_producer(ClientId, Config) ->
    case erlkaf_config:convert_kafka_config(Config) of
        {ok, ErlkafConfig, LibRdkafkaConfig} ->
            erlkaf_manager:start_producer(ClientId, ErlkafConfig, LibRdkafkaConfig);
        Error ->
            Error
    end.

-spec create_consumer_group(client_id(), binary(), [binary()], [client_option()], [topic_option()], atom(), any()) ->
    ok | {error, reason()}.

create_consumer_group(ClientId, GroupId, Topics, ClientConfig, TopicConfig, CbModule, CbArgs) ->
    erlkaf_manager:start_consumer_group(ClientId, GroupId, Topics, ClientConfig, TopicConfig, CbModule, CbArgs).

-spec stop_client(client_id()) ->
    ok | {error, reason()}.

stop_client(ClientId) ->
    erlkaf_manager:stop_client(ClientId).

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
        {ok, ClientRef, _ClientPid} ->
            case erlkaf_nif:produce(ClientRef, TopicName, Partition, Key, Value) of
                {error, ?RD_KAFKA_RESP_ERR_QUEUE_FULL} ->
                    %todo: investigate something smarter like storing the messages in DETS and
                    %send them back when we have space in the memory queue
                    produce(ClientId, TopicName, Partition, Key, Value);
                Resp ->
                    Resp
            end;
        undefined ->
            {error, ?ERR_UNDEFINED_CLIENT};
        Error ->
            Error
    end.