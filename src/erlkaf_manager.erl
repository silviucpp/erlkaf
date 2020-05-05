-module(erlkaf_manager).

-include("erlkaf_private.hrl").

-behaviour(gen_server).

-export([

    % api

    start_link/0,
    start_producer/3,
    start_consumer_group/7,
    stop_client/1,
    create_topic/3,

    % gen_server

    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).

-record(state, {}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

start_producer(ClientId, ErlkafConfig, LibRdkafkaConfig) ->
    erlkaf_utils:safe_call(?MODULE, {start_producer, ClientId, ErlkafConfig, LibRdkafkaConfig}).

start_consumer_group(ClientId, GroupId, Topics, ClientConfig, TopicConfig, CbModule, CbArgs) ->
    erlkaf_utils:safe_call(?MODULE, {start_consumer_group, ClientId, GroupId, Topics, ClientConfig, TopicConfig, CbModule, CbArgs}).

stop_client(ClientId) ->
    erlkaf_utils:safe_call(?MODULE, {stop_client, ClientId}, infinity).

create_topic(ClientRef, TopicName, TopicConfig) ->
    erlkaf_utils:safe_call(?MODULE, {create_topic, ClientRef, TopicName, TopicConfig}).

%gen server

init([]) ->
    {ok, #state{}}.

handle_call({create_topic, ClientRef, TopicName, TopicConfig}, _From, State) ->
    {reply, erlkaf_nif:producer_topic_new(ClientRef, TopicName, TopicConfig), State};

handle_call({start_producer, ClientId, ErlkafConfig, LibRdkafkaConfig}, _From, State) ->
    case internal_start_producer(ClientId, ErlkafConfig, LibRdkafkaConfig) of
        {ok, _Pid} ->
            {reply, ok, State};
        Error ->
            {reply, Error, State}
    end;

handle_call({start_consumer_group, ClientId, GroupId, Topics, ClientConfig, TopicConfig, CbModule, CbArgs}, _From, State) ->
    case internal_start_consumer(ClientId, GroupId, Topics, ClientConfig, TopicConfig, CbModule, CbArgs) of
        {ok, _Pid} ->
            {reply, ok, State};
        Error ->
            {reply, Error, State}
    end;

handle_call({stop_client, ClientId}, _From, State) ->
    {reply, internal_stop_client(ClientId), State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%internals

internal_start_producer(ClientId, ErlkafConfig, LibRdkafkaConfig) ->
    case erlkaf_cache_client:get(ClientId) of
        undefined ->
            DeliveryReportCallback = erlkaf_utils:lookup(delivery_report_callback, ErlkafConfig),
            HasDrCallback = DeliveryReportCallback =/= undefined,

            case erlkaf_nif:producer_new(HasDrCallback, LibRdkafkaConfig) of
                {ok, ProducerRef} ->
                    erlkaf_sup:add_client(ClientId, erlkaf_producer, [ClientId, DeliveryReportCallback, ErlkafConfig, ProducerRef]);
                Error ->
                    Error
            end;
        {ok, _, _} ->
            {error, ?ERR_ALREADY_EXISTING_CLIENT}
    end.

internal_start_consumer(ClientId, GroupId, Topics, ClientConfig, TopicConfig, CbModule, CbArgs) ->
    case erlkaf_cache_client:get(ClientId) of
        undefined ->
            case erlkaf_config:convert_kafka_config(ClientConfig) of
                {ok, EkClientConfig, RdkClientConfig} ->
                    case erlkaf_config:convert_topic_config(TopicConfig) of
                        {ok, EkTopicConfig, RdkTopicConfig} ->
                            Args = [ClientId, GroupId, Topics, EkClientConfig, RdkClientConfig, EkTopicConfig, RdkTopicConfig, CbModule, CbArgs],
                            erlkaf_sup:add_client(ClientId, erlkaf_consumer_group, Args);
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end;
        {ok, _, _} ->
            {error, ?ERR_ALREADY_EXISTING_CLIENT}
    end.

internal_stop_client(ClientId) ->
    case erlkaf_cache_client:take(ClientId) of
        [_] ->
            erlkaf_sup:remove_client(ClientId);
        _ ->
            {error, ?ERR_UNDEFINED_CLIENT}
    end.