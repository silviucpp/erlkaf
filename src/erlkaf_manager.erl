-module(erlkaf_manager).

-include("erlkaf_private.hrl").

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export([
    start_link/0,
    start_producer/3,
    stop_producer/1,
    create_topic/4
]).

-define(SERVER, ?MODULE).

-record(state, {}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

start_producer(ClientId, ErlkafConfig, LibRdkafkaConfig) ->
    call({start_producer, ClientId, ErlkafConfig, LibRdkafkaConfig}).

stop_producer(ClientId) ->
    call({stop_producer, ClientId}).

create_topic(ClientRef, TopicId, TopicName, TopicConfig) ->
    call({create_topic, ClientRef, TopicId, TopicName, TopicConfig}).

%gen server

init([]) ->
    {ok, #state{}}.

handle_call({create_topic, ClientRef, TopicId, TopicName, TopicConfig}, _From, State) ->
    {reply, erlkaf_nif:topic_new(ClientRef, erlkaf_utils:topicid2bin(TopicId), TopicName, TopicConfig), State};

handle_call({start_producer, ClientId, ErlkafConfig, LibRdkafkaConfig}, _From, State) ->
    case internal_start_producer(ClientId, ErlkafConfig, LibRdkafkaConfig) of
        {ok, _Pid} ->
            {reply, ok, State};
        Error ->
            {reply, Error, State}
    end;

handle_call({stop_producer, ClientId}, _From, State) ->
    {reply, internal_stop_producer(ClientId), State};

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
                    erlkaf_sup:add_client(ClientId, erlkaf_producer, [ClientId, DeliveryReportCallback, ProducerRef]);
                Error ->
                    Error
            end;
        {ok, _} ->
            {error, ?ERR_ALREADY_EXISTING_CLIENT}
    end.

internal_stop_producer(ClientId) ->
    case erlkaf_cache_client:del(ClientId) of
        true ->
            erlkaf_sup:remove_client(ClientId);
        _ ->
            {error, ?ERR_UNDEFINED_CLIENT}
    end.

call(Message) ->
    call(Message, 5000).

call(Message, Timeout) ->
    try
        gen_server:call(?MODULE, Message, Timeout)
    catch
        exit:{noproc, _} ->
            {error, erlkaf_not_started};
        _: Exception ->
            {error, Exception}
    end.
