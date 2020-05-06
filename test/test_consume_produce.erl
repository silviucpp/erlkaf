-module(test_consume_produce).

-include("erlkaf.hrl").

-define(TOPIC, <<"test_consume_produce">>).

-define(PRODUCER_CLIENT, erlkaf_producer).

-export([

    % api

    create/0,
    create/1,
    produce/2,
    produce/3,
    produce_multiple/4,

    % consumer callbacks

    init/4,
    handle_message/2,

    % producer callbacks

    delivery_report/2,
    stats_callback/2
]).

-behaviour(erlkaf_consumer_callbacks).
% stats_callback is defined in both erlkaf_consumer_callbacks and erlkaf_producer_callbacks
% -behaviour(erlkaf_producer_callbacks).

create() ->
    create(undefined).

create(BootstrapServers) ->
    erlkaf:start(),

    % create consumer

    GroupId = atom_to_binary(?MODULE, latin1),
    ConsumerConfig = append_bootstrap(BootstrapServers, []),
    TopicConf = [{auto_offset_reset, smallest}],
    ConsumerTopics = [
        {?TOPIC, [
            {callback_module, ?MODULE}
        ]}
    ],

    ok = erlkaf:create_consumer_group(client_consumer, GroupId, ConsumerTopics, ConsumerConfig, TopicConf),

    % create producer

    ProducerConfig = append_bootstrap(BootstrapServers, [
        {delivery_report_only_error, true},
        {delivery_report_callback, ?MODULE}
    ]),
    ok = erlkaf:create_producer(?PRODUCER_CLIENT, ProducerConfig),
    ok = erlkaf:create_topic(?PRODUCER_CLIENT, ?TOPIC, [{request_required_acks, 1}]),
    ok.

produce(Key, Value) ->
    ok = erlkaf:produce(?PRODUCER_CLIENT, ?TOPIC, Key, Value).

produce(Key, Value, Headers) ->
    ok = erlkaf:produce(?PRODUCER_CLIENT, ?TOPIC, Key, Value, Headers).

produce_multiple(0, _Key, _Value, _Headers) ->
    ok;
produce_multiple(Count, Key, Value, Headers) ->
    ok = erlkaf:produce(?PRODUCER_CLIENT, ?TOPIC, Key, Value, Headers),
    produce_multiple(Count -1, Key, Value, Headers).

% consumer callbacks

init(Topic, Partition, Offset, Args) ->
    io:format("### consumer -> init topic: ~p partition: ~p offset: ~p args: ~p ~n", [Topic, Partition, Offset, Args]),
    {ok, #{}}.

handle_message(Msg, State) ->
    io:format("### consumer -> handle_message : ~p ~n", [Msg]),
    {ok, State}.

% producer callbacks

delivery_report(DeliveryStatus, Message) ->
    io:format("### producer -> received delivery report: ~p ~n", [{DeliveryStatus, Message}]),
    ok.

stats_callback(ClientId, Stats) ->
    io:format("### producer -> stats_callback: ~p stats:~p ~n", [ClientId, length(Stats)]).

% internals

append_bootstrap(undefined, V) ->
    V;
append_bootstrap(BootstrapServer, V) ->
    [{bootstrap_servers, BootstrapServer} | V].
