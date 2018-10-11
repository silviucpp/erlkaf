
-module(test_producer).

-define(TOPIC, <<"benchmark">>).

-export([
    delivery_report/2,
    stats_callback/2,
    create_producer/0,
    produce/2
]).

-behaviour(erlkaf_producer_callbacks).

delivery_report(DeliveryStatus, Message) ->
    io:format("received delivery report: ~p ~n", [{DeliveryStatus, Message}]),
    ok.

stats_callback(ClientId, Stats) ->
    io:format("stats_callback: ~p stats:~p ~n", [ClientId, length(Stats)]).

create_producer() ->
    erlkaf:start(),

    ProducerConfig = [
        {bootstrap_servers, "172.17.3.163:9092"},
        {delivery_report_only_error, true},
        {delivery_report_callback, ?MODULE}
    ],
    ok = erlkaf:create_producer(client_producer, ProducerConfig),
    ok = erlkaf:create_topic(client_producer, ?TOPIC, [{request_required_acks, 1}]).

produce(Key, Value) ->
    ok = erlkaf:produce(client_producer, ?TOPIC, Key, Value).
