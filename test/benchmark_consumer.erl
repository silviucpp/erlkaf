-module(benchmark_consumer).

-include("erlkaf.hrl").
-include("erlkaf_private.hrl").
-include_lib("brod/include/brod.hrl").

-define(TOPIC, <<"benchmark">>).
-define(BROKER_HOST, <<"172.17.33.123">>).
-define(BROKER_PORT, 9092).

-export([
    start/1,
    % erlkaf callbacks
    init/4,
    handle_message/2,
    % brod callbacks
    init/2,
    handle_message/4
]).

-behaviour(erlkaf_consumer_callbacks).
-behaviour(brod_group_subscriber).

-record(state, {
    msg_recv,
    bytes_recv,
    last_ts
}).

start(erlkaf) ->
    lager:start(),
    case erlkaf:start() of
        ok ->
            ConsumerConfig = [{bootstrap_servers, <<?BROKER_HOST/binary, ":", (integer_to_binary(?BROKER_PORT))/binary>>}],
            TopicConf = [{auto_offset_reset, smallest}],
            GroupId = <<"erlkaf_consumer_benchmark">>,
            ok = erlkaf:create_consumer_group(client_consumer, GroupId, [?TOPIC], ConsumerConfig, TopicConf, ?MODULE, []);
        _ ->
            ok
    end;
start(brod) ->
    lager:start(),
    case brod:start() of
        ok ->
            ok = brod:start_client([{binary_to_list(?BROKER_HOST), ?BROKER_PORT}], client_bench),
            GroupConfig = [{offset_commit_policy, commit_to_kafka_v2}, {offset_commit_interval_seconds, 5}],
            GroupId = <<"brod_consumer_benchmark">>,
            ConsumerConfig = [{begin_offset, earliest}, {offset_reset_policy, reset_to_earliest}],
            {ok, _} = brod:start_link_group_subscriber(client_bench, GroupId, [?TOPIC], GroupConfig, ConsumerConfig, ?MODULE, []);
        _ ->
            ok
    end.

%erlkaf callbacks

init(_Topic, _Partition, _Offset, _Args) ->
    {ok, #state{}}.

handle_message(#erlkaf_msg{value = Value, partition = Partition}, State) ->
    {ok, update_stats(State, Partition, Value)}.

%brod callbacks

init(_Topic, []) ->
    {ok, #state{}}.

handle_message(_Topic, Partition, #kafka_message{value  = Value}, State) ->
    {ok, ack, update_stats(State, Partition, Value)}.

%internals

update_stats(State, Partition, Value) ->
    #state{msg_recv = MsgRecv, bytes_recv = BytesRecv, last_ts = LastTs} = State,

    case LastTs of
        undefined ->
            #state{msg_recv = 1, bytes_recv = byte_size(Value), last_ts = now_ms()};
        _ ->
            Now = now_ms(),
            Diff = Now - LastTs,
            case Diff > 1000 of
                true ->
                    DiffSec = Diff/1000,
                    MsgPerSec = (MsgRecv+1)/DiffSec,
                    BytesPerSec = (BytesRecv + byte_size(Value))/DiffSec,

                    ?INFO_MSG("##partition: ~p recv msg/s: ~p | bytes/s: ~s ~n", [Partition, MsgPerSec, format_size(BytesPerSec)]),

                    State#state{msg_recv = 0, bytes_recv = 0, last_ts = Now};
                _ ->
                    State#state{msg_recv = MsgRecv+1, bytes_recv = BytesRecv + byte_size(Value)}
            end
    end.

format_size(Size) ->
    format_size(Size, ["B","KB","MB","GB","TB","PB"]).

format_size(S, [_|[_|_] = L]) when S >= 1024 -> format_size(S/1024, L);
format_size(S, [M|_]) ->
    io_lib:format("~.2f ~s", [float(S), M]).

now_ms() ->
    {Mega, Sec, Micro} = os:timestamp(),
    Mega*1000000000+Sec*1000+(Micro div 1000).