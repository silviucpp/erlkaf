-module(benchmark_producer).

-define(TOPIC, <<"benchmark">>).

-export([
    delivery_report/3,
    benchmark/4
]).

-behavior(erlkaf_producer).

delivery_report(MsgRef, DeliveryStatus, Message) ->
    io:format("received delivery report: ~p ~n", [{MsgRef, DeliveryStatus, Message}]),
    ok.

benchmark(Driver, Concurrency, BytesPerMsg, MsgCount) ->
    init(Driver),
    Self = self(),

    Message = <<0:BytesPerMsg/little-signed-integer-unit:8>>,

    MsgsPerProc = MsgCount div Concurrency,

    ProcFun = fun() ->
        FunGen = fun(_) ->
            Key = integer_to_binary(rand(50000)),
            produce(Driver, Key, Message)
        end,

        ok = lists:foreach(FunGen, lists:seq(1, MsgsPerProc)),
        Self ! {self(), done}
    end,

    do_benchmark(Concurrency, BytesPerMsg, MsgCount, ProcFun).

do_benchmark(Concurrency, BytesPerMsg, MsgCount, Fun) ->
    List = lists:seq(1, Concurrency),

    A = os:timestamp(),
    Pids = [spawn_link(Fun) || _ <- List],
    [receive {Pid, done} -> ok end || Pid <- Pids],
    B = os:timestamp(),

    print(BytesPerMsg, MsgCount, A, B).

print(MsgBytesSize, MsgCount, A, B) ->
    Microsecs = timer:now_diff(B, A),
    Milliseconds = Microsecs/1000,
    Secs = Milliseconds/1000,

    MsgPerSec = MsgCount/Secs,
    BytesPerSec = (MsgCount*MsgBytesSize)/Secs,

    io:format("### time: ~p ms | msg count: ~p | msg/s: ~p | bytes/s: ~s ~n", [
        Milliseconds,
        MsgCount,
        MsgPerSec,
        format_size(BytesPerSec)
    ]).

init(erlkaf) ->
    erlkaf:start();
init(brod) ->
    brod:start().

produce(erlkaf, Key, Message) ->
    {ok, _} = erlkaf:produce(client_producer, ?TOPIC, Key, Message);
produce(brod, Key, Message) ->
    PartitionFun = fun(_Topic, PartitionsCount, _Key, _Value) ->
        {ok, erlang:crc32(term_to_binary(Key)) rem PartitionsCount}
    end,
    ok = brod:produce_sync(kafka_client, ?TOPIC, PartitionFun, Key, Message);
produce(_, _Key, _Message) ->
    ok.

rand(N) ->
    {_, _, MicroSecs} = os:timestamp(),
    (MicroSecs rem N) + 1.

format_size(Size) ->
    format_size(Size, ["B","KB","MB","GB","TB","PB"]).

format_size(S, [_|[_|_] = L]) when S >= 1024 -> format_size(S/1024, L);
format_size(S, [M|_]) ->
    io_lib:format("~.2f ~s", [float(S), M]).
