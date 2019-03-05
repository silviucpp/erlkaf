-module(test_consumer).

-include("erlkaf.hrl").

-define(TOPICS, [<<"benchmark">>]).

-export([
    create_consumer/0,
    init/4,
    handle_message/2
]).

-behaviour(erlkaf_consumer_callbacks).

-record(state, {}).

create_consumer() ->
    erlkaf:start(),

    GroupId = <<"erlkaf_consumer">>,

    ClientConfig = [
        {bootstrap_servers, "172.17.3.163:9092"}
    ],

    TopicConf = [
        {auto_offset_reset, smallest}
    ],

    ok = erlkaf:create_consumer_group(client_consumer, GroupId, ?TOPICS, ClientConfig, TopicConf, ?MODULE, []).

init(Topic, Partition, Offset, Args) ->
    io:format("init topic: ~p partition: ~p offset: ~p args: ~p ~n", [
        Topic,
        Partition,
        Offset,
        Args
    ]),
    {ok, #state{}}.

handle_message(Msgs, State) ->
    case Msgs of
        #erlkaf_msg{topic = Topic, partition = Partition, offset = Offset} ->
            io:format("handle_message topic: ~p partition: ~p offset: ~p state: ~p ~n", [Topic, Partition, Offset, State]);
        _ ->
            io:format("handle_message BATCH count: ~p state: ~p ~n", [length(Msgs), State])
    end,

    {ok, State}.