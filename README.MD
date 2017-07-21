# erlkaf

*Erlang kafka driver based on [librdkafka][1]*

### Implementation notes

This project is under development and the interface might change.

The library is implemented in top of `librdkafka` which is a C library implementation of the Apache Kafka protocol 
designed with message delivery reliability and high performance in mind, current figures exceed 1 million msgs/second 
for the producer and 3 million msgs/second for the consumer. 

##### How erlkaf affects the Erlang schedulers

It's well known that NIF's can affect the Erlang schedulers performances in case the functions are not returning in less
than 1-2 ms and blocks the scheduler threads.

Because the `librdkafka` driver is async, erlkaf won't block the scheduler threads and all calls to the native functions 
will return immediately. The `librdkafka` driver use it's own thread pool for managing the requests. Also each client 
has it's own thread from where is sending async the events (delivery reports, logs, statistics) to erlang 
using `enif_send`.

### TODO

- Add statistics support   
- Add benchmarks
- Add unit tests
- Add support for batch produce
- Document all configs

### User guide

Add `erlkaf` as a rebar dependency to your project:

```
{deps, [
  {erlkaf, ".*", {git, "https://github.com/silviucpp/erlkaf.git", "master"}},
}.
```

##### Producer example:

The following example will create a producer client with id `client1` which sends also delivery reports to the same module.

In order to receive the delivery reports you need to implement the `erlkaf_producer` protocol or to setup a function with
arity 3 into `delivery_report_callback` (`{delivery_report_callback, fun(MsgRef, DeliveryStatus, Message) -> .. end}`).

The function specified into delivery report callback is called async from another process (each producer has it's own process
from where it's dispatching the delivery reports)

Also in order to send messages to a topic you need to define a topic object attached to the client. To do this you can use the 
`erlkaf:create_topic` method.

```erlang
-module(test).

-export([
    delivery_report/3,
    create_producer/0,
    produce/2
]).

-behavior(erlkaf_producer).

delivery_report(MsgRef, DeliveryStatus, Message) ->
    io:format("received delivery report: ~p ~n", [{MsgRef, DeliveryStatus, Message}]),
    ok.

create_producer() ->
    ProducerConfig = [
        {bootstrap_servers, "broker1:9092,broker2:9092"},
        {delivery_report_only_error, false},
        {delivery_report_callback, ?MODULE}
    ],
    ok = erlkaf:create_producer(client1, ProducerConfig),
    ok = erlkaf:create_topic(client1, <<"benchmark">>, [{request_required_acks, 1}]).

produce(Key, Value) ->
    {ok, _} = erlkaf:produce(client1, <<"benchmark">>, Key, Value).
``` 

You can call those like:

```erlang
ok = erlkaf:start().
ok = test:create_producer().
test:produce(<<"key1">>, <<"val1">>).
```

And you will get into the console the delivery reports:
 
```erlang   
received delivery report: {#Ref<0.0.8.243>,ok, {erlkaf_msg,<<"benchmark">>,4,6172,<<"key1">>,<<"val1">>}} 
```    

In case you are not interested in the delivery reports don't specify any callback, or in case you want to receive the 
delivery reports only in case of errors you have to specify a callback and set `delivery_report_only_error` on `true`.

##### Consumer example:

The following example creates a consumer group that will consume messages from `benchmark` topic. For each topic and partition
the application will spawn an erlang process that will pull the messages. 

Each time the rebalance process takes place the process it's restarted so the `init/4` method will be called again. In case the
`handle_message/2` it's returning `{ok, State}` then the message is considered processed and the offset is stored to be committed
based on `auto_commit_interval_ms` setting.

```erlang
-module(test_consumer).

-include("erlkaf.hrl").

-export([
    create_consumer/0,
    init/4,
    handle_message/2
]).

-behavior(erlkaf_consumer).

-record(state, {}).

create_consumer() ->

    GroupId = <<"group_id_here">>,
    Topics = [<<"benchmark">>],

    ProdConfig = [
        {bootstrap_servers, "broker1:9092,broker2:9092"}
    ],

    TopicConf = [
        {auto_offset_reset, smallest}
    ],

    ok = erlkaf:create_consumer_group(GroupId, Topics, ProdConfig, TopicConf, ?MODULE, []).

init(Topic, Partition, Offset, Args) ->

    io:format("init topic: ~p partition: ~p offset: ~p args: ~p ~n", [
        Topic, 
        Partition, 
        Offset, 
        Args
    ]),

    {ok, #state{}}.

handle_message(#erlkaf_msg{topic = Topic, partition = Partition, offset = Offset}, State) ->

    io:format("handle_message topic: ~p partition: ~p offset: ~p state: ~p ~n", [
        Topic, 
        Partition, 
        Offset, 
        State
    ]),

    {ok, State}.
```

You can call those like:

```erlang
ok = erlkaf:start().
ok = test_consumer:create_consumer().
```

##### Start clients at startup

You may include client configs in `sys.config` have them started by default (by application controller)

Example of configuration (for `sys.config`):

```erlang
{erlkaf, [
    {clients, [
        {client1, [

            {type, producer},

            {topics, [
                {<<"benchmark">>, [{request_required_acks, 1}]}
            ]},

            {client_options, [
                {bootstrap_servers, "broker1.com:9092,broker2.com:9092"}
            ]}
        ]}
    ]}
]}
```

[1]:https://github.com/edenhill/librdkafka
