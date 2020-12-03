# erlkaf
[![Build Status](https://travis-ci.org/silviucpp/erlkaf.svg?branch=master)](https://travis-ci.org/silviucpp/erlkaf)
![GitHub](https://img.shields.io/github/license/silviucpp/erlkaf)
[![Hex.pm](https://img.shields.io/hexpm/v/erlkaf)](https://hex.pm/packages/erlkaf)

*Erlang kafka driver based on [librdkafka][1]*

### Implementation notes

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

### Upgrading from v1.X to v2.0.0

Version 2.0.0 contains a major rewrite of the consumer groups in order to be able to scale when you have lot of topics:
 
- In version 1.x each consumer group or producer is creating one dedicated native thread that's used to deliver the callbacks. Starting 2.0.0 no matter 
how many producers or consumer groups you have, one single thread will be used to deliver the callbacks. 

- In version 1.x each consumer group can handle a list of topics but you cannot specify a callback for each topic. Now the callbacks settings moved
into the topics list. This breaks the backward compatibility for the API and config. More details into [Changelog][3].

### User guide

On Ubuntu make sure you have installed :

```sh
sudo apt-get install libsasl2-dev liblz4-dev libzstd-dev
```

Add `erlkaf` as a dependency to your project. The library works with `rebar`, `rebar3` or `hex.pm`

```erlang
{deps, [
  {erlkaf, ".*", {git, "https://github.com/silviucpp/erlkaf.git", "master"}},
}.
```

Using `sys.config` you can have all clients (producers/consumers) started by default (by application controller)

Example of a configuration file (for `sys.config`):

```erlang
{erlkaf, [

    {global_client_options, [
        {bootstrap_servers, <<"broker1.com:9092,broker2.com:9092">>},
    ]},

    {clients, [
        {client_producer_id, [

            {type, producer},

            {topics, [
                {<<"benchmark">>, [{request_required_acks, 1}]}
            ]},

            {client_options, [
                {queue_buffering_max_messages, 10000}
            ]}
        ]},

        {client_consumer_id, [

            {type, consumer},

            {group_id, <<"erlkaf_consumer">>},
            {topics, [
                {<<"benchmark">>, [
                    {callback_module, module_topic1},
                    {callback_args, []},
                    {dispatch_mode, one_by_one}
                ]}
            ]},
            {topic_options, [
                {auto_offset_reset, smallest}
            ]},

            {client_options, [
                {offset_store_method, broker}
            ]}
        ]}
    ]}
]}
```

`global_client_options` will apply to all clients defined. In case the global property it's defined as well in the client
options it's value will be overwritten. 

For producers in case you don't need to customize the topic properties you can omit the `topics` property as time they will
be created on the first produce operation with default settings. 

### Producer API

The following example will create a producer client with id `client_producer` which sends also delivery reports to the same module.

In order to receive the delivery reports you need to implement the `erlkaf_producer_callbacks` protocol or to setup a function with
arity 2 into `delivery_report_callback` config (for example: `{delivery_report_callback, fun(DeliveryStatus, Message) -> .. end}`).

The function specified into delivery report callback is called async from another process (each producer has it's own process
from where it's dispatching the delivery reports)

In case you want to define any properties to the topic where you are going to produce messages, you need to create the topic object attached to the client. To do this you can use the 
`erlkaf:create_topic` method. This needs to be done before any produce operation which will lead in create the topic with default settings.

```erlang
-module(test_producer).

-define(TOPIC, <<"benchmark">>).

-export([
    delivery_report/2,
    create_producer/0,
    produce/2
]).

-behaviour(erlkaf_producer_callbacks).

delivery_report(DeliveryStatus, Message) ->
    io:format("received delivery report: ~p ~n", [{DeliveryStatus, Message}]),
    ok.

create_producer() ->
    erlkaf:start(),

    ProducerConfig = [
        {bootstrap_servers, <<"broker1:9092">>},
        {delivery_report_only_error, false},
        {delivery_report_callback, ?MODULE}
    ],
    ok = erlkaf:create_producer(client_producer, ProducerConfig),
    ok = erlkaf:create_topic(client_producer, ?TOPIC, [{request_required_acks, 1}]).

produce(Key, Value) ->
    ok = erlkaf:produce(client_producer, ?TOPIC, Key, Value).
``` 

You can call those like:

```erlang
ok = test_producer:create_producer().
test_producer:produce(<<"key1">>, <<"val1">>).
```

And you will get into the console the delivery reports:
 
```erlang   
received delivery report: {ok, {erlkaf_msg,<<"benchmark">>,4,6172,<<"key1">>,<<"val1">>}} 
```    

In case you are not interested in the delivery reports don't specify any callback, or in case you want to receive the 
delivery reports only in case of errors you have to specify a callback and set `delivery_report_only_error` on `true`.

##### Message queues

The produced messages are queued in memory based on (`queue_buffering_max_messages` and `queue_buffering_max_kbytes`), 
until they are delivered and acknowledged by the kafka broker, once the memory queue it's full there are three options defined by (`queue_buffering_overflow_strategy`) :

- `local_disk_queue` (default) - records are persisted on local disk and once there is enough space are sent to memory queue
- `block_calling_process` - calling process it's blocked until there is enough room into the memory queue 
- `drop_records` - records are dropped in case the memory queue is full
 
### Consumer API:

The following example creates a consumer group that will consume messages from `benchmark` topic. For each topic and partition
the application will spawn an erlang process that will pull the messages. 

Each time the rebalance process takes place the process it's restarted so the `init/4` method will be called again. In case the
`handle_message/2` it's returning `{ok, State}` then the message is considered processed and the offset is stored to be committed
based on `auto_commit_interval_ms` setting. In case you want to mark an error but also to update the state you can use `{error, Reason, NewState}`,
basically the event for the same message will be triggered again but with the new state.

```erlang
-module(test_consumer).

-include("erlkaf.hrl").

-define(TOPICS, [
    {<<"benchmark">>, [
       {callback_module, ?MODULE},
       {callback_args, []}, % default [] (you can skip it)
       {dispatch_mode, one_by_one} % default one_by_one (you can skip it)
   ]}
]).

-export([
    create_consumer/0,
    init/4,
    handle_message/2
]).

-behaviour(erlkaf_consumer_callbacks).

-record(state, {}).

create_consumer() ->
    erlkaf:start(),
    ClientId = client_consumer,
    GroupId = <<"erlkaf_consumer">>,
    ClientCfg = [{bootstrap_servers, <<"broker1:9092">>}],
    TopicConf = [{auto_offset_reset, smallest}],
    ok = erlkaf:create_consumer_group(ClientId, GroupId, ?TOPICS, ClientCfg, TopicConf).

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
ok = test_consumer:create_consumer().
```

### Statistics

In order to get statistics you can register the `stats_callback` callback which is called every `statistics_interval_ms` (default 0 which means
statistics are disabled). The granularity is 1000 ms. Also you can poll the stats for each client using `erlkaf:get_stats/1`.

### Configs

The list of all available properties of client and topics are described [here][2]

[1]:https://github.com/edenhill/librdkafka
[2]:https://github.com/silviucpp/erlkaf/blob/master/CONFIGURATION.md
[3]:https://github.com/silviucpp/erlkaf/blob/master/CHANGELOG.md
