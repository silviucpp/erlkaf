### Changelog:

#### v2.1.0

- Fix for kafka tombstones messages (https://github.com/silviucpp/erlkaf/issues/51)
- Add support for message timestamp (https://github.com/silviucpp/erlkaf/issues/41)

#### v2.0.9

- Fix build with OTP 25
- Fix create_consumer_group spec (https://github.com/silviucpp/erlkaf/pull/45)

#### v2.0.8

- Improve build scripts

#### v2.0.7

- Fix compilation error under elixir 1.13
- Fix crash when running under a debug compiled VM (https://github.com/silviucpp/erlkaf/issues/40)

#### v2.0.6

- Fix for message headers into delivery reports (#37)
- On Mac OS use openssl@1.1

#### v2.0.5

- Bug fixing https://github.com/silviucpp/erlkaf/issues/31
- Add API to get metadata from Kafka broker with dirty IO bound nif

#### v2.0.4

- Fix for processing messages that takes longer than max_poll_interval_ms

#### v2.0.3

- Upgrade to lager v3.9.2 (works on OTP 24)
- Add support for poll_idle_ms topic setting.

#### v2.0.2

- Upgrade to librdkafka v1.6.1
- Add more supported configs from librdkafka.

#### v2.0.1

- Fix OTP 23 build
- Fix for https://github.com/silviucpp/erlkaf/issues/21
- Fix memory access

#### v2.0.0

- Redesign the callback system. Instead using one thread per consumer/producer that polls for new events, now it's 
using one single thread for all producers and consumers that's notified when new events are available.
- Breaking backward compatibility: change the `create_consumer_group` function parameters, and consumer configs. More details below.

##### Upgrading from v1.X to v2.0.0

In version 1.x each consumer group could handle a list of topics but it couldn't specify a callback for each topic. 
Now the callbacks settings moved into the topics list as follow:

```erlang

% v1.x

{client_consumer_id, [

    {type, consumer},

    {group_id, <<"erlkaf_consumer">>},
    {callback_module, test_consumer},
    {callback_args, []},
    {topics, [<<"topic1">>, <<"topic2">>]},
    {topic_options, [
        {auto_offset_reset, smallest}
    ]},
    {client_options, []}
]}

% becomes in v2.0.0 or newer:

{client_consumer_id, [

    {type, consumer},

    {group_id, <<"erlkaf_consumer">>},
    {topics, [
        {<<"topic1">>, [
            {callback_module, module_topic1},
            {callback_args, []}
        ]},
        {<<"topic2">>, [
            {callback_module, module_topic2},
            {callback_args, []}
        ]}
    ]},

    {topic_options, [
        {auto_offset_reset, smallest}
    ]},
    {client_options, []}
]}

% Please notice that `callback_module` and `callback_args` were moved into the `topics` which 
% now it's a proplist where key is the topic name and value is the list of settings for the topic. 
```


#### v1.2.0

- Based on librdkafka v1.3.0
- Add new configs `isolation_level` and `plugin_library_paths`
- Set message_timeout_ms to 0
- Implemented exponential backoff retry policy on consumer

#### v1.1.9

- Fix hex package

#### v1.1.8

- Add zstd into the deps list
- Remove deprecated config `produce.offset.report`

#### v1.1.7 

- Based on librdkafka v1.0.1
- Removed configs: `queuing_strategy`, `offset_store_method`, `reconnect_backoff_jitter_ms`
- Added new configs: `reconnect_backoff_ms`, `reconnect_backoff_max_ms`, `max_poll_interval_ms`, `enable_idempotence`, `enable_gapless_guarantee`
- `get_stats` decodes json to maps instead of proplists

#### v1.1.6

- Fixed memory leaks on the consumer
- Fixed a segmentation fault caused when init handler is throwing exception on consumer
- Refactoring the entire consumer part

#### v1.1.5

- Add support for headers (requires broker version 0.11.0.0 or later)
- Code cleanup

#### v1.1.4

- Add support for dispatch_mode topic setting.
- Based on librdkafka v0.11.6

#### v1.1.3

- Add support for Trevis CI
- Remove plists from deps
- Available via HEX

#### v1.1.2

- Fix crash when stopping erlkaf while using consumers
- Update esq dependency

#### v1.1.1

- Add missing app dependency

#### v1.1.0

- Based on librdkafka v0.11.5
- Add support for the new broker configs: ssl_curves_list, ssl_sigalgs_list, ssl_keystore_location, ssl_keystore_password, fetch_max_bytes
- Add support for the new topic configs: queuing_strategy, compression_level, partitioner
- Fix build process on OSX High Sierra
- Upgrade deps to work on OTP 21 (thanks to Tomislav Trajakovic)

#### v1.0

- Initial implementation (both producer and consumer) supporting most of the features available in librdkafka.
- Based on librdkafka v0.11.3
- Tested on Mac OSX, Ubuntu 14.04 LTS, Ubuntu 16.04 LTS
