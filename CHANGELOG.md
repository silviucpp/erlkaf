### Changelog:

##### v2.0.0

- Based on librdkafka v1.3.0
- Add new configs `isolation_level` and `plugin_library_paths`

##### v1.1.9

- Fix hex package

##### v1.1.8

- Add zstd into the deps list
- Remove deprecated config `produce.offset.report`

##### v1.1.7 

- Based on librdkafka v1.0.1
- Removed configs: `queuing_strategy`, `offset_store_method`, `reconnect_backoff_jitter_ms`
- Added new configs: `reconnect_backoff_ms`, `reconnect_backoff_max_ms`, `max_poll_interval_ms`, `enable_idempotence`, `enable_gapless_guarantee`
- `get_stats` decodes json to maps instead of proplists

##### v1.1.6

- Fixed memory leaks on the consumer
- Fixed a segmentation fault caused when init handler is throwing exception on consumer
- Refactoring the entire consumer part

##### v1.1.5

- Add support for headers (requires broker version 0.11.0.0 or later)
- Code cleanup

##### v1.1.4

- Add support for dispatch_mode topic setting.
- Based on librdkafka v0.11.6

##### v1.1.3

- Add support for Trevis CI
- Remove plists from deps
- Available via HEX

##### v1.1.2

- Fix crash when stopping erlkaf while using consumers
- Update esq dependency

##### v1.1.1

- Add missing app dependency

##### v1.1.0

- Based on librdkafka v0.11.5
- Add support for the new broker configs: ssl_curves_list, ssl_sigalgs_list, ssl_keystore_location, ssl_keystore_password, fetch_max_bytes
- Add support for the new topic configs: queuing_strategy, compression_level, partitioner
- Fix build process on OSX High Sierra
- Upgrade deps to work on OTP 21 (thanks to Tomislav Trajakovic)

##### v1.0

- Initial implementation (both producer and consumer) supporting most of the features available in librdkafka.
- Based on librdkafka v0.11.3
- Tested on Mac OSX, Ubuntu 14.04 LTS, Ubuntu 16.04 LTS
