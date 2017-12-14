## Client global configs

C/P legend: C = Consumer, P = Producer, * = both

Property                                 | C/P | Range              |       Default | Description
-----------------------------------------|-----|--------------------|--------------:|--------------------------
debug                                    |  *  | generic, broker, topic, metadata, queue, msg, protocol, cgrp, security, fetch, feature, interceptor, plugin, all | undefined | A comma-separated list of debug contexts to enable. Debugging the Producer: broker,topic,msg. Consumer: cgrp,topic,fetch
client_id                                |  *  |                    |       rdkafka | Client identifier
bootstrap_servers                        |  *  |                    |               | Initial list of brokers host:port separated by comma
message_max_bytes                        |  *  | 1000 .. 1000000000 |       1000000 | Maximum transmit message size.
message_copy_max_bytes                   |  *  | 0 .. 1000000000    |         65535 | Maximum size for message to be copied to buffer. Messages larger than this will be passed by reference (zero-copy) at the expense of larger iovecs
receive_message_max_bytes                |  *  | 1000 .. 1000000000 |     100000000 | Maximum receive message size. This is a safety precaution to avoid memory exhaustion in case of protocol hickups. The value should be at least `fetch_message_max_bytes` * number of partitions consumed from + messaging overhead (e.g. 200000 bytes)
max_in_flight                            |  *  | 1 .. 1000000       |       1000000 | Maximum number of in-flight requests the client will send. This setting applies per broker connection
metadata_request_timeout_ms              |  *  | 10 .. 900000       |         60000 | Non-topic request timeout in milliseconds. This is for metadata requests
topic_metadata_refresh_interval_ms       |  *  | -1 .. 3600000      |        300000 | Topic metadata refresh interval in milliseconds. The metadata is automatically refreshed on error and connect. Use -1 to disable the intervalled refresh
metadata_max_age_ms                      |  *  | 1 .. 86400000      |            -1 | Metadata cache max age. Defaults to `metadata_refresh_interval_ms` * 3
topic_metadata_refresh_fast_interval_ms  |  *  | 1 .. 60000         |           250 | When a topic loses its leader a new metadata request will be enqueued with this initial interval, exponentially increasing until the topic metadata has been refreshed. This is used to recover quickly from transitioning leader brokers
topic_metadata_refresh_sparse            |  *  | true, false        |          true | Sparse metadata requests (consumes less network bandwidth)
topic_blacklist                          |  *  |                    |               | Topic blacklist, a comma-separated list of regular expressions for matching topic names that should be ignored in broker metadata information as if the topics did not exist
socket_timeout_ms                        |  *  | 10 .. 300000       |         60000 | Timeout for network requests
socket_send_buffer_bytes                 |  *  | 0 .. 100000000     |             0 | Broker socket send buffer size. System default is used if 0
socket_receive_buffer_bytes              |  *  | 0 .. 100000000     |             0 | Broker socket receive buffer size. System default is used if 0
socket_keepalive_enable                  |  *  | true, false        |         false | Enable TCP keep-alives (SO_KEEPALIVE) on broker sockets
socket_nagle_disable                     |  *  | true, false        |         false | Disable the Nagle algorithm (TCP_NODELAY)
socket_max_fails                         |  *  | 0 .. 1000000       |             3 | Disconnect from broker when this number of send failures (e.g., timed out requests) is reached. Disable with 0. NOTE: The connection is automatically re-established
broker_address_ttl                       |  *  | 0 .. 86400000      |          1000 | How long to cache the broker address resolving results (milliseconds)
broker_address_family                    |  *  | any, v4, v6        |           any | Allowed broker IP address families: any, v4, v6
reconnect_backoff_jitter_ms              |  *  | 0 .. 3600000       |           500 | Throttle broker reconnection attempts by this value +-50%
statistics_interval_ms                   |  *  | 0 .. 86400000      |             0 | Statistics emit interval. The application also needs to register a stats callback using `stats_callback` config. The granularity is 1000ms. A value of 0 disables statistics.
stats_callback                           |  *  | module or fun/2    |       undefined| A callback where stats are sent 
log_level                                |  *  | 0 .. 7             |             6 | Logging level (syslog(3) levels)
log_connection_close                     |  *  | true, false        |          true | Log broker disconnects. It might be useful to turn this off when interacting with 0.9 brokers with an aggressive `connection_max_idle_ms` value.
api_version_request                      |  *  | true, false        |          false | Request broker's supported API versions to adjust functionality to available protocol features. If set to false, or the ApiVersionRequest fails, the fallback version `broker_version_fallback` will be used. **NOTE**: Depends on broker version >=0.10.0. If the request is not supported by (an older) broker the `broker_version_fallback` fallback is used
api_version_fallback_ms                  |  *  | 0 .. 604800000     |       1200000 | Dictates how long the `broker_version_fallback` fallback is used in the case the ApiVersionRequest fails. **NOTE**: The ApiVersionRequest is only issued when a new connection to the broker is made (such as after an upgrade)
broker_version_fallback                  |  *  |                    |         0.9.0 | Older broker versions (<0.10.0) provides no way for a client to query for supported protocol features (ApiVersionRequest, see `api_version_request`) making it impossible for the client to know what features it may use. As a workaround a user may set this property to the expected broker version and the client will automatically adjust its feature set accordingly if the ApiVersionRequest fails (or is disabled). The fallback broker version will be used for `api_version_fallback_ms`. Valid values are: 0.9.0, 0.8.2, 0.8.1, 0.8.0.
security.protocol                        |  *  | plaintext, ssl, sasl_plaintext, sasl_ssl |     plaintext | Protocol used to communicate with brokers
ssl_cipher_suites                        |  *  |                 |               | A cipher suite is a named combination of authentication, encryption, MAC and key exchange algorithm used to negotiate the security settings for a network connection using TLS or SSL network protocol. See manual page for `ciphers(1)` and `SSL_CTX_set_cipher_list(3)
ssl_key_location                         |  *  |                 |               | Path to client's private key (PEM) used for authentication.
ssl_key_password                         |  *  |                 |               | Private key passphrase
ssl_certificate_location                 |  *  |                 |               | Path to client's public key (PEM) used for authentication.
ssl_ca_location                          |  *  |                 |               | File or directory path to CA certificate(s) for verifying the broker's key.
ssl_crl_location                         |  *  |                 |               | Path to CRL for verifying broker's certificate validity. 
sasl_mechanisms                          |  *  |                 |        GSSAPI | SASL mechanism to use for authentication. Supported: GSSAPI, PLAIN, SCRAM-SHA-256, SCRAM-SHA-512. **NOTE**: Despite the name only one mechanism must be configured.
sasl_kerberos_service_name               |  *  |                 |         kafka | Kerberos principal name that Kafka runs as
sasl_kerberos_principal                  |  *  |                 |   kafkaclient | This client's Kerberos principal name.
sasl_kerberos_kinit_cmd                  |  *  |                 |  | Full kerberos kinit command string, %{config.prop.name} is replaced by corresponding config object value, %{broker.name} returns the broker's hostname
sasl_kerberos_keytab                     |  *  |                 |               | Path to Kerberos keytab file. Uses system default if not set.**NOTE**: This is not automatically used but must be added to the template in `sasl_kerberos_kinit_cmd` as ` ... -t %{sasl_kerberos_keytab}`
sasl_kerberos_min_time_before_relogin    |  *  | 1 .. 86400000   |         60000 | Minimum time in milliseconds between key refresh attempts.
sasl_username                            |  *  |                 |               | SASL username for use with the PLAIN and SASL-SCRAM-.. mechanisms
sasl_password                            |  *  |                 |               | SASL password for use with the PLAIN and SASL-SCRAM-.. mechanism 
partition_assignment_strategy            |  *  |                 | range, roundrobin | Name of partition assignment strategy to use when elected group leader assigns partitions to group members
session_timeout_ms                       |  *  | 1 .. 3600000    |         30000 | Client group session and failure detection timeout
heartbeat_interval_ms                    |  *  | 1 .. 3600000    |          1000 | Group session keepalive heartbeat interval
coordinator_query_interval_ms            |  *  | 1 .. 3600000    |        600000 | How often to query for the current client group coordinator. If the currently assigned coordinator is down the configured query interval will be divided by ten to more quickly recover in case of coordinator reassignment
auto_commit_interval_ms                  |  C  | 0 .. 86400000   |          5000 | The frequency in milliseconds that the consumer offsets are committed (written) to offset storage. (0 = disable). This setting is used by the high-level consumer
queued_min_messages                      |  C  | 1 .. 10000000   |        100000 | Minimum number of messages per topic+partition in the local consumer queue
queued_max_messages_kbytes               |  C  | 1 .. 1000000000 |       1048576 | Maximum number of kilobytes per topic+partition in the local consumer queue. This value may be overshot by `fetch_message_max_bytes`
fetch_wait_max_ms                        |  C  | 0 .. 300000     |           100 | Maximum time the broker may wait to fill the response with `fetch_min_bytes`
fetch_message_max_bytes                  |  C  | 1 .. 1000000000 |       1048576 | Initial maximum number of bytes per topic+partition to request when fetching messages from the broker. If the client encounters a message larger than this value it will gradually try to increase it until the entire message can be fetched
fetch_min_bytes                          |  C  | 1 .. 100000000  |             1 | Minimum number of bytes the broker responds with. If fetch.wait.max.ms expires the accumulated data will be sent to the client regardless of this setting
fetch_error_backoff_ms                   |  C  | 0 .. 300000     |           500 | How long to postpone the next fetch request for a topic+partition in case of a fetch error
offset_store_method                      |  C  | none, file, broker |        broker | Offset commit store method: 'file' - local file store (offset.store.path, et.al), 'broker' - broker commit store (requires Apache Kafka 0.8.2 or later on the broker)
check_crcs                               |  C  | true, false     |         false | Verify CRC32 of consumed messages, ensuring no on-the-wire or on-disk corruption to the messages occurred. This check comes at slightly increased CPU usage
queue_buffering_max_messages             |  P  | 1 .. 10000000   |        100000 | Maximum number of messages allowed on the producer queue
queue_buffering_max_kbytes               |  P  | 1 .. 2097151    |       1048576 | Maximum number of kilobytes per topic+partition in the local consumer queue. This value may be overshot by fetch_message_max_bytes. This property has higher priority than queued_min_messages.
queue_buffering_max_ms                   |  P  | 0 .. 900000     |             0 | Maximum time, in milliseconds, for buffering data on the producer queue
queue_buffering_overflow_strategy        |  P  | local_disk_queue, block_calling_process, drop_records | local_disk_queue| What strategy to pick in case the memory queue is full: write messages on a local disk queue and send them in kafka when the in memory queue have enough space, block calling process until memory queue has enough space or drop the messages
message_send_max_retries                 |  P  | 0 .. 10000000   |             2 | How many times to retry sending a failing MessageSet. **Note:** retrying may cause reordering
retry_backoff_ms                         |  P  | 1 .. 300000     |           100 | The backoff time in milliseconds before retrying a message send
compression_codec                        |  P  | none, gzip, snappy, lz4 |          none | compression codec to use for compressing message sets. This is the default value for all topics, may be overriden by the topic configuration property `compression_codec`
batch_num_messages                       |  P  | 1 .. 1000000    |         10000 | Maximum number of messages batched in one MessageSet. The total MessageSet size is also limited by `message_max_bytes`
delivery_report_only_error               |  P  | true, false     |         false | Only provide delivery reports for failed messages
delivery_report_callback                 |  P  | module or fun/2 |       undefined| A callback where delivery reports are sent (`erlkaf_producer_callbacks` behaviour) 

## Topic configuration properties

C/P legend: C = Consumer, P = Producer, * = both

Property                                 | C/P | Range           |       Default | Description
-----------------------------------------|-----|-----------------|--------------:|--------------------------
request_required_acks                    |  P  | -1 .. 1000      |             1 | This field indicates how many acknowledgements the leader broker must receive from ISR brokers before responding to the request: *0*=Broker does not send any response/ack to client, *1*=Only the leader broker will need to ack the message, *-1* or *all*=broker will block until message is committed by all in sync replicas (ISRs) or broker's `in.sync.replicas` setting before sending response.
request_timeout_ms                       |  P  | 1 .. 900000     |          5000 | The ack timeout of the producer request in milliseconds. This value is only enforced by the broker and relies on `request_required_acks` being != 0
message_timeout_ms                       |  P  | 0 .. 900000     |        300000 | Local message timeout. This value is only enforced locally and limits the time a produced message waits for successful delivery. A time of 0 is infinite
compression_codec                        |  P  | none, gzip, snappy, lz4, inherit |       inherit | Compression codec to use for compressing message sets
auto_commit_interval_ms                  |  C  | 10 .. 86400000  |         60000 | The frequency in milliseconds that the consumer offsets are committed (written) to offset storage. This setting is used by the low-level legacy consumer
auto_offset_reset                        |  C  | smallest, earliest, beginning, largest, latest, end, error |       largest | Action to take when there is no initial offset in offset store or the desired offset is out of range: 'smallest','earliest' - automatically reset the offset to the smallest offset, 'largest','latest' - automatically reset the offset to the largest offset, 'error' - trigger an error.
offset_store_path                        |  C  |                 |             . | Path to local file for storing offsets. If the path is a directory a filename will be automatically generated in that directory based on the topic and partition
offset_store_sync_interval_ms            |  C  | -1 .. 86400000  |            -1 | fsync() interval for the offset file, in milliseconds. Use -1 to disable syncing, and 0 for immediate sync after each write
offset_store_method                      |  C  | file, broker    |        broker | Offset commit store method: 'file' - local file store (offset.store.path, et.al), 'broker' - broker commit store (requires "group.id" to be configured and Apache Kafka 0.8.2 or later on the broker.)

