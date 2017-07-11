
-record(erlkaf_msg, {
    topic,
    partition,
    offset,
    key,
    value
}).

-define(DEFULT_PARTITIONER, -1).

-type client_id() :: atom().
-type topic_id() :: atom().
-type reason() :: any().
-type key() :: undefined | binary().
-type partition() :: ?DEFULT_PARTITIONER | integer().


-type compression_codec() :: none | gzip | snappy | lz4 | inherit.
-type offset_reset() :: smallest | earliest | beginning | largest | latest.
-type offset_store_method() :: file | broker.
-type ip_family() :: any| v4| v6.
-type security_protocol() :: plaintext | ssl | sasl_plaintext | sasl_ssl.

-type topic_option() ::
    {request_required_acks, integer()} |
    {request_timeout_ms, integer()} |
    {message_timeout_ms, integer()} |
    {compression_codec, compression_codec()} |
    {auto_commit_interval_ms, integer()} |
    {auto_offset_reset, offset_reset()} |
    {offset_store_path, binary()} |
    {offset_store_sync_interval_ms, integer()} |
    {offset_store_method, offset_store_method()}.

-type client_option() ::
    {client_id, binary()} |
    {bootstrap_servers, binary()} |
    {message_max_bytes, integer()} |
    {message_copy_max_bytes, integer()} |
    {receive_message_max_bytes, integer()} |
    {max_in_flight, integer()} |
    {metadata_request_timeout_ms, integer()} |
    {topic_metadata_refresh_interval_ms, integer()} |
    {metadata_max_age_ms, integer()} |
    {topic_metadata_refresh_fast_interval_ms, integer()} |
    {topic_metadata_refresh_fast_cnt, integer()} |
    {topic_metadata_refresh_sparse, boolean()} |
    {topic_blacklist, binary()} |
    {socket_timeout_ms, integer()} |
    {socket_send_buffer_bytes, integer()} |
    {socket_receive_buffer_bytes, integer()} |
    {socket_keepalive_enable, boolean()} |
    {socket_nagle_disable, boolean()} |
    {socket_max_fails, integer()} |
    {broker_address_ttl, integer()} |
    {broker_address_family, ip_family()} |
    {reconnect_backoff_jitter_ms, integer()} |
    {statistics_interval_ms, integer()} |
    {log_level, integer()} |
    {log_queue, boolean()} |
    {log_thread_name, boolean()} |
    {log_connection_close, boolean()} |
    {api_version_request, boolean()} |
    {api_version_fallback_ms, integer()} |
    {broker_version_fallback, boolean()} |
    {security_protocol, security_protocol()} |
    {ssl_cipher_suites, binary()} |
    {ssl_key_location, binary()} |
    {ssl_key_password, binary()} |
    {ssl_certificate_location, binary()} |
    {ssl_ca_location, binary()} |
    {ssl_crl_location, binary()} |
    {sasl_mechanisms, binary()} |
    {sasl_kerberos_service_name, binary()} |
    {sasl_kerberos_principal, binary()} |
    {sasl_kerberos_kinit_cmd, binary()} |
    {sasl_kerberos_keytab, binary()} |
    {sasl_kerberos_min_time_before_relogin, integer()} |
    {sasl_username, binary()} |
    {sasl_password, binary()} |
    {session_timeout_ms, integer()} |
    {heartbeat_interval_ms, integer()} |
    {coordinator_query_interval_ms, integer()} |
    {auto_commit_interval_ms, integer()} |
    {queued_min_messages, integer()} |
    {queued_max_messages_kbytes, integer()} |
    {fetch_wait_max_ms, integer()} |
    {fetch_message_max_bytes, integer()} |
    {max_partition_fetch_bytes, integer()} |
    {fetch_min_bytes, integer()} |
    {fetch_error_backoff_ms, integer()} |
    {offset_store_method, offset_store_method()} |
    {check_crcs, boolean()} |
    {queue_buffering_max_messages, integer()} |
    {queue_buffering_max_kbytes, integer()} |
    {queue_buffering_max_ms, integer()} |
    {message_send_max_retries, integer()} |
    {retry_backoff_ms, integer()} |
    {compression_codec, compression_codec()} |
    {batch_num_messages, integer()} |
    {delivery_report_only_error, boolean()}.

