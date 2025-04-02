-module(erlkaf_config).

% librdkafka configs are described here:
% https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md

-export([
    convert_topic_config/1,
    convert_kafka_config/1
]).

convert_topic_config(Conf) ->
    try
        filter_topic_config(Conf, [], [])
    catch
        _:Reason ->
            Reason
    end.

convert_kafka_config(Conf) ->
    try
        filter_kafka_config(Conf, [], [])
    catch
        _:Reason ->
            Reason
    end.

filter_topic_config([{K, V} = H | T], ErlkafAcc, RdKafkaConf) ->
    case is_erlkaf_topic_config(K, V) of
        true ->
            filter_topic_config(T, [H | ErlkafAcc] , RdKafkaConf);
        _ ->
            filter_topic_config(T, ErlkafAcc , [to_librdkafka_topic_config(K, V) | RdKafkaConf])
    end;
filter_topic_config([], ErlkafAcc, RdKafkaConf) ->
    {ok, ErlkafAcc, RdKafkaConf}.

filter_kafka_config([{K, V} = H | T], ErlkafAcc, RdKafkaConf) ->
    case is_erlkaf_config(K, V) of
        true ->
            filter_kafka_config(T, [H | ErlkafAcc] , RdKafkaConf);
        _ ->
            filter_kafka_config(T, ErlkafAcc , [to_librdkafka_config(K, V) | RdKafkaConf])
    end;
filter_kafka_config([], ErlkafAcc, RdKafkaConf) ->
    {ok, ErlkafAcc, RdKafkaConf}.

% topic related configs

is_erlkaf_topic_config(_, _) ->
    false.

to_librdkafka_topic_config(request_required_acks, V) ->
    {<<"request.required.acks">>, erlkaf_utils:to_binary(V)};
to_librdkafka_topic_config(request_timeout_ms, V) ->
    {<<"request.timeout.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_topic_config(message_timeout_ms, V) ->
    {<<"message.timeout.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_topic_config(partitioner, V) ->
    {<<"partitioner">>, erlkaf_utils:to_binary(V)};
to_librdkafka_topic_config(compression_codec, V) ->
    {<<"compression.codec">>, erlkaf_utils:to_binary(V)};
to_librdkafka_topic_config(compression_level, V) ->
    {<<"compression.level">>, erlkaf_utils:to_binary(V)};
to_librdkafka_topic_config(auto_commit_interval_ms, V) ->
    {<<"auto.commit.interval.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_topic_config(auto_offset_reset, V) ->
    {<<"auto.offset.reset">>, erlkaf_utils:to_binary(V)};
to_librdkafka_topic_config(offset_store_path, V) ->
    {<<"offset.store.path">>, erlkaf_utils:to_binary(V)};
to_librdkafka_topic_config(offset_store_sync_interval_ms, V) ->
    {<<"offset.store.sync.interval.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_topic_config(consume_callback_max_messages, V) ->
    {<<"consume.callback.max.messages">>, erlkaf_utils:to_binary(V)};
to_librdkafka_topic_config(K, V) ->
    throw({error, {options, {K, V}}}).

% client related configs

is_erlkaf_config(delivery_report_callback = K, V) ->
    check_callback(K, V, 2);
is_erlkaf_config(stats_callback = K, V) ->
    check_callback(K, V, 2);
is_erlkaf_config(oauthbearer_token_refresh_callback = K, V) ->
    check_callback(K, V, 1);
is_erlkaf_config(local_queue_path = K, []) -> throw({error, {options, {K, []}}});
is_erlkaf_config(local_queue_path = K, V) ->
    case io_lib:latin1_char_list(V) of
        true -> true;
        _ -> throw({error, {options, {K, V}}})
    end;
is_erlkaf_config(queue_buffering_overflow_strategy = K, V) ->
    case V of
        local_disk_queue ->
            true;
        block_calling_process ->
            true;
        drop_records ->
            true;
        _ ->
            throw({error, {options, {K, V}}})
    end;
is_erlkaf_config(poll_consumer = _K, V) ->
    is_boolean(V);
is_erlkaf_config(_, _) ->
    false.

to_librdkafka_config(builtin_features, V) ->
    {<<"builtin_features">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(debug, V) ->
    {<<"debug">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(connections_max_idle_ms, V) ->
    {<<"connections.max.idle.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(client_id, V) ->
    {<<"client.id">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(bootstrap_servers, V) ->
    {<<"bootstrap.servers">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(message_max_bytes, V) ->
    {<<"message.max.bytes">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(message_copy_max_bytes, V) ->
    {<<"message.copy.max.bytes">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(receive_message_max_bytes, V) ->
    {<<"receive.message.max.bytes">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(max_in_flight, V) ->
    {<<"max.in.flight">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(metadata_request_timeout_ms, V) ->
    {<<"metadata.request.timeout.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(topic_metadata_refresh_interval_ms, V) ->
    {<<"topic.metadata.refresh.interval.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(metadata_max_age_ms, V) ->
    {<<"metadata.max.age.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(topic_metadata_refresh_fast_interval_ms, V) ->
    {<<"topic.metadata.refresh.fast.interval.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(topic_metadata_refresh_sparse, V) ->
    {<<"topic.metadata.refresh.sparse">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(topic_metadata_propagation_max_ms, V) ->
    {<<"topic.metadata.propagation.max.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(topic_blacklist, V) ->
    {<<"topic.blacklist">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(socket_timeout_ms, V) ->
    {<<"socket.timeout.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(socket_send_buffer_bytes, V) ->
    {<<"socket.send.buffer.bytes">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(socket_receive_buffer_bytes, V) ->
    {<<"socket.receive.buffer.bytes">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(socket_keepalive_enable, V) ->
    {<<"socket.keepalive.enable">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(socket_nagle_disable, V) ->
    {<<"socket.nagle.disable">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(socket_max_fails, V) ->
    {<<"socket.max.fails">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(broker_address_ttl, V) ->
    {<<"broker.address.ttl">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(broker_address_family, V) ->
    {<<"broker.address.family">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(reconnect_backoff_ms, V) ->
    {<<"reconnect.backoff.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(reconnect_backoff_max_ms, V) ->
    {<<"reconnect.backoff.max.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(statistics_interval_ms, V) ->
    {<<"statistics.interval.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(enabled_events, V) ->
    {<<"enabled.events">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(log_level, V) ->
    {<<"log_level">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(log_queue, V) ->
    {<<"log.queue">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(log_thread_name, V) ->
    {<<"log.thread.name">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(enable_random_seed, V) ->
    {<<"enable.random.seed">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(log_connection_close, V) ->
    {<<"log.connection.close">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(api_version_request, V) ->
    {<<"api.version.request">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(api_version_request_timeout_ms, V) ->
    {<<"api.version.request.timeout.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(api_version_fallback_ms, V) ->
    {<<"api.version.fallback.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(broker_version_fallback, V) ->
    {<<"broker.version.fallback">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(security_protocol, V) ->
    {<<"security.protocol">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(ssl_cipher_suites, V) ->
    {<<"ssl.cipher.suites">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(ssl_curves_list, V) ->
    {<<"ssl.curves.list">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(ssl_sigalgs_list, V) ->
    {<<"ssl.sigalgs.list">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(ssl_key_location, V) ->
    {<<"ssl.key.location">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(ssl_key_password, V) ->
    {<<"ssl.key.password">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(ssl_key_pem, V) ->
    {<<"ssl.key.pem">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(ssl_certificate_location, V) ->
    {<<"ssl.certificate.location">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(ssl_certificate_pem, V) ->
    {<<"ssl.certificate.pem">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(ssl_ca_location, V) ->
    {<<"ssl.ca.location">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(ssl_crl_location, V) ->
    {<<"ssl.crl.location">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(ssl_keystore_location, V) ->
    {<<"ssl.keystore.location">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(ssl_keystore_password, V) ->
    {<<"ssl.keystore.password">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(enable_ssl_certificate_verification, V) ->
    {<<"enable.ssl.certificate.verification">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(ssl_endpoint_identification_algorithm, V) ->
    {<<"ssl.endpoint.identification.algorithm">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(sasl_mechanisms, V) ->
    {<<"sasl.mechanisms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(sasl_kerberos_service_name, V) ->
    {<<"sasl.kerberos.service.name">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(sasl_kerberos_principal, V) ->
    {<<"sasl.kerberos.principal">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(sasl_kerberos_kinit_cmd, V) ->
    {<<"sasl.kerberos.kinit.cmd">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(sasl_kerberos_keytab, V) ->
    {<<"sasl.kerberos.keytab">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(sasl_kerberos_min_time_before_relogin, V) ->
    {<<"sasl.kerberos.min.time.before.relogin">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(sasl_username, V) ->
    {<<"sasl.username">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(sasl_password, V) ->
    {<<"sasl.password">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(sasl_oauthbearer_config, V) ->
    {<<"sasl.oauthbearer.config">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(enable_sasl_oauthbearer_unsecure_jwt, V) ->
    {<<"enable.sasl.oauthbearer.unsecure.jwt">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(sasl_oauthbearer_method, V) ->
    {<<"sasl.oauthbearer.method">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(sasl_oauthbearer_client_id, V) ->
    {<<"sasl.oauthbearer.client.id">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(sasl_oauthbearer_client_secret, V) ->
    {<<"sasl.oauthbearer.client.secret">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(sasl_oauthbearer_scope, V) ->
    {<<"sasl.oauthbearer.scope">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(sasl_oauthbearer_extensions, V) ->
    {<<"sasl.oauthbearer.extensions">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(sasl_oauthbearer_token_endpoint_url, V) ->
    {<<"sasl.oauthbearer.token.endpoint.url">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(group_instance_id, V) ->
    {<<"group.instance.id">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(session_timeout_ms, V) ->
    {<<"session.timeout.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(partition_assignment_strategy, V) ->
    {<<"partition.assignment.strategy">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(heartbeat_interval_ms, V) ->
    {<<"heartbeat.interval.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(group_protocol_type, V) ->
    {<<"group.protocol.type">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(coordinator_query_interval_ms, V) ->
    {<<"coordinator.query.interval.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(max_poll_interval_ms, V) ->
    {<<"max.poll.interval.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(auto_commit_interval_ms, V) ->
    {<<"auto.commit.interval.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(queued_min_messages, V) ->
    {<<"queued.min.messages">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(queued_max_messages_kbytes, V) ->
    {<<"queued.max.messages.kbytes">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(fetch_wait_max_ms, V) ->
    {<<"fetch.wait.max.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(fetch_message_max_bytes, V) ->
    {<<"fetch.message.max.bytes">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(fetch_max_bytes, V) ->
    {<<"fetch.max.bytes">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(fetch_min_bytes, V) ->
    {<<"fetch.min.bytes">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(fetch_error_backoff_ms, V) ->
    {<<"fetch.error.backoff.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(allow_auto_create_topics, V) ->
    {<<"allow.auto.create.topics">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(client_rack, V) ->
    {<<"client.rack">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(transactional_id, V) ->
    {<<"transactional.id">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(transaction_timeout_ms, V) ->
    {<<"transaction.timeout.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(check_crcs, V) ->
    {<<"check.crcs">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(isolation_level, V) ->
    {<<"isolation.level">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(enable_idempotence, V) ->
    {<<"enable.idempotence">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(enable_gapless_guarantee, V) ->
    {<<"enable.gapless.guarantee">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(queue_buffering_max_messages, V) ->
    {<<"queue.buffering.max.messages">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(queue_buffering_max_kbytes, V) ->
    {<<"queue.buffering.max.kbytes">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(queue_buffering_max_ms, V) ->
    {<<"queue.buffering.max.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(message_send_max_retries, V) ->
    {<<"message.send.max.retries">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(retry_backoff_ms, V) ->
    {<<"retry.backoff.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(queue_buffering_backpressure_threshold, V) ->
    {<<"queue.buffering.backpressure.threshold">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(compression_codec, V) ->
    {<<"compression.codec">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(batch_num_messages, V) ->
    {<<"batch.num.messages">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(batch_size, V) ->
    {<<"batch.size">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(delivery_report_only_error, V) ->
    {<<"delivery.report.only.error">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(plugin_library_paths, V) ->
    {<<"plugin.library.paths">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(sticky_partitioning_linger_ms, V) ->
    {<<"sticky.partitioning.linger.ms">>, erlkaf_utils:to_binary(V)};
to_librdkafka_config(K, V) ->
    throw({error, {options, {K, V}}}).

check_callback(K, V, Arity) ->
    case is_function(V, Arity) orelse is_atom(V) of
        false ->
            throw({error, {options, {K, V}}});
        _ ->
            true
    end.







