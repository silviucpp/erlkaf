#include "erlkaf_producer.h"
#include "nif_utils.h"
#include "macros.h"
#include "topicmanager.h"
#include "erlkaf_nif.h"
#include "erlkaf_config.h"
#include "erlkaf_logger.h"
#include "rdkafka.h"
#include "queuecallbacksdispatcher.h"
#include "erlkaf_message.h"

#include <string.h>
#include <memory>
#include <string>
#include <future>

namespace {

struct enif_producer
{
    rd_kafka_t* kf;
    TopicManager* topics;
    ErlNifPid owner_pid;
    std::future<bool>* closed_future;
};

bool cleanup_producer(enif_producer* producer, bool stop_feedback)
{
    rd_kafka_flush(producer->kf, 30000);

    if(stop_feedback)
    {
        ErlNifEnv* env = enif_alloc_env();
        enif_send(NULL, &producer->owner_pid, env, ATOMS.atomClientStopped);
        enif_free_env(env);
    }

    return true;
}

void delivery_report_callback (rd_kafka_t* rk, const rd_kafka_message_t* msg, void* data)
{
    UNUSED(rk);
    enif_producer* producer = static_cast<enif_producer*>(data);
    ErlNifEnv* env = enif_alloc_env();

    ERL_NIF_TERM status = msg->err == 0 ? ATOMS.atomOk : make_error(env, rd_kafka_err2str(msg->err));
    enif_send(NULL, &producer->owner_pid, env, enif_make_tuple3(env, ATOMS.atomDeliveryReport, status, make_message_nif(env, msg)));
    enif_free_env(env);
}

int stats_callback(rd_kafka_t *rk, char *json, size_t json_len, void *opaque)
{
    UNUSED(rk);

    enif_producer* producer = static_cast<enif_producer*>(opaque);

    if(producer->owner_pid.pid == 0)
        return 0;

    ErlNifEnv* env = enif_alloc_env();
    ERL_NIF_TERM stats = make_binary(env, json, json_len);
    enif_send(NULL, &producer->owner_pid, env, enif_make_tuple2(env, ATOMS.atomStats, stats));
    enif_free_env(env);
    return 0;
}

bool populate_headers(ErlNifEnv* env, ERL_NIF_TERM headers_term, rd_kafka_headers_t* out)
{
    ERL_NIF_TERM head;
    const ERL_NIF_TERM *items;
    int arity;
    ErlNifBinary key;
    ErlNifBinary value;

    while(enif_get_list_cell(env, headers_term, &head, &headers_term))
    {
        if(!enif_get_tuple(env, head, &arity, &items) || arity != 2)
            return false;

        if (!get_binary(env, items[0], &key) || !get_binary(env, items[1], &value))
            return false;

        if(rd_kafka_header_add(out, reinterpret_cast<const char*>(key.data), key.size, value.data, value.size) != RD_KAFKA_RESP_ERR_NO_ERROR)
            return false;
    }

    return true;
}

}  // namespace

void enif_producer_free(ErlNifEnv* env, void* obj)
{
    UNUSED(env);

    enif_producer* producer = static_cast<enif_producer*>(obj);

    if(producer->closed_future)
    {
        producer->closed_future->get();
        delete producer->closed_future;
    }
    else if(producer->kf)
    {
        erlkaf_data* data = static_cast<erlkaf_data*>(enif_priv_data(env));
        data->notifier_->remove(producer->kf);
        cleanup_producer(producer, false);
    }

    if(producer->topics)
        delete producer->topics;

    if(producer->kf)
        rd_kafka_destroy(producer->kf);
}

ERL_NIF_TERM enif_producer_topic_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UNUSED(argc);

    std::string topic_name;
    enif_producer* producer;

    erlkaf_data* data = static_cast<erlkaf_data*>(enif_priv_data(env));

    if(!enif_get_resource(env, argv[0], data->res_producer,  reinterpret_cast<void**>(&producer)))
        return make_badarg(env);

    if(!get_string(env, argv[1], &topic_name))
        return make_badarg(env);

    scoped_ptr(config, rd_kafka_topic_conf_t, rd_kafka_topic_conf_new(), rd_kafka_topic_conf_destroy);

    ERL_NIF_TERM parse_result = parse_topic_config(env, argv[2], config.get());

    if(parse_result != ATOMS.atomOk)
        return parse_result;

    bool already_exist;

    if(!producer->topics->AddTopic(topic_name, config.get(), &already_exist))
        return make_error(env, already_exist ? "topic already exist" : "failed to create topic");

    config.release();
    return ATOMS.atomOk;
}

ERL_NIF_TERM enif_producer_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UNUSED(argc);

    bool has_dr_callback;

    if(!get_boolean(argv[0], &has_dr_callback))
        return make_badarg(env);

    scoped_ptr(config, rd_kafka_conf_t, rd_kafka_conf_new(), rd_kafka_conf_destroy);

    ERL_NIF_TERM parse_result = parse_kafka_config(env, argv[1], config.get());

    if(parse_result != ATOMS.atomOk)
        return parse_result;

    rd_kafka_conf_set_log_cb(config.get(), logger_callback);
    rd_kafka_conf_set_stats_cb(config.get(), stats_callback);

    if(has_dr_callback)
        rd_kafka_conf_set_dr_msg_cb(config.get(), delivery_report_callback);

    erlkaf_data* data = static_cast<erlkaf_data*>(enif_priv_data(env));

    scoped_ptr(producer, enif_producer, static_cast<enif_producer*>(enif_alloc_resource(data->res_producer, sizeof(enif_producer))), enif_release_resource);

    if(!producer.get())
        return make_error(env, "failed to alloc producer");

    memset(producer.get(), 0, sizeof(enif_producer));

    rd_kafka_conf_set_opaque(config.get(), producer.get());

    char errstr[512];
    scoped_ptr(rk, rd_kafka_t, rd_kafka_new(RD_KAFKA_PRODUCER, config.get(), errstr, sizeof(errstr)), rd_kafka_destroy);

    if (!rk)
        return make_error(env, errstr);

    config.release();

    producer->topics = new TopicManager(rk.get());
    producer->kf = rk.release();
    producer->closed_future = nullptr;

    data->notifier_->watch(producer->kf, false);

    ERL_NIF_TERM term = enif_make_resource(env, producer.get());
    enif_release_resource(producer.get());

    producer.release();

    return enif_make_tuple2(env, ATOMS.atomOk, term);
}

ERL_NIF_TERM enif_producer_set_owner(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UNUSED(argc);
    erlkaf_data* data = static_cast<erlkaf_data*>(enif_priv_data(env));

    enif_producer* producer;

    if(!enif_get_resource(env, argv[0], data->res_producer,  reinterpret_cast<void**>(&producer)))
        return make_badarg(env);

    if(!enif_get_local_pid(env, argv[1], &producer->owner_pid))
        return make_badarg(env);

    return ATOMS.atomOk;
}

ERL_NIF_TERM enif_producer_cleanup(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UNUSED(argc);

    erlkaf_data* data = static_cast<erlkaf_data*>(enif_priv_data(env));
    enif_producer* producer;

    if(!enif_get_resource(env, argv[0], data->res_producer,  reinterpret_cast<void**>(&producer)))
        return make_badarg(env);

    data->notifier_->remove(producer->kf);
    producer->closed_future = new std::future<bool>(std::async(std::launch::async, cleanup_producer, producer, true));

    return ATOMS.atomOk;
}

ERL_NIF_TERM enif_get_metadata(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UNUSED(argc);

    erlkaf_data* data = static_cast<erlkaf_data*>(enif_priv_data(env));

    enif_producer* producer;

    if(!enif_get_resource(env, argv[0], data->res_producer,  reinterpret_cast<void**>(&producer)))
        return make_badarg(env);

    const struct rd_kafka_metadata *metadata;
    rd_kafka_resp_err_t err;

    /* Fetch metadata */
    err = rd_kafka_metadata(producer->kf, 1, NULL, &metadata, 5000);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        return make_error(env, "failed to get metadata");
    }

    ERL_NIF_TERM metadata_map = enif_make_new_map(env);
    ERL_NIF_TERM broker_arr[metadata->broker_cnt];

    for (int i = 0 ; i < metadata->broker_cnt ; i++) {
        ERL_NIF_TERM broker_map = enif_make_new_map(env);
        ERL_NIF_TERM id_term = enif_make_int(env, metadata->brokers[i].id);
        ERL_NIF_TERM host_term = make_binary(env, metadata->brokers[i].host, strlen(metadata->brokers[i].host));
        ERL_NIF_TERM port_term = enif_make_int64(env, metadata->brokers[i].port);
        enif_make_map_put(env, broker_map, ATOMS.atomId, id_term, &broker_map);
        enif_make_map_put(env, broker_map, ATOMS.atomHost, host_term, &broker_map);
        enif_make_map_put(env, broker_map, ATOMS.atomPort, port_term, &broker_map);
        broker_arr[i] = broker_map;
    }

    ERL_NIF_TERM broker_list = enif_make_list_from_array(env, broker_arr, metadata->broker_cnt);
    enif_make_map_put(env, metadata_map, ATOMS.atomBrokers, broker_list, &metadata_map);

    ERL_NIF_TERM topic_arr[metadata->topic_cnt];

    for (int i = 0 ; i < metadata->topic_cnt ; i++) {
        const struct rd_kafka_metadata_topic *t = &metadata->topics[i];
        if (t->err) {
            if (t->err == RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE)
                return make_error(env, "failed to get topic info");
        }
        ERL_NIF_TERM topic_map = enif_make_new_map(env);
        ERL_NIF_TERM topic_name_term = make_binary(env, t->topic, strlen(t->topic));
        enif_make_map_put(env, topic_map, ATOMS.atomName, topic_name_term, &topic_map);

        ERL_NIF_TERM partition_arr[t->partition_cnt];

        for (int j = 0 ; j < t->partition_cnt ; j++) {
            const struct rd_kafka_metadata_partition *p;
            p = &t->partitions[j];

            if (p->err)
                return make_error(env, "failed to get partition info");

            ERL_NIF_TERM partition_map = enif_make_new_map(env);
            ERL_NIF_TERM id_term = enif_make_int(env, p->id);
            ERL_NIF_TERM leader_term = enif_make_int(env, p->leader);

            ERL_NIF_TERM replica_arr[p->replica_cnt];
            for (int k = 0 ; k < p->replica_cnt ; k++) {
                ERL_NIF_TERM replica_term = enif_make_int(env, p->replicas[k]);
                replica_arr[k] = replica_term;
            }

            ERL_NIF_TERM replica_list = enif_make_list_from_array(env, replica_arr, p->replica_cnt);

            ERL_NIF_TERM isr_arr[p->isr_cnt];
            for (int k = 0 ; k < p->isr_cnt ; k++) {
                ERL_NIF_TERM isr_term = enif_make_int(env, p->isrs[k]);
                isr_arr[k] = isr_term;
            }

            ERL_NIF_TERM isr_list = enif_make_list_from_array(env, isr_arr, p->isr_cnt);
            enif_make_map_put(env, partition_map, ATOMS.atomId, id_term, &partition_map);
            enif_make_map_put(env, partition_map, ATOMS.atomLeader, leader_term, &partition_map);
            enif_make_map_put(env, partition_map, ATOMS.atomReplicas, replica_list, &partition_map);
            enif_make_map_put(env, partition_map, ATOMS.atomIsrs, isr_list, &partition_map);
            partition_arr[j] = partition_map;
        }
        ERL_NIF_TERM partition_list = enif_make_list_from_array(env, partition_arr, t->partition_cnt);
        enif_make_map_put(env, topic_map, ATOMS.atomPartitions, partition_list, &topic_map);
        topic_arr[i] = topic_map;
    }

    ERL_NIF_TERM topic_list = enif_make_list_from_array(env, topic_arr, metadata->topic_cnt);
    enif_make_map_put(env, metadata_map, ATOMS.atomTopics, topic_list, &metadata_map);

    rd_kafka_metadata_destroy(metadata);

    return enif_make_tuple2(env, ATOMS.atomOk, metadata_map);
}

ERL_NIF_TERM enif_produce(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UNUSED(argc);

    erlkaf_data* data = static_cast<erlkaf_data*>(enif_priv_data(env));

    enif_producer* producer;
    std::string topic_name;
    int32_t partition;
    ErlNifBinary key;
    ErlNifBinary value;

    scoped_ptr(headers, rd_kafka_headers_t, NULL, rd_kafka_headers_destroy);

    if(!enif_get_resource(env, argv[0], data->res_producer,  reinterpret_cast<void**>(&producer)))
        return make_badarg(env);

    if(!get_string(env, argv[1], &topic_name))
        return make_badarg(env);

    if(!enif_get_int(env, argv[2], &partition))
        return make_badarg(env);

    if (!get_binary(env, argv[3], &key))
    {
        if(!enif_is_identical(ATOMS.atomUndefined, argv[3]))
            return make_badarg(env);

        memset(&key, 0, sizeof(ErlNifBinary));
    }

    if (!get_binary(env, argv[4], &value))
    {
        if(!enif_is_identical(ATOMS.atomUndefined, argv[4]))
            return make_badarg(env);

        memset(&value, 0, sizeof(ErlNifBinary));
    }

    if(!enif_is_identical(argv[5], ATOMS.atomUndefined))
    {
        uint32_t length;

        if(!enif_get_list_length(env, argv[5], &length))
            return make_badarg(env);

        if(length > 0)
        {
            headers.reset(rd_kafka_headers_new(length));
            if(!populate_headers(env, argv[5], headers.get()))
                return make_badarg(env);
        }
    }

    if(!headers.get())
    {
        rd_kafka_topic_t* topic = producer->topics->GetOrCreateTopic(topic_name);

        if(topic == NULL)
            return make_error(env, "failed to create topic object");

        if (rd_kafka_produce(topic, partition, RD_KAFKA_MSG_F_COPY, value.data, value.size, key.data, key.size, NULL) != 0)
            return make_error(env, enif_make_int(env, rd_kafka_last_error()));
    }
    else
    {
        rd_kafka_resp_err_t result = rd_kafka_producev(producer->kf,
                                                       RD_KAFKA_V_TOPIC(topic_name.c_str()),
                                                       RD_KAFKA_V_PARTITION(partition),
                                                       RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                                                       RD_KAFKA_V_VALUE(value.data, value.size),
                                                       RD_KAFKA_V_KEY(key.data, key.size),
                                                       RD_KAFKA_V_HEADERS(headers.get()),
                                                       RD_KAFKA_V_END);

        if(result != RD_KAFKA_RESP_ERR_NO_ERROR)
            return make_error(env, enif_make_int(env, result));
        else
            headers.release();
    }

    return ATOMS.atomOk;
}
