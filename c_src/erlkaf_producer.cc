#include "erlkaf_producer.h"
#include "nif_utils.h"
#include "macros.h"
#include "topicmanager.h"
#include "erlkaf_nif.h"
#include "erlkaf_config.h"
#include "erlkaf_logger.h"
#include "rdkafka.h"

#include <string.h>
#include <memory>

static const char* kThreadOptsId = "librdkafka_producer_thread_opts";
static const char* kPollThreadId = "librdkafka_producer_poll_thread";

struct enif_producer
{
    rd_kafka_t* kf;
    TopicManager* topics;
    ErlNifThreadOpts* thread_opts;
    ErlNifTid thread_id;
    ErlNifPid owner_pid;
    bool stop_feedback;
    bool running;
};

static void* producer_poll_thread(void* arg)
{
    enif_producer* producer = static_cast<enif_producer*>(arg);

    while (producer->running)
        rd_kafka_poll(producer->kf, 100);

    rd_kafka_flush(producer->kf, 30000);

    if(producer->stop_feedback)
    {
        ErlNifEnv* env = enif_alloc_env();
        enif_send(NULL, &producer->owner_pid, env, ATOMS.atomClientStopped);
        enif_free_env(env);
    }

    return NULL;
}

void enif_producer_free(ErlNifEnv* env, void* obj)
{
    UNUSED(env);

    enif_producer* producer = static_cast<enif_producer*>(obj);
    producer->running = false;

    if(producer->thread_opts)
    {
        void *result = NULL;
        enif_thread_join(producer->thread_id, &result);
        enif_thread_opts_destroy(producer->thread_opts);
    }

    if(producer->topics)
        delete producer->topics;

    if(producer->kf)
        rd_kafka_destroy(producer->kf);
}

static void delivery_report_callback (rd_kafka_t* rk, const rd_kafka_message_t* msg, void* data)
{
    UNUSED(rk);
    enif_producer* producer = static_cast<enif_producer*>(data);
    ErlNifEnv* env = enif_alloc_env();

    ERL_NIF_TERM status = msg->err == 0 ? ATOMS.atomOk : make_error(env, rd_kafka_err2str(msg->err));
    ERL_NIF_TERM key = msg->key == NULL ? ATOMS.atomUndefined : make_binary(env, reinterpret_cast<const char*>(msg->key), msg->key_len);
    const char* topic_name = rd_kafka_topic_name(msg->rkt);

    ERL_NIF_TERM term = enif_make_tuple6(env,
                                         ATOMS.atomMessage,
                                         make_binary(env, topic_name, strlen(topic_name)),
                                         enif_make_int(env, msg->partition),
                                         enif_make_int64(env, msg->offset),
                                         key,
                                         make_binary(env, reinterpret_cast<const char*>(msg->payload), msg->len));

    enif_send(NULL, &producer->owner_pid, env, enif_make_tuple3(env, ATOMS.atomDeliveryReport, status, term));
    enif_free_env(env);
}

static int stats_callback(rd_kafka_t *rk, char *json, size_t json_len, void *opaque)
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

ERL_NIF_TERM enif_producer_topic_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UNUSED(argc);

    std::string topic_name;
    enif_producer* producer;

    erlkaf_data* data = static_cast<erlkaf_data*>(enif_priv_data(env));

    if(!enif_get_resource(env, argv[0], data->res_producer, (void**) &producer))
        return make_badarg(env);

    if(!get_string(env, argv[1], &topic_name))
        return make_badarg(env);

    if(producer->topics->GetTopic(topic_name))
        return make_error(env, "topic already exist");

    scoped_ptr(config, rd_kafka_topic_conf_t, rd_kafka_topic_conf_new(), rd_kafka_topic_conf_destroy);

    ERL_NIF_TERM parse_result = parse_topic_config(env, argv[2], config.get());

    if(parse_result != ATOMS.atomOk)
        return parse_result;

    if(!producer->topics->AddTopic(topic_name, config.get()))
        return make_error(env, "failed to create topic");

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
    memset(producer.get(), 0, sizeof(enif_producer));

    if(!producer.get())
        return make_error(env, "failed to alloc producer");

    rd_kafka_conf_set_opaque(config.get(), producer.get());

    char errstr[512];
    scoped_ptr(rk, rd_kafka_t, rd_kafka_new(RD_KAFKA_PRODUCER, config.get(), errstr, sizeof(errstr)), rd_kafka_destroy);

    if (!rk)
        return make_error(env, errstr);

    config.release();

    producer->topics = new TopicManager(rk.get());
    producer->running = true;
    producer->stop_feedback = false;
    producer->kf = rk.release();
    producer->thread_opts = enif_thread_opts_create(const_cast<char*>(kThreadOptsId));

    if (enif_thread_create(const_cast<char*>(kPollThreadId), &producer->thread_id, producer_poll_thread, producer.get(), producer->thread_opts) != 0)
        return make_error(env, "failed to create producer thread");

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

    if(!enif_get_resource(env, argv[0], data->res_producer, (void**) &producer))
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

    if(!enif_get_resource(env, argv[0], data->res_producer, (void**) &producer))
        return make_badarg(env);

    producer->stop_feedback = true;
    producer->running = false;

    return ATOMS.atomOk;
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

    if(!enif_get_resource(env, argv[0], data->res_producer, (void**) &producer))
        return make_badarg(env);

    if(!get_string(env, argv[1], &topic_name))
        return make_badarg(env);

    rd_kafka_topic_t* topic = producer->topics->GetTopic(topic_name);

    if(topic == NULL)
        return make_error(env, "topic not found");

    if(!enif_get_int(env, argv[2], &partition))
        return make_badarg(env);

    if (!get_binary(env, argv[3], &key))
    {
        if(!enif_is_identical(ATOMS.atomUndefined, argv[3]))
            return make_badarg(env);

        memset(&key, 0, sizeof(ErlNifBinary));
    }

    if (!get_binary(env, argv[4], &value))
        return make_badarg(env);

    if (rd_kafka_produce(topic, partition, RD_KAFKA_MSG_F_COPY, value.data, value.size, key.data, key.size, NULL) != 0)
        return make_error(env, enif_make_int(env, rd_kafka_last_error()));

    return ATOMS.atomOk;
}
