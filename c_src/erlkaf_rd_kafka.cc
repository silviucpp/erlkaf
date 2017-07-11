#include "erlkaf_rd_kafka.h"
#include "nif_utils.h"
#include "macros.h"
#include "topicmanager.h"
#include "erlkaf_nif.h"
#include "erlkaf_config.h"
#include "erlkaf_logger.h"

const char* kThreadOptsId = "librdkafka_thread_opts";
const char* kPollThreadId = "librdkafka_poll_thread";

struct enif_rd_kafka
{
    rd_kafka_t* kf;
    TopicManager* topics;
    ErlNifThreadOpts* thread_opts;
    ErlNifTid thread_id;
    ErlNifPid owner_pid;
    bool has_dr_callback;
    bool running;
};

struct callback_data
{
    ErlNifEnv* env;
    ERL_NIF_TERM tag;
    ErlNifPid pid;
};

void* producer_poll_thread(void* arg);

callback_data* callback_data_alloc(ERL_NIF_TERM ref, const ErlNifPid& pid)
{
    callback_data* callback = static_cast<callback_data*>(enif_alloc(sizeof(callback_data)));
    callback->env = enif_alloc_env();
    callback->tag = enif_make_copy(callback->env, ref);
    callback->pid = pid;
    return callback;
}

void callback_data_free(callback_data* cb)
{
    if(cb->env)
        enif_free_env(cb->env);

    enif_free(cb);
}

void enif_rd_kafka_free(ErlNifEnv* env, void* obj)
{
    UNUSED(env);

    enif_rd_kafka* enif_kafka = static_cast<enif_rd_kafka*>(obj);
    enif_kafka->running = false;

    if(enif_kafka->thread_opts)
    {
        void *result = NULL;
        enif_thread_join(enif_kafka->thread_id, &result);
        enif_thread_opts_destroy(enif_kafka->thread_opts);
    }

    if(enif_kafka->topics)
        delete enif_kafka->topics;

    if(enif_kafka->kf)
        rd_kafka_destroy(enif_kafka->kf);
}

enif_rd_kafka* enif_kafka_new(erlkaf_data* data, rd_kafka_t* kf, bool has_dr_callback)
{
    scoped_ptr(nif_kafka, enif_rd_kafka, static_cast<enif_rd_kafka*>(enif_alloc_resource(data->res_kafka_handler, sizeof(enif_rd_kafka))), enif_release_resource);

    if(nif_kafka.get() == NULL)
    {
        rd_kafka_destroy(kf);
        return NULL;
    }

    memset(nif_kafka.get(), 0, sizeof(enif_rd_kafka));

    nif_kafka->topics = new TopicManager(kf);
    nif_kafka->running = true;
    nif_kafka->kf = kf;
    nif_kafka->has_dr_callback = has_dr_callback;
    nif_kafka->thread_opts = enif_thread_opts_create(const_cast<char*>(kThreadOptsId));

    if (enif_thread_create(const_cast<char*>(kPollThreadId), &nif_kafka->thread_id, producer_poll_thread, nif_kafka.get(), nif_kafka->thread_opts) != 0)
        return NULL;

    return nif_kafka.release();
}

void delivery_report_callback (rd_kafka_t* rk, const rd_kafka_message_t* msg, void* data)
{
    UNUSED(rk);
    UNUSED(data);

    callback_data* cb = static_cast<callback_data*>(msg->_private);

    ERL_NIF_TERM status = msg->err == 0 ? ATOMS.atomOk : make_error(cb->env, rd_kafka_err2str(msg->err));
    ERL_NIF_TERM key = msg->key == NULL ? ATOMS.atomUndefined : make_binary(cb->env, reinterpret_cast<const char*>(msg->key), msg->key_len);
    const char* topic_name = rd_kafka_topic_name(msg->rkt);

    ERL_NIF_TERM term = enif_make_tuple6(cb->env,
                                        ATOMS.atomMessage,
                                        make_binary(cb->env, topic_name, strlen(topic_name)),
                                        enif_make_int(cb->env, msg->partition),
                                        enif_make_int64(cb->env, msg->offset),
                                        key,
                                        make_binary(cb->env, reinterpret_cast<const char*>(msg->payload), msg->len));

    enif_send(NULL, &cb->pid, cb->env, enif_make_tuple4(cb->env, ATOMS.atomDeliveryReport, cb->tag, status, term));
    callback_data_free(cb);
}

void* producer_poll_thread(void* arg)
{
    enif_rd_kafka* enif_kafka = static_cast<enif_rd_kafka*>(arg);

    while (enif_kafka->running)
        rd_kafka_poll(enif_kafka->kf, 100);

    rd_kafka_flush(enif_kafka->kf, 5000);

    return NULL;
}

ERL_NIF_TERM enif_topic_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UNUSED(argc);

    std::string topic_name;
    std::string topic_id;
    enif_rd_kafka* enif_kafka;

    erlkaf_data* data = static_cast<erlkaf_data*>(enif_priv_data(env));

    if(!enif_get_resource(env, argv[0], data->res_kafka_handler, (void**) &enif_kafka))
        return make_badarg(env);

    if(!get_string(env, argv[1], &topic_id) || !get_string(env, argv[2], &topic_name))
        return make_badarg(env);

    if(enif_kafka->topics->GetTopic(topic_id))
        return make_error(env, "topic already exist");

    scoped_ptr(config, rd_kafka_topic_conf_t, rd_kafka_topic_conf_new(), rd_kafka_topic_conf_destroy);

    ERL_NIF_TERM parse_result = parse_topic_config(env, argv[3], config.get());

    if(parse_result != ATOMS.atomOk)
        return parse_result;

    if(!enif_kafka->topics->AddTopic(topic_id, topic_name, config.get()))
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

    if(has_dr_callback)
        rd_kafka_conf_set_dr_msg_cb(config.get(), delivery_report_callback);

    char errstr[512];
    rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_PRODUCER, config.get(), errstr, sizeof(errstr));

    if (!rk)
        return make_error(env, errstr);

    config.release();

    erlkaf_data* data = static_cast<erlkaf_data*>(enif_priv_data(env));

    enif_rd_kafka* nif_kafka = enif_kafka_new(data, rk, has_dr_callback);

    if(nif_kafka == NULL)
        return make_error(env, "failed to create native kafka object");

    ERL_NIF_TERM term = enif_make_resource(env, nif_kafka);
    enif_release_resource(nif_kafka);
    return enif_make_tuple2(env, ATOMS.atomOk, term);
}

ERL_NIF_TERM enif_client_set_owner(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UNUSED(argc);
    erlkaf_data* data = static_cast<erlkaf_data*>(enif_priv_data(env));

    enif_rd_kafka* enif_kafka;

    if(!enif_get_resource(env, argv[0], data->res_kafka_handler, (void**) &enif_kafka))
        return make_badarg(env);

    if(!enif_get_local_pid(env, argv[1], &enif_kafka->owner_pid))
        return make_badarg(env);

    return ATOMS.atomOk;
}

ERL_NIF_TERM enif_produce(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UNUSED(argc);

    erlkaf_data* data = static_cast<erlkaf_data*>(enif_priv_data(env));

    enif_rd_kafka* enif_kafka;
    std::string topic_id;
    int32_t partition;
    ErlNifBinary key;
    ErlNifBinary value;

    if(!enif_get_resource(env, argv[0], data->res_kafka_handler, (void**) &enif_kafka))
        return make_badarg(env);

    if(!get_string(env, argv[1], &topic_id))
        return make_badarg(env);

    rd_kafka_topic_t* topic = enif_kafka->topics->GetTopic(topic_id);

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

    ERL_NIF_TERM tag = enif_make_ref(env);

    callback_data* callback = NULL;

    if(enif_kafka->owner_pid.pid && enif_kafka->has_dr_callback)
        callback = callback_data_alloc(tag, enif_kafka->owner_pid);

    if (rd_kafka_produce(topic, partition, RD_KAFKA_MSG_F_COPY, value.data, value.size, key.data, key.size, callback) != 0)
    {
        if(callback)
            callback_data_free(callback);

        return make_error(env, enif_make_int(env, rd_kafka_last_error()));
    }

    return enif_make_tuple2(env, ATOMS.atomOk, tag);
}
