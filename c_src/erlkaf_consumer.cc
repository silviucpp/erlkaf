#include "erlkaf_consumer.h"
#include "nif_utils.h"
#include "macros.h"
#include "erlkaf_nif.h"
#include "erlkaf_config.h"
#include "rdkafka.h"
#include "erlkaf_logger.h"

static const char* kThreadOptsId = "librdkafka_consumer_thread_opts";
static const char* kPollThreadId = "librdkafka_consumer_poll_thread";

#include <vector>
#include <memory>
#include <string.h>
#include <unistd.h>

struct enif_consumer
{
    rd_kafka_t* kf;
    ErlNifPid owner;
    ErlNifThreadOpts* thread_opts;
    ErlNifTid thread_id;
    ErlNifResourceType* res_queue;
    bool running;
    bool stop_feedback;
};

struct enif_queue
{
    rd_kafka_queue_t* queue;
    enif_consumer* consumer;
};

void enif_queue_free(ErlNifEnv* env, void* obj)
{
    UNUSED(env);

    enif_queue* q = static_cast<enif_queue*>(obj);

    if(q->queue)
        rd_kafka_queue_forward(q->queue, rd_kafka_queue_get_consumer(q->consumer->kf));

    enif_release_resource(q->consumer);
}

void enif_consumer_free(ErlNifEnv* env, void* obj)
{
    UNUSED(env);

    enif_consumer* consumer = static_cast<enif_consumer*>(obj);
    consumer->running = false;

    if(consumer->thread_opts)
    {
        void *result = NULL;
        enif_thread_join(consumer->thread_id, &result);
        enif_thread_opts_destroy(consumer->thread_opts);
    }
}

enif_queue* enif_new_queue(enif_consumer* consumer, rd_kafka_t* rk, const std::string& topic, int32_t partition)
{
    rd_kafka_queue_t* partition_queue = rd_kafka_queue_get_partition(rk, topic.c_str(), partition);
    ASSERT(partition_queue);

    enif_keep_resource(consumer);
    rd_kafka_queue_forward(partition_queue, NULL);

    enif_queue* q = static_cast<enif_queue*>(enif_alloc_resource(consumer->res_queue, sizeof(enif_queue)));
    q->queue = partition_queue;
    q->consumer = consumer;
    return q;
}

ERL_NIF_TERM partition_list_to_nif(ErlNifEnv* env, enif_consumer* consumer, rd_kafka_t *rk, rd_kafka_topic_partition_list_t* partitions, bool assign)
{
    if(!partitions)
        return enif_make_list(env, 0);

    ERL_NIF_TERM items[partitions->cnt];

    for (int i = 0 ; i < partitions->cnt ; i++)
    {
        rd_kafka_topic_partition_t obj = partitions->elems[i];
        std::string topic = obj.topic;

        if(assign)
        {
            enif_queue* queue = enif_new_queue(consumer, rk, topic, obj.partition);

            ERL_NIF_TERM queue_term = enif_make_resource(env, queue);
            ERL_NIF_TERM topic_name_term = make_binary(env, topic.c_str(), topic.length());
            ERL_NIF_TERM partition_term = enif_make_int(env, obj.partition);
            ERL_NIF_TERM offset_term = enif_make_int64(env, obj.offset);

            enif_release_resource(queue);
            items[i] = enif_make_tuple4(env, topic_name_term, partition_term, offset_term, queue_term);
        }
        else
        {
            items[i] = enif_make_tuple2(env, make_binary(env, topic.c_str(), topic.length()), enif_make_int(env, obj.partition));
        }
    }

    return enif_make_list_from_array(env, items, partitions->cnt);
}

static void* consumer_poll_thread(void* arg)
{
    enif_consumer* consumer = static_cast<enif_consumer*>(arg);

    while (consumer->running)
    {
        rd_kafka_message_t* msg = rd_kafka_consumer_poll(consumer->kf, 100);

        if(msg)
        {
            //because communication between nif and erlang it's based on async messages might be a small window
            //between starting of revoking partitions (queued are forwarded back on the main queue) and when actual we revoked them
            //when we get the messages here. we drop all this messages as time they have no impact because offset is not changed.
            //we are sleeping here as well to not consume lot of cpu
            rd_kafka_message_destroy(msg);
            usleep(50000);
        }
    }

    rd_kafka_consumer_close(consumer->kf);
    rd_kafka_destroy(consumer->kf);

    if(consumer->stop_feedback)
    {
        ErlNifEnv* env = enif_alloc_env();
        enif_send(NULL, &consumer->owner, env, ATOMS.atomClientStopped);
        enif_free_env(env);
    }

    return NULL;
}

void assign_partitions(ErlNifEnv* env, enif_consumer* consumer, rd_kafka_t *rk, rd_kafka_topic_partition_list_t *partitions)
{
    rd_kafka_resp_err_t response = rd_kafka_assign(rk, partitions);

    if(response != RD_KAFKA_RESP_ERR_NO_ERROR)
        log_message(rk, kRdLogLevelError, "failed to assign the new partitions"+std::string(rd_kafka_err2str(response)));

    ERL_NIF_TERM list = partition_list_to_nif(env, consumer, rk, partitions, true);
    enif_send(NULL, &consumer->owner, env, enif_make_tuple2(env, ATOMS.atomAssignPartition, list));
}

void revoke_partitions(ErlNifEnv* env, enif_consumer* consumer, rd_kafka_t *rk, rd_kafka_topic_partition_list_t *partitions)
{
    if(!consumer->running)
    {
        rd_kafka_assign(rk, NULL);
        return;
    }

    ERL_NIF_TERM list = partition_list_to_nif(env, consumer, rk, partitions, false);
    enif_send(NULL, &consumer->owner, env, enif_make_tuple2(env, ATOMS.atomRevokePartition, list));
}

static void rebalance_cb (rd_kafka_t *rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t *partitions, void *opaque)
{
    enif_consumer* consumer = static_cast<enif_consumer*>(opaque);
    ErlNifEnv* env = enif_alloc_env();

    switch (err)
    {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
            assign_partitions(env, consumer, rk, partitions);
            break;

        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
            revoke_partitions(env, consumer, rk, partitions);
            break;

        default:
            log_message(rk, kRdLogLevelError, "rebalance error: "+std::string(rd_kafka_err2str(err)));
            revoke_partitions(env, consumer, rk, partitions);
    }

    enif_free_env(env);
}

static int stats_callback(rd_kafka_t *rk, char *json, size_t json_len, void *opaque)
{
    UNUSED(rk);

    enif_consumer* consumer = static_cast<enif_consumer*>(opaque);
    ErlNifEnv* env = enif_alloc_env();
    ERL_NIF_TERM stats = make_binary(env, json, json_len);
    enif_send(NULL, &consumer->owner, env, enif_make_tuple2(env, ATOMS.atomStats, stats));
    enif_free_env(env);
    return 0;
}

rd_kafka_topic_partition_list_t* topic_subscribe(ErlNifEnv* env, ERL_NIF_TERM list)
{
    uint32_t length;

    if(!enif_get_list_length(env, list, &length) || length < 1)
        return NULL;

    scoped_ptr(topics, rd_kafka_topic_partition_list_t, rd_kafka_topic_partition_list_new(length), rd_kafka_topic_partition_list_destroy);

    ERL_NIF_TERM head;

    while(enif_get_list_cell(env, list, &head, &list))
    {
        std::string topic_name;

        if(!get_string(env, head, &topic_name))
            return NULL;

        rd_kafka_topic_partition_list_add(topics.get(), topic_name.c_str(), RD_KAFKA_PARTITION_UA);
    }

    return topics.release();
}

ERL_NIF_TERM enif_consumer_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UNUSED(argc);

    char errstr[512];
    std::string group_id;
    ErlNifPid owner;

    if(!enif_self(env, &owner))
        return make_badarg(env);

    if(!get_string(env, argv[0], &group_id))
        return make_badarg(env);

    scoped_ptr(client_conf, rd_kafka_conf_t, rd_kafka_conf_new(), rd_kafka_conf_destroy);
    scoped_ptr(topic_conf, rd_kafka_topic_conf_t, rd_kafka_topic_conf_new(), rd_kafka_topic_conf_destroy);

    ERL_NIF_TERM parse_result = parse_kafka_config(env, argv[2], client_conf.get());

    if(parse_result != ATOMS.atomOk)
        return parse_result;

    parse_result = parse_topic_config(env, argv[3], topic_conf.get());

    if(parse_result != ATOMS.atomOk)
        return parse_result;

    if (rd_kafka_conf_set(client_conf.get(), "group.id", group_id.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        return make_error(env, errstr);

    erlkaf_data* data = static_cast<erlkaf_data*>(enif_priv_data(env));

    scoped_ptr(consumer, enif_consumer, static_cast<enif_consumer*>(enif_alloc_resource(data->res_consumer, sizeof(enif_consumer))), enif_release_resource);

    if(consumer.get() == NULL)
        return make_error(env, "failed to alloc consumer");

    memset(consumer.get(), 0, sizeof(enif_consumer));

    rd_kafka_conf_set_opaque(client_conf.get(), consumer.get());
    rd_kafka_conf_set_default_topic_conf(client_conf.get(), topic_conf.release());
    rd_kafka_conf_set_log_cb(client_conf.get(), logger_callback);
    rd_kafka_conf_set_rebalance_cb(client_conf.get(), rebalance_cb);
    rd_kafka_conf_set_stats_cb(client_conf.get(), stats_callback);

    scoped_ptr(rk, rd_kafka_t, rd_kafka_new(RD_KAFKA_CONSUMER, client_conf.get(), errstr, sizeof(errstr)), rd_kafka_destroy);

    if (!rk.get())
        return make_error(env, errstr);

    client_conf.release();

    rd_kafka_poll_set_consumer(rk.get());

    scoped_ptr(topics, rd_kafka_topic_partition_list_t, topic_subscribe(env, argv[1]), rd_kafka_topic_partition_list_destroy);

    if(topics.get() == NULL)
        return make_error(env, "invalid topic list");

    rd_kafka_resp_err_t err = rd_kafka_subscribe(rk.get(), topics.get());

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
        return make_error(env, rd_kafka_err2str(err));

    consumer->running = true;
    consumer->stop_feedback = false;
    consumer->owner = owner;
    consumer->kf = rk.release();
    consumer->thread_opts = enif_thread_opts_create(const_cast<char*>(kThreadOptsId));
    consumer->res_queue = data->res_queue;

    enif_keep_resource(data->res_queue);

    if (enif_thread_create(const_cast<char*>(kPollThreadId), &consumer->thread_id, consumer_poll_thread, consumer.get(), consumer->thread_opts) != 0)
        return make_error(env, "failed to create consumer thread");

    ERL_NIF_TERM term = enif_make_resource(env, consumer.get());
    return enif_make_tuple2(env, ATOMS.atomOk, term);
}

ERL_NIF_TERM enif_consumer_queue_cleanup(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UNUSED(argc);

    erlkaf_data* data = static_cast<erlkaf_data*>(enif_priv_data(env));

    enif_queue* q;

    if(!enif_get_resource(env, argv[0], data->res_queue, (void**) &q))
        return make_badarg(env);

    if(q->queue)
    {
        rd_kafka_queue_forward(q->queue, rd_kafka_queue_get_consumer(q->consumer->kf));
        q->queue = NULL;
    }

    return ATOMS.atomOk;
}

ERL_NIF_TERM enif_consumer_partition_revoke_completed(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UNUSED(argc);

    erlkaf_data* data = static_cast<erlkaf_data*>(enif_priv_data(env));
    enif_consumer* c;

    if(!enif_get_resource(env, argv[0], data->res_consumer, (void**) &c))
        return make_badarg(env);

    rd_kafka_assign(c->kf, NULL);
    return ATOMS.atomOk;
}

ERL_NIF_TERM enif_consumer_queue_poll(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UNUSED(argc);

    erlkaf_data* data = static_cast<erlkaf_data*>(enif_priv_data(env));
    enif_queue* q;

    if(!enif_get_resource(env, argv[0], data->res_queue, (void**) &q))
        return make_badarg(env);

    uint32_t max_batch_size;

    if(!enif_get_uint(env, argv[1], &max_batch_size))
        return make_badarg(env);

    std::vector<ERL_NIF_TERM> messages;
    messages.reserve(max_batch_size);

    ERL_NIF_TERM topic = 0;
    ERL_NIF_TERM partition = 0;
    bool first = true;
    int64_t last_offset = -1;

    while(messages.size() < max_batch_size)
    {
        rd_kafka_event_t* event = rd_kafka_queue_poll(q->queue, 0);

        if(!event)
            break;

        ASSERT(rd_kafka_event_type(event) == RD_KAFKA_EVENT_FETCH);

        size_t msg_count = rd_kafka_event_message_count(event);

        for (size_t i = 0; i < msg_count; i++)
        {
            const rd_kafka_message_t* msg = rd_kafka_event_message_next(event);

            if(first)
            {
                const char* topic_name = rd_kafka_topic_name(msg->rkt);
                topic = make_binary(env, topic_name, strlen(topic_name));
                partition = enif_make_int(env, msg->partition);
                first = false;
            }

            ERL_NIF_TERM key = msg->key == NULL ? ATOMS.atomUndefined : make_binary(env, reinterpret_cast<const char*>(msg->key), msg->key_len);
            ERL_NIF_TERM offset = enif_make_int64(env, msg->offset);
            ERL_NIF_TERM value = make_binary(env, reinterpret_cast<const char*>(msg->payload), msg->len);
            ERL_NIF_TERM msg_term = enif_make_tuple6(env, ATOMS.atomMessage, topic, partition, offset, key, value);

            last_offset = msg->offset;
            messages.push_back(msg_term);
        }

        rd_kafka_event_destroy(event);
    }

    ERL_NIF_TERM list = enif_make_list_from_array(env, &messages[0], messages.size());
    return enif_make_tuple(env, 3, ATOMS.atomOk, list, enif_make_int64(env, last_offset));
}

ERL_NIF_TERM enif_consumer_offset_store(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UNUSED(argc);

    std::string topic_name;
    int32_t partition;
    long offset;

    erlkaf_data* data = static_cast<erlkaf_data*>(enif_priv_data(env));
    enif_consumer* c;

    if(!enif_get_resource(env, argv[0], data->res_consumer, (void**) &c))
        return make_badarg(env);

    if(!get_string(env, argv[1], &topic_name))
        return make_badarg(env);

    if(!enif_get_int(env, argv[2], &partition))
        return make_badarg(env);

    if(!enif_get_int64(env, argv[3], &offset))
        return make_badarg(env);

    scoped_ptr(topic, rd_kafka_topic_t, rd_kafka_topic_new(c->kf, topic_name.c_str(), NULL), rd_kafka_topic_destroy);

    rd_kafka_resp_err_t error = rd_kafka_offset_store(topic.get(), partition, offset);

    if(error == RD_KAFKA_RESP_ERR_NO_ERROR )
        return ATOMS.atomOk;

    return make_error(env, error);
}

ERL_NIF_TERM enif_consumer_cleanup(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UNUSED(argc);

    erlkaf_data* data = static_cast<erlkaf_data*>(enif_priv_data(env));
    enif_consumer* consumer;

    if(!enif_get_resource(env, argv[0], data->res_consumer, (void**) &consumer))
        return make_badarg(env);

    consumer->stop_feedback = true;
    consumer->running = false;
    return ATOMS.atomOk;
}

