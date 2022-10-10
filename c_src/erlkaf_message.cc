#include "erlkaf_message.h"
#include "nif_utils.h"
#include "rdkafka.h"

#include <vector>
#include <string.h>

namespace {

inline ERL_NIF_TERM get_headers(ErlNifEnv* env, const rd_kafka_message_t* msg)
{
    rd_kafka_headers_t* headers = NULL;

    if(rd_kafka_message_headers(msg, &headers) == RD_KAFKA_RESP_ERR_NO_ERROR && headers)
    {
        std::vector<ERL_NIF_TERM> array;
        array.reserve(rd_kafka_header_cnt(headers));

        const char *name;
        const void *valuep;
        size_t size;
        size_t idx = 0;

        while (!rd_kafka_header_get_all(headers, idx++, &name, &valuep, &size))
            array.push_back(enif_make_tuple2(env, make_binary(env, name, strlen(name)), make_binary(env, reinterpret_cast<const char*>(valuep), size)));

        return enif_make_list_from_array(env, array.data(), array.size());
    }

    return ATOMS.atomUndefined;
}

inline ERL_NIF_TERM rd_kafka_timestamp_type_to_nif(rd_kafka_timestamp_type_t t)
{
    switch (t)
    {
        case RD_KAFKA_TIMESTAMP_NOT_AVAILABLE:
            return ATOMS.atomNotAvailable;

        case RD_KAFKA_TIMESTAMP_CREATE_TIME:
            return ATOMS.atomCreateTime;

        case RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME:
            return ATOMS.atomLogAppendTime;
    }
}

}

ERL_NIF_TERM make_message_nif(ErlNifEnv* env, ERL_NIF_TERM topic, ERL_NIF_TERM partition, const rd_kafka_message_t* msg)
{
    rd_kafka_timestamp_type_t ts_type;
    int64_t ts = rd_kafka_message_timestamp(msg, &ts_type);

    ERL_NIF_TERM key = msg->key == NULL ? ATOMS.atomUndefined : make_binary(env, reinterpret_cast<const char*>(msg->key), msg->key_len);
    ERL_NIF_TERM offset = enif_make_int64(env, msg->offset);
    ERL_NIF_TERM value = msg->payload == NULL ? ATOMS.atomUndefined : make_binary(env, reinterpret_cast<const char*>(msg->payload), msg->len);
    ERL_NIF_TERM headers = get_headers(env, msg);
    ERL_NIF_TERM ts_tuple = enif_make_tuple2(env, enif_make_int64(env, ts), rd_kafka_timestamp_type_to_nif(ts_type));
    return enif_make_tuple8(env, ATOMS.atomMessage, topic, partition, offset, key, value, headers, ts_tuple);
}

ERL_NIF_TERM make_message_nif(ErlNifEnv* env, const rd_kafka_message_t* msg)
{
    const char* topic_name = rd_kafka_topic_name(msg->rkt);
    return make_message_nif(env, make_binary(env, topic_name, strlen(topic_name)), enif_make_int(env, msg->partition), msg);
}
