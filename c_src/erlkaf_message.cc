#include "erlkaf_message.h"
#include "nif_utils.h"
#include "rdkafka.h"

#include <vector>
#include <string.h>

namespace {

ERL_NIF_TERM get_headers(ErlNifEnv* env, const rd_kafka_message_t* msg)
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

}

ERL_NIF_TERM make_message_nif(ErlNifEnv* env, ERL_NIF_TERM topic, ERL_NIF_TERM partition, const rd_kafka_message_t* msg)
{
    ERL_NIF_TERM key = msg->key == NULL ? ATOMS.atomUndefined : make_binary(env, reinterpret_cast<const char*>(msg->key), msg->key_len);
    ERL_NIF_TERM offset = enif_make_int64(env, msg->offset);
    ERL_NIF_TERM value = make_binary(env, reinterpret_cast<const char*>(msg->payload), msg->len);
    ERL_NIF_TERM headers = get_headers(env, msg);
    return enif_make_tuple7(env, ATOMS.atomMessage, topic, partition, offset, key, value, headers);
}

ERL_NIF_TERM make_message_nif(ErlNifEnv* env, const rd_kafka_message_t* msg)
{
    const char* topic_name = rd_kafka_topic_name(msg->rkt);
    return make_message_nif(env, make_binary(env, topic_name, strlen(topic_name)), enif_make_int(env, msg->partition), msg);
}
