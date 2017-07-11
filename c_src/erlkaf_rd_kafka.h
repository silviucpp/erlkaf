#ifndef ERLKAF_C_SRC_ERLKAF_RD_KAFKA_H_
#define ERLKAF_C_SRC_ERLKAF_RD_KAFKA_H_

#include "erlkaf_nif.h"
#include "rdkafka.h"

void enif_rd_kafka_free(ErlNifEnv* env, void* obj);

ERL_NIF_TERM enif_topic_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM enif_producer_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM enif_client_set_owner(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM enif_produce(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

#endif
