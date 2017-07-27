#ifndef ERLKAF_C_SRC_ERLKAF_PRODUCER_H_
#define ERLKAF_C_SRC_ERLKAF_PRODUCER_H_

#include "erl_nif.h"

void enif_producer_free(ErlNifEnv* env, void* obj);

ERL_NIF_TERM enif_producer_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM enif_producer_topic_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM enif_producer_set_owner(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM enif_producer_cleanup(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM enif_produce(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

#endif
