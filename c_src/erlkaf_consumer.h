#ifndef ERLKAF_C_SRC_ERLKAF_CONSUMER_H_
#define ERLKAF_C_SRC_ERLKAF_CONSUMER_H_

#include "erl_nif.h"

void enif_queue_free(ErlNifEnv* env, void* obj);
void enif_consumer_free(ErlNifEnv* env, void* obj);
ERL_NIF_TERM enif_consumer_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM enif_consumer_partition_revoke_completed(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM enif_consumer_queue_poll(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM enif_consumer_queue_cleanup(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM enif_consumer_offset_store(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

#endif
