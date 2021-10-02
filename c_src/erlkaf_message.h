#ifndef C_SRC_ERLKAF_MESSAGE_H_
#define C_SRC_ERLKAF_MESSAGE_H_

#include "erlkaf_nif.h"

typedef struct rd_kafka_message_s rd_kafka_message_t;

ERL_NIF_TERM make_message_nif(ErlNifEnv* env, ERL_NIF_TERM topic, ERL_NIF_TERM partition, const rd_kafka_message_t* msg);
ERL_NIF_TERM make_message_nif(ErlNifEnv* env, const rd_kafka_message_t* msg);

#endif  // C_SRC_ERLKAF_MESSAGE_H_
