#ifndef ERLKAF_C_SRC_ERLKAF_CONFIG_H_
#define ERLKAF_C_SRC_ERLKAF_CONFIG_H_

#include "erl_nif.h"
#include "rdkafka.h"

ERL_NIF_TERM parse_topic_config(ErlNifEnv* env, ERL_NIF_TERM list, rd_kafka_topic_conf_t* conf);
ERL_NIF_TERM parse_kafka_config(ErlNifEnv* env, ERL_NIF_TERM list, rd_kafka_conf_t* conf);

#endif

