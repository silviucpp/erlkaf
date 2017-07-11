#ifndef ERLKAF_C_SRC_ERLKAF_LOGGER_H_
#define ERLKAF_C_SRC_ERLKAF_LOGGER_H_

#include "rdkafka.h"
#include "erlkaf_nif.h"

void logger_callback(const rd_kafka_t *rk, int level, const char *fac, const char *buf);
ERL_NIF_TERM nif_set_logger_pid(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

#endif
