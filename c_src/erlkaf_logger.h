#ifndef C_SRC_ERLKAF_LOGGER_H_
#define C_SRC_ERLKAF_LOGGER_H_

#include "erlkaf_nif.h"

#include <string>

typedef struct rd_kafka_s rd_kafka_t;

enum kRdLogLevel {
    kRdLogLevelEmergency = 0,
    kRdLogLevelAlert,
    kRdLogLevelCritical,
    kRdLogLevelError,
    kRdLogLevelWarning,
    kRdLogLevelNotice,
    kRdLogLevelInfo,
    kRdLogLevelDebug
};

void logger_callback(const rd_kafka_t *rk, int level, const char *fac, const char *buf);
ERL_NIF_TERM nif_set_logger_pid(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

void log_message(const rd_kafka_t *rk, kRdLogLevel level, const std::string& msg);

#endif  // C_SRC_ERLKAF_LOGGER_H_
