#ifndef C_SRC_ERLKAF_OAUTHBEARER_H_
#define C_SRC_ERLKAF_OAUTHBEARER_H_

#include "erlkaf_nif.h"

#include <string>

typedef struct rd_kafka_s rd_kafka_t;

ERL_NIF_TERM oauthbearer_set_token(rd_kafka_t* kf, std::string token, long lifetime, std::string principal, std::string extensions_str);
ERL_NIF_TERM oauthbearer_set_token_failure(rd_kafka_t* kf, std::string error);

#endif  // C_SRC_ERLKAF_OAUTHBEARER_H_
