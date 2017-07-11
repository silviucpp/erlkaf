#ifndef ERLKAF_C_SRC_ERLKAF_NIF_H_
#define ERLKAF_C_SRC_ERLKAF_NIF_H_

#include "erl_nif.h"

struct atoms
{
    ERL_NIF_TERM atomOk;
    ERL_NIF_TERM atomUndefined;
    ERL_NIF_TERM atomError;
    ERL_NIF_TERM atomTrue;
    ERL_NIF_TERM atomFalse;
    ERL_NIF_TERM atomBadArg;
    ERL_NIF_TERM atomOptions;

    ERL_NIF_TERM atomMessage;
    ERL_NIF_TERM atomDeliveryReport;
    ERL_NIF_TERM atomLogEvent;

};

struct erlkaf_data
{
    ErlNifResourceType* res_kafka_handler;
};

extern atoms ATOMS;

#endif
