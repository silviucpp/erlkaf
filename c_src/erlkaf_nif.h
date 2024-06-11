#ifndef C_SRC_ERLKAF_NIF_H_
#define C_SRC_ERLKAF_NIF_H_

#include "erl_nif.h"

class QueueCallbacksDispatcher;

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
    ERL_NIF_TERM atomBrokers;
    ERL_NIF_TERM atomTopics;
    ERL_NIF_TERM atomPartitions;
    ERL_NIF_TERM atomId;
    ERL_NIF_TERM atomHost;
    ERL_NIF_TERM atomPort;
    ERL_NIF_TERM atomName;
    ERL_NIF_TERM atomLeader;
    ERL_NIF_TERM atomReplicas;
    ERL_NIF_TERM atomIsrs;
    ERL_NIF_TERM atomDeliveryReport;
    ERL_NIF_TERM atomLogEvent;
    ERL_NIF_TERM atomAssignPartition;
    ERL_NIF_TERM atomRevokePartition;
    ERL_NIF_TERM atomStats;
    ERL_NIF_TERM atomClientStopped;
    ERL_NIF_TERM atomNotAvailable;
    ERL_NIF_TERM atomCreateTime;
    ERL_NIF_TERM atomLogAppendTime;
    ERL_NIF_TERM atomOauthbearerTokenRefresh;
};

struct erlkaf_data
{
    ErlNifResourceType* res_producer;
    ErlNifResourceType* res_consumer;
    ErlNifResourceType* res_queue;
    QueueCallbacksDispatcher* notifier_;
};

extern atoms ATOMS;

#endif  // C_SRC_ERLKAF_NIF_H_
