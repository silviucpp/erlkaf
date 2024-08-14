#include "erlkaf_nif.h"
#include "nif_utils.h"
#include "macros.h"
#include "erlkaf_logger.h"
#include "erlkaf_producer.h"
#include "erlkaf_consumer.h"
#include "queuecallbacksdispatcher.h"

const char kAtomOk[] = "ok";
const char kAtomUndefined[] = "undefined";
const char kAtomError[] = "error";
const char kAtomTrue[] = "true";
const char kAtomFalse[] = "false";
const char kAtomBadArg[] = "badarg";
const char kAtomOptions[] = "options";
const char kAtomBrokers[] = "brokers";
const char kAtomTopics[] = "topics";
const char kAtomPartitions[] = "partitions";
const char kAtomId[] = "id";
const char kAtomHost[] = "host";
const char kAtomPort[] = "port";
const char kAtomName[] = "name";
const char kAtomLeader[] = "leader";
const char kAtomReplicas[] = "replicas";
const char kAtomIsrs[] = "isrs";
const char kAtomMessage[] = "erlkaf_msg";
const char kAtomDeliveryReport[] = "delivery_report";
const char kAtomLogEvent[] = "log_message";
const char kAtomAssignPartition[] = "assign_partitions";
const char kAtomRevokePartition[] = "revoke_partitions";
const char kAtomStats[] = "stats";
const char kAtomClientStopped[] = "client_stopped";
const char kAtomNotAvailable[] = "not_available";
const char kAtomCreateTime[] = "create_time";
const char kAtomLogAppendTime[] = "log_append_time";
const char kAtomOauthbearerTokenRefresh[] = "oauthbearer_token_refresh";

atoms ATOMS;

void open_resources(ErlNifEnv* env, erlkaf_data* data)
{
    ErlNifResourceFlags flags =  static_cast<ErlNifResourceFlags>(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);
    data->res_producer = enif_open_resource_type(env, NULL, "res_producer", enif_producer_free, flags, NULL);
    data->res_consumer = enif_open_resource_type(env, NULL, "res_consumer", enif_consumer_free, flags, NULL);
    data->res_queue = enif_open_resource_type(env, NULL, "res_queue", enif_queue_free, flags, NULL);
}

int on_nif_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    UNUSED(load_info);

    ATOMS.atomOk = make_atom(env, kAtomOk);
    ATOMS.atomUndefined = make_atom(env, kAtomUndefined);
    ATOMS.atomError = make_atom(env, kAtomError);
    ATOMS.atomTrue = make_atom(env, kAtomTrue);
    ATOMS.atomFalse = make_atom(env, kAtomFalse);
    ATOMS.atomOptions = make_atom(env, kAtomOptions);
    ATOMS.atomBadArg = make_atom(env, kAtomBadArg);
    ATOMS.atomBrokers = make_atom(env, kAtomBrokers);
    ATOMS.atomTopics = make_atom(env, kAtomTopics);
    ATOMS.atomPartitions = make_atom(env, kAtomPartitions);
    ATOMS.atomId = make_atom(env, kAtomId);
    ATOMS.atomHost = make_atom(env, kAtomHost);
    ATOMS.atomPort = make_atom(env, kAtomPort);
    ATOMS.atomName = make_atom(env, kAtomName);
    ATOMS.atomLeader = make_atom(env, kAtomLeader);
    ATOMS.atomReplicas = make_atom(env, kAtomReplicas);
    ATOMS.atomIsrs = make_atom(env, kAtomIsrs);
    ATOMS.atomMessage = make_atom(env, kAtomMessage);
    ATOMS.atomDeliveryReport = make_atom(env, kAtomDeliveryReport);
    ATOMS.atomLogEvent = make_atom(env, kAtomLogEvent);
    ATOMS.atomAssignPartition = make_atom(env, kAtomAssignPartition);
    ATOMS.atomRevokePartition = make_atom(env, kAtomRevokePartition);
    ATOMS.atomStats = make_atom(env, kAtomStats);
    ATOMS.atomClientStopped = make_atom(env, kAtomClientStopped);
    ATOMS.atomNotAvailable = make_atom(env, kAtomNotAvailable);
    ATOMS.atomCreateTime = make_atom(env, kAtomCreateTime);
    ATOMS.atomLogAppendTime = make_atom(env, kAtomLogAppendTime);
    ATOMS.atomOauthbearerTokenRefresh = make_atom(env, kAtomOauthbearerTokenRefresh);

    erlkaf_data* data = static_cast<erlkaf_data*>(enif_alloc(sizeof(erlkaf_data)));
    open_resources(env, data);

    data->notifier_ = new QueueCallbacksDispatcher();

    *priv_data = data;
    return 0;
}

void on_nif_unload(ErlNifEnv* env, void* priv_data)
{
    UNUSED(env);

    erlkaf_data* data = static_cast<erlkaf_data*>(priv_data);

    if(data->notifier_)
        delete data->notifier_;

    enif_free(data);
}

int on_nif_upgrade(ErlNifEnv* env, void** priv, void** old_priv, ERL_NIF_TERM info)
{
    UNUSED(old_priv);
    UNUSED(info);

    erlkaf_data* old_data = static_cast<erlkaf_data*>(*old_priv);
    erlkaf_data* data = static_cast<erlkaf_data*>(enif_alloc(sizeof(erlkaf_data)));
    open_resources(env, data);
    data->notifier_ = old_data->notifier_;

    old_data->notifier_ = nullptr;
    *priv = data;
    return 0;
}

static ErlNifFunc nif_funcs[] =
{
    {"set_log_process", 1, nif_set_logger_pid},

    {"producer_new", 2, enif_producer_new},
    {"producer_set_owner", 2, enif_producer_set_owner},
    {"producer_topic_new", 3, enif_producer_topic_new},
    {"producer_topic_delete", 3, enif_producer_topic_delete},
    {"producer_cleanup", 1, enif_producer_cleanup},
    {"produce", 7, enif_produce},
    {"get_metadata", 1, enif_get_metadata, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"producer_oauthbearer_set_token", 5, enif_producer_oauthbearer_set_token},
    {"producer_oauthbearer_set_token_failure", 2, enif_producer_oauthbearer_set_token_failure},

    {"consumer_new", 4, enif_consumer_new},
    {"consumer_partition_revoke_completed", 1, enif_consumer_partition_revoke_completed},
    {"consumer_queue_poll", 2, enif_consumer_queue_poll},
    {"consumer_queue_cleanup", 1, enif_consumer_queue_cleanup},
    {"consumer_offset_store", 4, enif_consumer_offset_store},
    {"consumer_cleanup", 1, enif_consumer_cleanup},
    {"consumer_oauthbearer_set_token", 5, enif_consumer_oauthbearer_set_token},
    {"consumer_oauthbearer_set_token_failure", 2, enif_consumer_oauthbearer_set_token_failure}
};

ERL_NIF_INIT(erlkaf_nif, nif_funcs, on_nif_load, NULL, on_nif_upgrade, on_nif_unload)
