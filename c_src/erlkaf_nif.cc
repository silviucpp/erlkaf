#include "erlkaf_nif.h"
#include "nif_utils.h"
#include "macros.h"
#include "erlkaf_logger.h"
#include "erlkaf_producer.h"
#include "erlkaf_consumer.h"

const char kAtomOk[] = "ok";
const char kAtomUndefined[] = "undefined";
const char kAtomError[] = "error";
const char kAtomTrue[] = "true";
const char kAtomFalse[] = "false";
const char kAtomBadArg[] = "badarg";
const char kAtomOptions[] = "options";
const char kAtomMessage[] = "erlkaf_msg";
const char kAtomDeliveryReport[] = "delivery_report";
const char kAtomLogEvent[] = "log_message";
const char kAtomAssignPartition[] = "assign_partitions";
const char kAtomRevokePartition[] = "revoke_partitions";

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
    ATOMS.atomMessage = make_atom(env, kAtomMessage);
    ATOMS.atomDeliveryReport = make_atom(env, kAtomDeliveryReport);
    ATOMS.atomLogEvent = make_atom(env, kAtomLogEvent);
    ATOMS.atomAssignPartition = make_atom(env, kAtomAssignPartition);
    ATOMS.atomRevokePartition = make_atom(env, kAtomRevokePartition);

    erlkaf_data* data = static_cast<erlkaf_data*>(enif_alloc(sizeof(erlkaf_data)));
    open_resources(env, data);

    *priv_data = data;
    return 0;
}

void on_nif_unload(ErlNifEnv* env, void* priv_data)
{
    UNUSED(env);

    erlkaf_data* data = static_cast<erlkaf_data*>(priv_data);
    enif_free(data);
}

int on_nif_upgrade(ErlNifEnv* env, void** priv, void** old_priv, ERL_NIF_TERM info)
{
    UNUSED(old_priv);
    UNUSED(info);

    erlkaf_data* data = static_cast<erlkaf_data*>(enif_alloc(sizeof(erlkaf_data)));
    open_resources(env, data);

    *priv = data;
    return 0;
}

static ErlNifFunc nif_funcs[] =
{
    {"set_log_process", 1, nif_set_logger_pid},

    {"producer_new", 2, enif_producer_new},
    {"producer_set_owner", 2, enif_producer_set_owner},
    {"producer_topic_new", 3, enif_producer_topic_new},
    {"produce", 5, enif_produce},

    {"consumer_new", 4, enif_consumer_new},
    {"consumer_partition_revoke_completed", 1, enif_consumer_partition_revoke_completed},
    {"consumer_queue_poll", 1, enif_consumer_queue_poll},
    {"consumer_offset_store", 4, enif_consumer_offset_store}
};

ERL_NIF_INIT(erlkaf_nif, nif_funcs, on_nif_load, NULL, on_nif_upgrade, on_nif_unload)
