#include "erlkaf_nif.h"
#include "nif_utils.h"
#include "macros.h"
#include "erlkaf_logger.h"
#include "erlkaf_rd_kafka.h"

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

atoms ATOMS;

void open_resources(ErlNifEnv* env, erlkaf_data* data)
{
    ErlNifResourceFlags flags =  static_cast<ErlNifResourceFlags>(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);
    data->res_kafka_handler = enif_open_resource_type(env, NULL, "res_rd_kafka", enif_rd_kafka_free, flags, NULL);
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
    {"topic_new", 4, enif_topic_new},
    {"producer_new", 2, enif_producer_new},
    {"client_set_owner", 2, enif_client_set_owner},
    {"produce", 5, enif_produce}
};

ERL_NIF_INIT(erlkaf_nif, nif_funcs, on_nif_load, NULL, on_nif_upgrade, on_nif_unload)
