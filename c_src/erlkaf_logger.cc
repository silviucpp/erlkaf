#include "erlkaf_logger.h"
#include "rdkafka.h"
#include "nif_utils.h"
#include "macros.h"

#include <string.h>

static ErlNifPid log_pid = {0};

void logger_callback(const rd_kafka_t *rk, int level, const char *fac, const char *buf)
{
    if(log_pid.pid == 0)
        return;

    ErlNifEnv* env = enif_alloc_env();

    const char* name = rd_kafka_name(rk);

    ERL_NIF_TERM log_event = enif_make_tuple5(env,
                                              ATOMS.atomLogEvent,
                                              enif_make_int(env, level),
                                              make_binary(env, name, strlen(name)),
                                              make_binary(env, fac, strlen(fac)),
                                              make_binary(env, buf, strlen(buf)));

    enif_send(NULL, &log_pid, env, log_event);
    enif_free_env(env);
}

ERL_NIF_TERM nif_set_logger_pid(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UNUSED(argc);

    if(enif_is_identical(argv[0], ATOMS.atomUndefined))
    {
        log_pid = {0};
        return ATOMS.atomOk;
    }

    if(!enif_get_local_pid(env, argv[0], &log_pid))
        return make_badarg(env);

    return ATOMS.atomOk;
}

void log_message(const rd_kafka_t *rk, kRdLogLevel level, const std::string& msg)
{
    logger_callback(rk, static_cast<int>(level), "", msg.c_str());
}
