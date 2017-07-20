#include "erlkaf_config.h"
#include "nif_utils.h"
#include "erlkaf_nif.h"

#include <functional>
#include <signal.h>

template <typename T> struct set_config_fun
{
    std::function<rd_kafka_conf_res_t (T *conf, const char *name, const char *value, char *errstr, size_t errstr_size)> set_value;
};

const set_config_fun<rd_kafka_topic_conf_t> kTopicConfFuns = {
    rd_kafka_topic_conf_set
};

const set_config_fun<rd_kafka_conf_t> kKafkaConfFuns = {
    rd_kafka_conf_set
};

template <typename T> ERL_NIF_TERM parse_config(ErlNifEnv* env, ERL_NIF_TERM list, T* conf, set_config_fun<T> fun)
{
    if(!enif_is_list(env, list))
        return make_bad_options(env, list);

    char errstr[512];
    ERL_NIF_TERM head;
    const ERL_NIF_TERM *items;
    int arity;

    while(enif_get_list_cell(env, list, &head, &list))
    {
        if(!enif_get_tuple(env, head, &arity, &items) || arity != 2)
            return make_bad_options(env, head);

        std::string key;
        std::string value;

        if(!get_string(env, items[0], &key))
            return make_bad_options(env, head);

        if(!get_string(env, items[1], &value))
            return make_bad_options(env, head);

        if(fun.set_value(conf, key.c_str(), value.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
            return make_error(env, errstr);
    }

    return ATOMS.atomOk;
}

bool appy_kafka_default_config(rd_kafka_conf_t* config)
{
    if(rd_kafka_conf_set(config, "enable.auto.commit", "true", NULL, 0) != RD_KAFKA_CONF_OK)
        return false;

    if(rd_kafka_conf_set(config, "enable.auto.offset.store", "false", NULL, 0) != RD_KAFKA_CONF_OK)
        return false;

    if(rd_kafka_conf_set(config, "enable.partition.eof", "false", NULL, 0) != RD_KAFKA_CONF_OK)
        return false;

#ifdef SIGIO
    //quick termination
    char tmp[128];
    snprintf(tmp, sizeof(tmp), "%i", SIGIO);

    if(rd_kafka_conf_set(config, "internal.termination.signal", tmp, NULL, 0) != RD_KAFKA_CONF_OK)
        return false;
#endif

    return true;
}

bool appy_topic_default_config(rd_kafka_topic_conf_t* config)
{
    if(rd_kafka_topic_conf_set(config, "produce.offset.report", "true", NULL, 0) != RD_KAFKA_CONF_OK)
        return false;

    if(rd_kafka_topic_conf_set(config, "auto.commit.enable", "false", NULL, 0) != RD_KAFKA_CONF_OK)
        return false;

    return true;
}

ERL_NIF_TERM parse_topic_config(ErlNifEnv* env, ERL_NIF_TERM list, rd_kafka_topic_conf_t* conf)
{
    if(!appy_topic_default_config(conf))
        return make_error(env, "failed to apply default topic config");

    return parse_config(env, list, conf, kTopicConfFuns);
}

ERL_NIF_TERM parse_kafka_config(ErlNifEnv* env, ERL_NIF_TERM list, rd_kafka_conf_t* conf)
{
    if(!appy_kafka_default_config(conf))
        return make_error(env, "failed to apply default kafka config");

    return parse_config(env, list, conf, kKafkaConfFuns);
}
