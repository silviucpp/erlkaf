#include "erlkaf_oauthbearer.h"
#include "rdkafka.h"

#include <string.h>
#include <vector>
#include <sstream>
#include <iostream>

namespace {

char** split_extensions(const std::string extensions_str, size_t* length)
{
    std::stringstream extensions_stream(extensions_str);
    std::string extension_tmp;
    std::string kv_tmp;
    std::vector<std::string> extensions_vector;

    while (getline(extensions_stream, extension_tmp, ','))
    {
        std::stringstream kv_stream(extension_tmp);
        while (getline(kv_stream, kv_tmp, '='))
            extensions_vector.push_back(kv_tmp);
    }

    *length = extensions_vector.size();
    char ** extensions = new char*[*length];

    for(size_t i = 0; i < *length; ++i)
    {
        extensions[i] = new char[extensions_vector[i].size() + 1];
        strcpy(extensions[i], extensions_vector[i].c_str());
    }

    return extensions;
}

void free_extensions(char** extensions, size_t length)
{
    if (extensions != nullptr)
    {
        for (size_t i = 0; i < length; ++i)
            if (extensions[i] != nullptr)
                delete[] extensions[i];

        delete[] extensions;
    }
}

}

ERL_NIF_TERM oauthbearer_set_token(rd_kafka_t* kf, std::string token, long lifetime, std::string principal, std::string extensions_str)
{
    char set_token_errstr[512];
    size_t extension_key_value_cnt = 0;
    char **extension_key_value = NULL;

    if (extensions_str != "")
        extension_key_value = split_extensions(extensions_str, &extension_key_value_cnt);

    if (rd_kafka_oauthbearer_set_token(kf, token.c_str(), lifetime * 1000, principal.c_str(),
        (const char **)extension_key_value, extension_key_value_cnt,
        set_token_errstr, sizeof(set_token_errstr)) != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        rd_kafka_oauthbearer_set_token_failure(kf, set_token_errstr);
        free_extensions(extension_key_value, extension_key_value_cnt);
        return ATOMS.atomError;
    }
    else
    {
        free_extensions(extension_key_value, extension_key_value_cnt);
        return ATOMS.atomOk;
    }
}

ERL_NIF_TERM oauthbearer_set_token_failure(rd_kafka_t* kf, std::string error)
{
    rd_kafka_oauthbearer_set_token_failure(kf, error.c_str());
    return ATOMS.atomOk;
}
