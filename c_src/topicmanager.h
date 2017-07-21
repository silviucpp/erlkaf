#ifndef ERLKAF_C_SRC_TOPICMANAGER_H_
#define ERLKAF_C_SRC_TOPICMANAGER_H_

#include "rdkafka.h"
#include "macros.h"

#include <map>
#include <string>

class TopicManager
{
public:

    TopicManager(rd_kafka_t *rk);
    ~TopicManager();

    rd_kafka_topic_t* AddTopic(const std::string& name, rd_kafka_topic_conf_t* conf);
    bool ReleaseTopic(const std::string& name);
    rd_kafka_topic_t* GetTopic(const std::string& name);

private:

    DISALLOW_COPY_AND_ASSIGN(TopicManager);

    void Cleanup();

    std::map<std::string, rd_kafka_topic_t*> topics_;
    rd_kafka_t* rk_;
};

#endif
