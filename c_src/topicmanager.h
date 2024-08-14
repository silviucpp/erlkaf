#ifndef C_SRC_TOPICMANAGER_H_
#define C_SRC_TOPICMANAGER_H_

#include "macros.h"
#include "critical_section.h"
#include "rdkafka.h"

#include <map>
#include <string>

typedef struct rd_kafka_s rd_kafka_t;
typedef struct rd_kafka_topic_s rd_kafka_topic_t;
typedef struct rd_kafka_topic_conf_s rd_kafka_topic_conf_t;

class TopicManager
{
public:

    explicit TopicManager(rd_kafka_t *rk);
    ~TopicManager();

    rd_kafka_topic_t* AddTopic(const std::string& name, rd_kafka_topic_conf_t* conf, bool* already_exist);
    void* DeleteTopic(const std::string& name, rd_kafka_DeleteTopic_t* del_topics, bool* not_found);
    rd_kafka_topic_t* GetOrCreateTopic(const std::string& name);

private:
    void Cleanup();

    CriticalSection crt_;
    std::map<std::string, rd_kafka_topic_t*> topics_;
    rd_kafka_t* rk_;

    DISALLOW_COPY_AND_ASSIGN(TopicManager);
};

#endif  // C_SRC_TOPICMANAGER_H_
