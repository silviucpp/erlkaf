#include "topicmanager.h"
#include "rdkafka.h"

TopicManager::TopicManager(rd_kafka_t *rk) : rk_(rk) { }

TopicManager::~TopicManager()
{
    Cleanup();
}

rd_kafka_topic_t* TopicManager::AddTopic(const std::string& name, rd_kafka_topic_conf_t* conf, bool* already_exist)
{
    CritScope ss(&crt_);

    auto it = topics_.find(name);

    if(it != topics_.end())
    {
        *already_exist = true;
        return NULL;
    }

    *already_exist = false;
    rd_kafka_topic_t* topic = rd_kafka_topic_new(rk_, name.c_str(), conf);

    if(!topic)
        return NULL;

    topics_[name] = topic;
    return topic;
}

void* TopicManager::DeleteTopic(const std::string& name, rd_kafka_DeleteTopic_t* del_topics, bool* not_found)
{
    CritScope ss(&crt_);

    auto it = topics_.find(name);

    if(it == topics_.end())
    {
        *not_found = true;
        return NULL;
    }

    rd_kafka_AdminOptions_t *options;
    options = rd_kafka_AdminOptions_new(rk_, RD_KAFKA_ADMIN_OP_DELETETOPICS);


    *not_found = false;
    rd_kafka_DeleteTopics(rk_, &del_topics, 1, options, NULL);

    rd_kafka_AdminOptions_destroy(options);

    return NULL;
}

void TopicManager::Cleanup()
{
    CritScope ss(&crt_);

    for(auto it = topics_.begin(); it != topics_.end(); ++it)
        rd_kafka_topic_destroy(it->second);

    topics_.clear();
}

// this methods is never called after cleanup so it's safe to partially protect it
// and avoid useless locks

rd_kafka_topic_t* TopicManager::GetOrCreateTopic(const std::string& name)
{
    auto it = topics_.find(name);

    if(it != topics_.end())
        return it->second;

    bool already_exist;
    rd_kafka_topic_t* topic = AddTopic(name, NULL, &already_exist);
    return topic ? topic : (already_exist ? GetOrCreateTopic(name) : NULL);
}
