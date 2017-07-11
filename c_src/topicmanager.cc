#include "topicmanager.h"

TopicManager::TopicManager(rd_kafka_t *rk) : rk_(rk)
{

}

TopicManager::~TopicManager()
{
    Cleanup();
}

rd_kafka_topic_t* TopicManager::AddTopic(const std::string& id, const std::string& name, rd_kafka_topic_conf_t *conf)
{
    if(GetTopic(id) != NULL)
        return NULL;

    rd_kafka_topic_t* topic = rd_kafka_topic_new(rk_, name.c_str(), conf);

    if(!topic)
        return NULL;

    topics_[id] = topic;
    return topic;
}

bool TopicManager::ReleaseTopic(const std::string& id)
{
    auto it = topics_.find(id);

    if(it == topics_.end())
        return false;

    rd_kafka_topic_destroy(it->second);
    topics_.erase(it);
    return true;
}

void TopicManager::Cleanup()
{
    for(auto it = topics_.begin(); it != topics_.end(); ++it)
        rd_kafka_topic_destroy(it->second);

    topics_.clear();
}

rd_kafka_topic_t* TopicManager::GetTopic(const std::string& id)
{
    auto it = topics_.find(id);

    if(it == topics_.end())
        return NULL;

    return it->second;
}
