#ifndef BACKEND_H
#define BACKEND_H

#include <memory>
#include <mutex>

#include <fc/log/logger.hpp>

#include <librdkafka/rdkafka.h>
// FIXME: zookeeper reports many errors
#ifdef DLL_EXPORT
#define ADHOC_UNDEF_EXPORT
#undef DLL_EXPORT
#endif
#include <zookeeper/zookeeper.h>
#include <zookeeper/zookeeper.jute.h>
#ifdef ADHOC_UNDEF_EXPORT
#define DLL_EXPORT
#endif

#include <eosio/pubsub_plugin/pubsub_plugin.hpp>
#include <chainbase/chainbase.hpp>
namespace eosio {

using chainbase::database;

class backend
{
public:
    backend(const std::string& uri, const std::string& topic, const int partition);
    ~backend();
    void wipe();

    void publish(const std::string &msg);
    void dump(const std::string &msg);
    
    rd_kafka_t *getHandle();
    pubsub_runtime::kafka_log_ptr m_log;
private:
    std::string m_uri, m_topic;
    int m_partition;
    // context for rdkafka
    rd_kafka_t *m_rk;
    rd_kafka_topic_t *m_rkt;
    zhandle_t *m_zh;
    rd_kafka_conf_t *m_conf;
	rd_kafka_topic_conf_t *m_topic_conf; 

    std::string m_latest_block;
};

} // namespace

#endif // BACKEND_H
