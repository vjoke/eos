/**
 * Refer to https://github.com/edenhill/librdkafka/blob/master/examples/rdkafka_zookeeper_example.c
 */

#include <sys/time.h>
#include <fc/io/json.hpp>
#include <eosio/pubsub_plugin/pubsub_plugin.hpp>
#include <jansson.h>
#include <iostream>

#include "backend.h"

namespace eosio
{

#define BROKER_PATH "/brokers/ids"

static backend *mybackend = NULL;

static void set_brokerlist_from_zookeeper(zhandle_t *zzh, char *brokers)
{
    if (zzh)
    {
        struct String_vector brokerlist;
        if (zoo_get_children(zzh, BROKER_PATH, 1, &brokerlist) != ZOK)
        {
            std::cout << ">>>> no brokers found on path " << BROKER_PATH << "\n";
            return;
        }

        int i;
        char *brokerptr = brokers;
        for (i = 0; i < brokerlist.count; i++)
        {
            char path[255], cfg[1024];
            sprintf(path, "/brokers/ids/%s", brokerlist.data[i]);
            int len = sizeof(cfg);
            zoo_get(zzh, path, 0, cfg, &len, NULL);

            if (len > 0)
            {
                cfg[len] = '\0';
                json_error_t jerror;
                json_t *jobj = json_loads(cfg, 0, &jerror);
                if (jobj)
                {
                    json_t *jhost = json_object_get(jobj, "host");
                    json_t *jport = json_object_get(jobj, "port");

                    if (jhost && jport)
                    {
                        const char *host = json_string_value(jhost);
                        const int port = json_integer_value(jport);
                        sprintf(brokerptr, "%s:%d", host, port);

                        brokerptr += strlen(brokerptr);
                        if (i < brokerlist.count - 1)
                        {
                            *brokerptr++ = ',';
                        }
                    }
                    json_decref(jobj);
                }
            }
        }

        deallocate_String_vector(&brokerlist);
        std::cout << ">>>> "
                  << "found brokers " << brokers << "\n";
    }
}

static void watcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx)
{
    char brokers[1024];
    backend *be = (backend *)watcherCtx;
    FC_ASSERT(be);
    if (type == ZOO_CHILD_EVENT && strncmp(path, BROKER_PATH, sizeof(BROKER_PATH) - 1) == 0)
    {
        brokers[0] = '\0';
        set_brokerlist_from_zookeeper(zh, brokers);
        if (brokers[0] != '\0' && be != NULL && be->getHandle() != NULL)
        {
            rd_kafka_t *rk = be->getHandle();
            rd_kafka_brokers_add(rk, brokers);
            rd_kafka_poll(rk, 10);
        }
    }
}

static zhandle_t *initialize_zookeeper(backend *be, const char *zookeeper, const int debug)
{
    zhandle_t *zh;
    if (debug)
    {
        zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
    }

    zh = zookeeper_init(zookeeper, watcher, 10000, 0, be, 0);
    if (zh == NULL)
    {
        FC_THROW("Zookeeper connection not established.");
    }
    return zh;
}

static void msg_delivered(rd_kafka_t *rk,
                          const rd_kafka_message_t *
                              rkmessage,
                          void *opaque)
{
    // FIXME
    backend *be = NULL;
    int64_t msgid = -1;

    if (rkmessage && rkmessage->_private)
    {
        if (mybackend != rkmessage->_private)
            msgid = (int64_t)rkmessage->_private;
        else
            be = (backend *)rkmessage->_private;
    }

    auto err = rd_kafka_last_error();
    if (err)
    {
        std::cout << ">>>> kafka message deliver error " << rd_kafka_err2str(err) << "\n";
        if (be)
        {
            be->m_log->error++;
        }
        else if (msgid > 0)
        {
            mybackend->m_log->error++;
        }
    }
    else
    {
        if (be)
        {
            be->m_log->success++;
        }
        else if (msgid > 0)
        {
            mybackend->m_log->success++;
        }
    }
}

backend::backend(const std::string &uri, const std::string &topic, const int partition) : m_uri(uri), m_topic(topic), m_partition(partition)
{
    m_log = std::make_shared<pubsub_runtime::kafka_log>();
    FC_ASSERT(m_log);
    m_log->tag = "kafka";
    m_log->latest_block_num = 0;
    m_log->latest_tx_block_num = 0;
    m_log->queue_size = 0;
    m_log->count = 0;
    m_log->sent = 0;
    m_log->success = 0;
    m_log->error = 0;

    if (m_partition < 0)
    {
        std::cout << ">>>> unexpected partition " << m_partition << "\n";
        m_partition = 0;
    }

    char errstr[512];
    char brokers[1024];

    /* Kafka configuration */
    m_conf = rd_kafka_conf_new();

    /* Topic configuration */
    m_topic_conf = rd_kafka_topic_conf_new();

    /* Set kafka version */
    rd_kafka_conf_res_t res = RD_KAFKA_CONF_UNKNOWN;
    res = rd_kafka_conf_set(m_conf, "api.version.request", "false",
                            errstr, sizeof(errstr));

    if (res != RD_KAFKA_CONF_OK)
    {
        FC_THROW("Kafka set conf error ${e}", ("e", errstr));
    }
    // Set the maximum message size to 10M
    res = rd_kafka_conf_set(m_conf, "message.max.bytes", "10485760",
                            errstr, sizeof(errstr));

    if (res != RD_KAFKA_CONF_OK)
    {
        FC_THROW("Kafka set conf error ${e}", ("e", errstr));
    }

    res = rd_kafka_conf_set(m_conf, "broker.version.fallback", "0.8.2.2",
                            errstr, sizeof(errstr));

    if (res != RD_KAFKA_CONF_OK)
    {
        FC_THROW("Kafka set conf error ${e}", ("e", errstr));
    }

    if (m_uri.empty())
    {
        m_uri = std::string("localhost:2181");
    }

    m_zh = initialize_zookeeper(this, m_uri.c_str(), 0);
    /* Add brokers */
    set_brokerlist_from_zookeeper(m_zh, brokers);
    if (rd_kafka_conf_set(m_conf, "metadata.broker.list",
                          brokers, errstr, sizeof(errstr) != RD_KAFKA_CONF_OK))
    {
        FC_THROW("Kafka failed to set brokers: ${e}", ("e", errstr));
    }

    std::cout << ">>>> kafka broker list from zk " << m_uri << " " << brokers << "\n";
    /* Set up a message delivery report callback.
	 * It will be called once for each message, either on successful
	 * delivery to broker, or upon failure to deliver to broker. 
     */
    rd_kafka_conf_set_dr_msg_cb(m_conf, msg_delivered);

    /* Create Kafka handle */
    if (!(m_rk = rd_kafka_new(RD_KAFKA_PRODUCER, m_conf,
                              errstr, sizeof(errstr))))
    {
        FC_THROW("Kafka failed to create new producer ${e}", ("e", errstr));
    }

    /* Create topic */
    m_rkt = rd_kafka_topic_new(m_rk, m_topic.c_str(), m_topic_conf);

    mybackend = this;
    rd_kafka_poll(m_rk, 0);
    std::cout << ">>>> created pubsub backend"
              << "\n";
}

backend::~backend()
{
    /* Destroy topic */
    if (m_rkt != NULL)
    {
        rd_kafka_topic_destroy(m_rkt);
    }

    /* Destroy the handle */
    if (m_rk != NULL)
    {
        rd_kafka_destroy(m_rk);
        m_rk = NULL;
    }

    /* Let background threads clean up and terminate cleanly. */
    rd_kafka_wait_destroyed(2000);

    /** Free the zookeeper data. */
    if (m_zh != NULL)
    {
        zookeeper_close(m_zh);
        m_zh = NULL;
    }

    /* Release resources */
    if (m_conf != NULL)
    {
        // rd_kafka_conf_destroy(m_conf);
        m_conf = NULL;
    }

    if (m_topic_conf != NULL)
    {
        // rd_kafka_topic_conf_destroy(m_topic_conf);
        m_topic_conf = NULL;
    }

    std::cout << ">>>> pubsub backend destroyed"
              << "\n";
}

rd_kafka_t *backend::getHandle()
{
    return m_rk;
}

void backend::wipe()
{
    ilog("wipe backend ...");
}

void backend::publish(const std::string &msg)
{
    try {
        // TODO: use boost::any to hold different messages
        // debugging only global_action_seq
        int64_t msgid = -1;
        void *opaque = NULL;

        // FC_THROW("deliberated ${e}", ("e", "crash")); 

        // if (msg.find("global_action_seq") != std::string::npos)
        // {
        //     auto actions = fc::json::from_string(msg).as<pubsub_message::actions_result>();
        //     m_log->latest_tx_block_num = actions.last_irreversible_block;
        // }
        // else
        // {
        //     auto block = fc::json::from_string(msg).as<pubsub_message::block_result>();
        //     m_log->latest_block_num = block.block_num;
        //     m_latest_block = msg;
        // }

        if (msgid != -1)
        {
            opaque = (void *)(msgid + 1);
        }
        else
        {
            opaque = this;
        }

        /* Send/Produce message. */
        if (rd_kafka_produce(m_rkt, m_partition,
                            RD_KAFKA_MSG_F_COPY,
                            /* Payload and length */
                            (void *)msg.c_str(), msg.length(),
                            /* Optional key and its length */
                            NULL, 0,
                            /* Message opaque, provided in
                            * delivery report callback as
                            * msg_opaque. */
                            opaque) == -1)
        {
            std::cout << ">>>> kafka failed to produce to topic " << rd_kafka_topic_name(m_rkt)
                    << " partition: " << m_partition << " error: " << rd_kafka_errno2err(errno) 
                    << " message size: " << msg.length() << "\n";
            m_log->error++;
        }
        else
        {
            m_log->sent++;
        }

        /* Poll to handle delivery reports */
        rd_kafka_poll(m_rk, 0);
        m_log->queue_size = rd_kafka_outq_len(m_rk);

        dump(msg);
    } catch (...) {
        std::cerr << "failed to publish message to kafka\n";
    }
    
}

void backend::dump(const std::string &msg)
{
    m_log->count++;
    m_log->timestamp = fc::string(fc::time_point::now());
}

} // namespace eosio
