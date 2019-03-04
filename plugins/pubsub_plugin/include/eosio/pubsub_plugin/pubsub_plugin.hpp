/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */

#pragma once

#include <eosio/chain_plugin/chain_plugin.hpp>
#include <appbase/application.hpp>
#include <boost/signals2/connection.hpp>
#include <boost/container/flat_set.hpp>
#include <memory>

#include "consumer.h"

namespace eosio {

class backend;

/**
 * Provides persistence to message broker for:
 *   Blocks
 *   Transactions
 *
 *   The goal ultimately is for all chainbase data to be output to kafka for consuming by other services
 *   Currently, only blocks and transactions are mirrored.
 *  
 *   Tested ok with kafka 0.8.2.2
 */

namespace pubsub_message {

    // FIXME: copied from eosio.token.hpp
    struct transfer_args {
        account_name from;
        account_name to;
        asset        quantity;
        string       memo;
    };

    struct transfer_action {
        account_name    account;
        action_name     name;
        // action parameters
        account_name    from;
        account_name    to;
        asset           quantity;
        string          memo; 
    };

    struct transaction_result {
        std::string                 trx_id;
        int                         status;
        unsigned int                cpu_usage_us;
        unsigned int                net_usage_words;
        vector<transfer_action>     transfer_actions;
    };

    struct block_result {
        uint64_t                    block_num;
        std::string                 block_id;
        std::string                 prev_block_id;
        fc::variant                 timestamp;
        std::string                 transaction_merkle_root;
        uint32_t                    transaction_count;
        std::string                 producer;

        vector<transaction_result>  transactions;
    };

    using block_result_ptr = std::shared_ptr<block_result>;
    
    struct ordered_action_result {
         uint64_t                     global_action_seq = 0;
         int32_t                      account_action_seq = 0;
         uint64_t                     block_num;
         chain::block_timestamp_type  block_time;
         fc::variant                  action_trace;
         uint8_t                      depth;
    };
    // Borrowed from history_plugin
    struct actions_result {
         vector<ordered_action_result> actions;
         uint64_t                      last_irreversible_block;
         optional<bool>                time_limit_exceeded_error;
      };

    using actions_result_ptr = std::shared_ptr<actions_result>;
    using message_ptr = std::shared_ptr<std::string>;
} // pubsub_message

namespace pubsub_runtime {
    struct pubsub_log {
        std::string tag;                // tag of this log
        std::string timestamp;          // timestamp
        int64_t lib;                    // last irreversible block num
        int64_t count;                  // message counter, including blocks and transactions
        int64_t latest_block_num;       // latest block num of on_block
        int64_t latest_tx_block_num;    // latest block of tx message
        uint32_t queue_size;            // fifo pending queue size
    };

    using pubsub_log_ptr = std::shared_ptr<pubsub_log>;

    struct kafka_log {
        std::string tag;                // tag of this log
        std::string timestamp;          // timestamp
        int64_t latest_block_num;       // latest block num of on_block
        int64_t latest_tx_block_num;    // latest block of tx message
        uint32_t queue_size;            // pending queue size
        int64_t count;                  // publish counter
        int64_t sent;                   // send counter
        int64_t success;                // success counter
        int64_t error;                  // error counter
    };

    using kafka_log_ptr = std::shared_ptr<kafka_log>;

    struct runtime_status {
        pubsub_log plugin;
        kafka_log kafka;
    };
} // pubsub_runtime

using pubsub_plugin_impl_ptr = std::shared_ptr<class pubsub_plugin_impl>;
class pubsub_plugin final : public plugin<pubsub_plugin> {
public:
    APPBASE_PLUGIN_REQUIRES((chain_plugin))
    pubsub_plugin();
    virtual void set_program_options(options_description& cli, options_description& cfg) override;

    void plugin_initialize(const variables_map& options);
    void plugin_startup();
    void plugin_shutdown();
    pubsub_runtime::runtime_status status();
    
private:
    pubsub_plugin_impl_ptr my;
};
}

FC_REFLECT( eosio::pubsub_message::transfer_args, (from)(to)(quantity)(memo) )
FC_REFLECT( eosio::pubsub_message::transfer_action, (account)(name)(from)(to)(quantity)(memo) )
FC_REFLECT( eosio::pubsub_message::transaction_result, (trx_id)(status)(cpu_usage_us)(net_usage_words)(transfer_actions) )
FC_REFLECT( eosio::pubsub_message::block_result, (block_num)(block_id)(prev_block_id)(timestamp)(transaction_merkle_root)(transaction_count)(producer)(transactions) )
FC_REFLECT( eosio::pubsub_message::actions_result, (actions)(last_irreversible_block)(time_limit_exceeded_error) )
FC_REFLECT( eosio::pubsub_message::ordered_action_result, (global_action_seq)(account_action_seq)(block_num)(block_time)(action_trace)(depth) )
FC_REFLECT( eosio::pubsub_runtime::pubsub_log, (tag)(timestamp)(lib)(count)(latest_block_num)(latest_tx_block_num)(queue_size))
FC_REFLECT( eosio::pubsub_runtime::kafka_log, (tag)(timestamp)(latest_block_num)(latest_tx_block_num)(queue_size)(count)(sent)(success)(error))
FC_REFLECT( eosio::pubsub_runtime::runtime_status, (plugin)(kafka))