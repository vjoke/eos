/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */
#include <eosio/pubsub_plugin/pubsub_plugin.hpp>
#include <eosio/http_plugin/http_plugin.hpp>
#include <iostream>
#include <fc/io/json.hpp>

#include "consumer_core.h"
#include "applied_message.h"

namespace {
const char* PUBSUB_URI_OPTION = "pubsub-uri";
const char* PUBSUB_TOPIC_OPTION = "pubsub-topic";
const char* PUBSUB_PARTITION_OPTION = "pubsub-partition";
const char* PUBSUB_BLOCK_MARGIN_OPTION = "pubsub-block-margin";
const char* PUBSUB_BLOCK_OFFSET_OPTION = "pubsub-block-offset";
}

namespace fc { class variant; }

namespace eosio {

static appbase::abstract_plugin& _pubsub_plugin = app().register_plugin<pubsub_plugin>();

using namespace pubsub_message;
using namespace eosio;

#define CALL(api_name, api_handle, call_name, INVOKE, http_response_code) \
{std::string("/v1/" #api_name "/" #call_name), \
   [api_handle](string, string body, url_response_callback cb) mutable { \
          try { \
             if (body.empty()) body = "{}"; \
             INVOKE \
             cb(http_response_code, fc::json::to_string(result)); \
          } catch (...) { \
             http_plugin::handle_exception(#api_name, #call_name, body, cb); \
          } \
       }}

#define INVOKE_R_V(api_handle, call_name) \
     auto result = api_handle->call_name();

pubsub_plugin::pubsub_plugin()
{
    m_chain_plug = app().find_plugin<chain_plugin>();
    FC_ASSERT(m_chain_plug);

    m_activated = false;
    m_block_margin = 0;
    m_block_offset = 0;
    
    m_log = std::make_shared<pubsub_runtime::pubsub_log>();
    FC_ASSERT(m_log);
    m_log->tag = "plugin";
    m_log->lib = 0;
    m_log->count = 0;
    m_log->latest_block_num = 0;
    m_log->latest_tx_block_num = 0;
    m_log->queue_size = 0;

}

void pubsub_plugin::set_program_options(options_description& cli, options_description& cfg)
{
    dlog("set_program_options");

    cfg.add_options()
            (PUBSUB_URI_OPTION, bpo::value<std::string>(),
             "Pubsub zookeeper URI array"
             " Default url 'localhost' is used if not specified in URI.")
            (PUBSUB_TOPIC_OPTION, bpo::value<std::string>(),
             "Pubsub topic string"
             " Default 'EosWallet' is used if not specified.")
            (PUBSUB_PARTITION_OPTION, bpo::value<int>(),
             "Pubsub topic partitions"
             " Default 0 is used if not specified.")
            (PUBSUB_BLOCK_MARGIN_OPTION, bpo::value<int>(),
             "Pubsub margin blocks"
             " Default 2000 is used if not specified.")
            (PUBSUB_BLOCK_OFFSET_OPTION, bpo::value<int64_t>(),
             "Pubsub block offset"
             " Default 0 is used if not specified.")
            ;
}

void pubsub_plugin::plugin_initialize(const variables_map& options)
{
    ilog("initialize");
    
    std::string uri_str, topic_str;
    int topic_partition = 0;
    int block_margin = 2000;
    int64_t block_offset = 0;
    
    if (options.count(PUBSUB_URI_OPTION)) {
        uri_str = options.at(PUBSUB_URI_OPTION).as<std::string>();
        if (uri_str.empty()) {
            wlog("db URI not specified => use 'localhost' instead.");
            uri_str = "localhost";
        }
    }
    
    if (options.count(PUBSUB_TOPIC_OPTION)) {
        topic_str = options.at(PUBSUB_TOPIC_OPTION).as<std::string>();
        if (topic_str.empty()) {
            wlog("topic not specified => use 'EosWallet' instead.");
            topic_str = "EosWallet";
        }
    }

    if (options.count(PUBSUB_PARTITION_OPTION)) {
        topic_partition = options.at(PUBSUB_PARTITION_OPTION).as<int>();
        if (topic_partition < 0) {
            wlog("topic partition not specified => use 0 instead.");
            topic_partition = 0;
        }
    }
    
    if (options.count(PUBSUB_BLOCK_MARGIN_OPTION)) {
        block_margin = options.at(PUBSUB_BLOCK_MARGIN_OPTION).as<int>();
        if (block_margin < 0) {
            wlog("block margin not specified => use 2000 instead.");
            block_margin = 2000;
        }
    }

    if (options.count(PUBSUB_BLOCK_OFFSET_OPTION)) {
        block_offset = options.at(PUBSUB_BLOCK_OFFSET_OPTION).as<int64_t>();
        if (block_offset < 0) {
            wlog("block offset not specified => use 0 instead.");
            block_offset = 0;
        }
    }

    ilog("Publish to ${u} with topic=${t} partition=${p} block_margin=${m} block_offset=${b}", 
        ("u", uri_str)("t", topic_str)("p", topic_partition)("m", block_margin)("b", block_offset));

    m_block_margin = block_margin;
    m_block_offset = block_offset;

    m_be = std::make_shared<backend>(uri_str, topic_str, topic_partition);

    if (options.at("replay-blockchain").as<bool>() ||
         options.at("hard-replay-blockchain").as<bool>() ||
         options.at("delete-all-blocks").as<bool>()) {
        ilog("Resync requested: wiping backend");
        m_be->wipe();
    }
    auto& chain = m_chain_plug->chain();

    m_applied_message_consumer = std::make_unique<consumer<pubsub_message::message_ptr>>(std::make_unique<applied_message>(m_be));

    m_accepted_block_connection.emplace(chain.accepted_block.connect([=](const chain::block_state_ptr &b) {
        on_block(b);
        dump();
    }));

    m_applied_transaction_connection.emplace(chain.applied_transaction.connect([=](const chain::transaction_trace_ptr& t) {
        on_transaction(t);
        dump();
    }));
}

void pubsub_plugin::on_block(const chain::block_state_ptr& b)
{
    auto &chain = m_chain_plug->chain();
    auto lib = chain.last_irreversible_block_num();
    if (lib <= m_log->lib) {
        return;
    }

    auto process_block = [&](const uint32_t bn) -> auto {
        const auto block = chain.fetch_block_by_number(bn);
        if (block != nullptr) {
            push_block(block);
        }
    };

    int64_t i = 0;
    if (0 == m_log->lib) {
        // first time, may lose some block
        i = (lib - m_block_margin) > 0 ? (lib - m_block_margin) : lib;
        wlog("#### start block num ${i}", ("i", i));
    } else {
        i = m_log->lib + 1;
    }

    for (; i <= lib; i++) {
        process_block(i);
    }

    m_log->lib = lib;
}

void pubsub_plugin::push_block(const chain::signed_block_ptr& block) 
{
    const auto block_num = static_cast<int32_t>(block->block_num());

    if ((!m_activated) && (block_num >= m_block_offset)) {
        m_activated = true;
    }

    if (!m_activated) {
        return;
    } 

    // for debugging
    m_log->latest_block_num = block_num;

    block_result_ptr br = std::make_shared<block_result>();
    const auto block_id = block->id().str();
    const auto prev_block_id = block->previous.str();
    const auto timestamp = std::chrono::milliseconds{
                            std::chrono::seconds{block->timestamp.operator fc::time_point().sec_since_epoch()}}.count();
    const auto transaction_merkle_root = block->transaction_mroot.str();
    const auto transaction_count = block->transactions.size(); 
    const auto producer = block->producer.to_string();

    br->block_num = block_num;
    br->block_id = block_id;
    br->prev_block_id = prev_block_id;
    br->timestamp = timestamp;
    br->transaction_merkle_root = transaction_merkle_root;
    br->transaction_count = transaction_count;
    br->producer = producer;
    // get status from receipt
    for (const auto&receipt : block->transactions) {
        chain::transaction_id_type trx_id;

        std::vector<pubsub_message::transfer_args> transfer_actions;
        if (receipt.trx.contains<chain::transaction_id_type>()) {
            trx_id = receipt.trx.get<chain::transaction_id_type>(); 
            elog("#### unexpected receipt");
        } else {
            auto& pt = receipt.trx.get<chain::packed_transaction>();
            auto mtrx = std::make_shared<chain::transaction_metadata>(pt);
            trx_id = mtrx->id;
            parse_transfer_actions(mtrx, transfer_actions);
        }

        const auto cpu_usage_us = receipt.cpu_usage_us;
        const auto net_usage_words = receipt.net_usage_words;

        br->transactions.emplace_back(transaction_result{
            trx_id.str(),
            receipt.status,
            cpu_usage_us,
            net_usage_words,
            std::move(transfer_actions)
        });
    }

    const std::string &block_str = fc::json::to_pretty_string(*br);
    message_ptr block_msg = std::make_shared<std::string>(block_str);
    m_log->queue_size = m_applied_message_consumer->push(block_msg);
    m_log->count++;
}

void pubsub_plugin::parse_transfer_actions(const chain::transaction_metadata_ptr& tm, std::vector<pubsub_message::transfer_args> &results)
{
    try {
        for (const auto &act: tm->trx.actions) {
            if (act.account == N(eosio.token) && act.name == N(transfer)) {
                auto transfer = act.data_as<pubsub_message::transfer_args>();
                    results.emplace_back(std::move(transfer));
            }
        }

        for (const auto &act: tm->trx.context_free_actions) {
            if (act.account == N(eosio.token) && act.name == N(transfer)) {
                auto transfer = act.data_as<pubsub_message::transfer_args>();
                    results.emplace_back(std::move(transfer));
            }
        }
    } catch (...) {
        elog("failed to parse transfer action for txid:${txid}", ("txid", tm->id.str()));
    }
}

void pubsub_plugin::on_transaction(const chain::transaction_trace_ptr& trace) {
    auto& chain = m_chain_plug->chain();
    const auto abi_serializer_max_time = m_chain_plug->get_abi_serializer_max_time();

    actions_result_ptr result = std::make_shared<actions_result>();
    auto block_num = chain.pending_block_state()->block_num;
    result->last_irreversible_block = block_num;

    if ((!m_activated) && (result->last_irreversible_block >= m_block_offset)) {
        m_activated = true;
    }
    
    if (!m_activated) {
        return;
    }

    for( const auto& at : trace->action_traces ) {
        int32_t account_action_seq = 0; 
        if (at.act.name != N(onblock)) {
            result->actions.emplace_back( ordered_action_result{
                                 at.receipt.global_sequence,
                                 account_action_seq,
                                 chain.pending_block_state()->block_num, 
                                 chain.pending_block_time(),
                                 chain.to_variant_with_abi(at, abi_serializer_max_time)
                                 });

        }
    }

    if (result->actions.size() > 0) {
        std::string tx_str = fc::json::to_pretty_string(*result);
        message_ptr tx_msg = std::make_shared<std::string>(tx_str);
        m_log->latest_tx_block_num = block_num;
        m_log->queue_size = m_applied_message_consumer->push(tx_msg);
        m_log->count++;
    }
}

void pubsub_plugin::dump() 
{
    m_log->timestamp = fc::string(fc::time_point::now());
}

void pubsub_plugin::plugin_startup()
{
    ilog("startup");
    auto plugin = app().find_plugin<http_plugin>();
    if (plugin == nullptr) {
        wlog("pubsub API is not available since http plugin is missing");
        return;
    }

    plugin->add_api({
       CALL(pubsub, this, status,
            INVOKE_R_V(this, status), 200),
   });
}

pubsub_runtime::runtime_status pubsub_plugin::status()
{
    pubsub_runtime::runtime_status ret;

    ret.plugin = *m_log;
    ret.kafka = *m_be->m_log;

    return ret;
}

void pubsub_plugin::plugin_shutdown()
{
    ilog("shutdown ...");
    m_accepted_block_connection.reset();
    m_applied_transaction_connection.reset();
    m_be = nullptr;
    m_applied_message_consumer = nullptr;
    ilog("shutdown ok");
}

#undef INVOKE_R_V
#undef CALL

} // namespace eosio

