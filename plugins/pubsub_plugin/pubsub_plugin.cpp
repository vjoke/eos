/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */
#include <eosio/pubsub_plugin/pubsub_plugin.hpp>
#include <eosio/http_plugin/http_plugin.hpp>
#include <iostream>
#include <fc/io/json.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/chrono.hpp>

#include "consumer_core.h"
#include "applied_message.h"

// #define TEST_ENV

namespace {
const char* PUBSUB_URI_OPTION           = "pubsub-uri";
const char* PUBSUB_TOPIC_OPTION         = "pubsub-topic";
const char* PUBSUB_PARTITION_OPTION     = "pubsub-partition";
const char* PUBSUB_BLOCK_MARGIN_OPTION  = "pubsub-block-margin";
const char* PUBSUB_BLOCK_OFFSET_OPTION  = "pubsub-block-offset";
const char* PUBSUB_ACCEPT_BLOCK         = "pubsub-accept-block";
const char* PUBSUB_OUTPUT_INLINE_ACTIONS  = "pubsub-output-inline-actions";
const char* PUBSUB_BLOCK_START          = "pubsub-block-start";
const char* PUBSUB_UPDATE_VIA_BLOCK_NUM = "pubsub-update-via-block-num";
const char* PUBSUB_STORE_BLOCKS         = "pubsub-store-blocks";
const char* PUBSUB_STORE_BLOCK_STATES   = "pubsub-store-block-states";
const char* PUBSUB_STORE_TRANSACTIONS   = "pubsub-store-transactions";
const char* PUBSUB_STORE_TRANSACTION_TRACES = "pubsub-store-transaction-traces"; 
const char* PUBSUB_STORE_ACTION_TRACES  = "pubsub-store-action-traces";
const char* PUBSUB_FILTER_ON            = "pubsub-filter-on";
const char* PUBSUB_FILTER_OUT           = "pubsub-filter-out";
}

namespace fc { class variant; }

namespace eosio {

using chain::account_name;
using chain::action_name;
using chain::block_id_type;
using chain::permission_name;
using chain::transaction;
using chain::signed_transaction;
using chain::signed_block;
using chain::transaction_id_type;
using chain::packed_transaction;

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


struct filter_entry {
   name receiver;
   name action;
   name actor;

   friend bool operator<( const filter_entry& a, const filter_entry& b ) {
      return std::tie( a.receiver, a.action, a.actor ) < std::tie( b.receiver, b.action, b.actor );
   }

   //            receiver          action       actor
   bool match( const name& rr, const name& an, const name& ar ) const {
      return (receiver.value == 0 || receiver == rr) &&
             (action.value == 0 || action == an) &&
             (actor.value == 0 || actor == ar);
   }
};

class pubsub_plugin_impl {
public:
    pubsub_plugin_impl(); 
    ~pubsub_plugin_impl();

    fc::optional<boost::signals2::scoped_connection> accepted_block_connection;
    fc::optional<boost::signals2::scoped_connection> irreversible_block_connection;
    fc::optional<boost::signals2::scoped_connection> accepted_transaction_connection;
    fc::optional<boost::signals2::scoped_connection> applied_transaction_connection;

    void accepted_block( const chain::block_state_ptr& );
    void applied_irreversible_block(const chain::block_state_ptr&);
    void accepted_transaction(const chain::transaction_metadata_ptr&);
    void applied_transaction(const chain::transaction_trace_ptr&);
    void process_accepted_transaction(const chain::transaction_metadata_ptr&);
    void _process_accepted_transaction(const chain::transaction_metadata_ptr&);
    void process_applied_transaction(const chain::transaction_trace_ptr&);
    void _process_applied_transaction(const chain::transaction_trace_ptr&);
    void process_accepted_block( const chain::block_state_ptr& );
    void _process_accepted_block( const chain::block_state_ptr& );
    void process_irreversible_block(const chain::block_state_ptr&);
    void _process_irreversible_block(const chain::block_state_ptr&);

    bool add_action_trace( std::vector<ordered_action_result> &actions, const chain::action_trace& atrace,
                          const chain::transaction_trace_ptr& t,
                          bool executed, const std::chrono::milliseconds& now,
                          bool& write_ttrace, uint8_t depth );
    bool filter_include( const account_name& receiver, const action_name& act_name,
                        const vector<chain::permission_level>& authorization ) const;
    bool filter_include( const transaction& trx ) const;

    void init();
    void wipe();

    bool configured{false};
    bool wipe_data_on_startup{false};
    bool m_accept_block = false;
    bool m_output_inline_actions = true;
    uint32_t m_start_block_num = 0;
    std::atomic_bool m_start_block_reached{false};

    bool m_is_producer = false;
    bool m_filter_on_star = true;
    std::set<filter_entry> m_filter_on;
    std::set<filter_entry> m_filter_out;
    bool m_update_blocks_via_block_num = false;
    bool m_store_blocks = true;
    bool m_store_block_states = true;
    bool m_store_transactions = true;
    bool m_store_transaction_traces = true;
    bool m_store_action_traces = true;
    std::atomic_bool m_startup{true};
    fc::optional<chain::chain_id_type> m_chain_id;
    // fc::microseconds abi_serializer_max_time;

    int64_t m_block_offset;
    int64_t m_block_margin;
    int64_t m_unexpected_txid;
    int64_t m_unpack_failed;
    
    bool m_activated = false;
    std::unique_ptr<consumer<pubsub_message::message_ptr>> m_applied_message_consumer;

    chain_plugin*  m_chain_plug;
    std::shared_ptr<backend> m_be;

    pubsub_runtime::pubsub_log_ptr m_log;

    void parse_transfer_actions(const chain::transaction_metadata_ptr& tm, std::vector<pubsub_message::transfer_action> &results);

private:
    void on_transaction(const chain::transaction_trace_ptr& t);
    void on_block(const chain::block_state_ptr& b);
    void push_block(const chain::signed_block_ptr& block);
};

bool pubsub_plugin_impl::filter_include( const account_name& receiver, const action_name& act_name,
                                           const vector<chain::permission_level>& authorization ) const
{
   bool include = false;
   if( m_filter_on_star ) {
      include = true;
   } else {
      auto itr = std::find_if( m_filter_on.cbegin(), m_filter_on.cend(), [&receiver, &act_name]( const auto& filter ) {
         return filter.match( receiver, act_name, 0 );
      } );
      if( itr != m_filter_on.cend() ) {
         include = true;
      } else {
         for( const auto& a : authorization ) {
            auto itr = std::find_if( m_filter_on.cbegin(), m_filter_on.cend(), [&receiver, &act_name, &a]( const auto& filter ) {
               return filter.match( receiver, act_name, a.actor );
            } );
            if( itr != m_filter_on.cend() ) {
               include = true;
               break;
            }
         }
      }
   }

   if( !include ) { return false; }
   if( m_filter_out.empty() ) { return true; }

   auto itr = std::find_if( m_filter_out.cbegin(), m_filter_out.cend(), [&receiver, &act_name]( const auto& filter ) {
      return filter.match( receiver, act_name, 0 );
   } );
   if( itr != m_filter_out.cend() ) { return false; }

   for( const auto& a : authorization ) {
      auto itr = std::find_if( m_filter_out.cbegin(), m_filter_out.cend(), [&receiver, &act_name, &a]( const auto& filter ) {
         return filter.match( receiver, act_name, a.actor );
      } );
      if( itr != m_filter_out.cend() ) { return false; }
   }

   return true;
}

bool pubsub_plugin_impl::filter_include( const transaction& trx ) const
{
   if( !m_filter_on_star || !m_filter_out.empty() ) {
      bool include = false;
      for( const auto& a : trx.actions ) {
         if( filter_include( a.account, a.name, a.authorization ) ) {
            include = true;
            break;
         }
      }
      if( !include ) {
         for( const auto& a : trx.context_free_actions ) {
            if( filter_include( a.account, a.name, a.authorization ) ) {
               include = true;
               break;
            }
         }
      }
      return include;
   }
   return true;
}

void pubsub_plugin_impl::accepted_transaction( const chain::transaction_metadata_ptr& t ) {
   try {
      if( m_store_transactions ) {
          process_accepted_transaction(t);
      }
   } catch (fc::exception& e) {
      elog("FC Exception while accepted_transaction ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while accepted_transaction ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while accepted_transaction");
   }
}

void pubsub_plugin_impl::applied_transaction( const chain::transaction_trace_ptr& t ) {
   try {
      // Traces emitted from an incomplete block leave the producer_block_id as empty.
      //
      // Avoid adding the action traces or transaction traces to the database if the producer_block_id is empty.
      // This way traces from speculatively executed transactions are not included in the Mongo database which can
      // avoid potential confusion for consumers of that database.
      //
      // Due to forks, it could be possible for multiple incompatible action traces with the same block_num and trx_id
      // to exist in the database. And if the producer double produces a block, even the block_time may not
      // disambiguate the two action traces. Without a producer_block_id to disambiguate and determine if the action
      // trace comes from an orphaned fork branching off of the blockchain, consumers of the Mongo DB database may be
      // reacting to a stale action trace that never actually executed in the current blockchain.
      //
      // It is better to avoid this potential confusion by not logging traces from speculative execution, i.e. emitted
      // from an incomplete block. This means that traces will not be recorded in speculative read-mode, but
      // users should not be using the mongo_db_plugin in that mode anyway.
      //
      // Allow logging traces if node is a producer for testing purposes, so a single nodeos can do both for testing.
      //
      // It is recommended to run mongo_db_plugin in read-mode = read-only.
      //
      if( !m_is_producer && !t->producer_block_id.valid() )
         return;

        process_applied_transaction(t);
      
   } catch (fc::exception& e) {
      elog("FC Exception while applied_transaction ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while applied_transaction ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while applied_transaction");
   }
}

void pubsub_plugin_impl::applied_irreversible_block( const chain::block_state_ptr& bs ) {
    try {
        if (m_accept_block) {
            return;
        }

        if( m_store_blocks || m_store_block_states || m_store_transactions ) {
            process_irreversible_block(bs);
        }
    } catch (fc::exception& e) {
        elog("FC Exception while applied_irreversible_block ${e}", ("e", e.to_string()));
    } catch (std::exception& e) {
        elog("STD Exception while applied_irreversible_block ${e}", ("e", e.what()));
    } catch (...) {
        elog("Unknown exception while applied_irreversible_block");
    }
}

void pubsub_plugin_impl::accepted_block( const chain::block_state_ptr& bs ) {
    try {
        if( !m_start_block_reached ) {
            if( bs->block_num >= m_start_block_num ) {
                m_start_block_reached = true;
            }
        }

        if (!m_accept_block) {
            return;
        }

        if( m_store_blocks || m_store_block_states ) {
            process_accepted_block(bs);
        }
    } catch (fc::exception& e) {
        elog("FC Exception while accepted_block ${e}", ("e", e.to_string()));
    } catch (std::exception& e) {
        elog("STD Exception while accepted_block ${e}", ("e", e.what()));
    } catch (...) {
        elog("Unknown exception while accepted_block");
    }
}

void handle_pubsub_exception( const std::string& desc, int line_num ) {
    bool shutdown = true;
    try {
        try {
            throw;
        } catch( fc::exception& er ) {
            elog( "pubsub fc exception, ${desc}, line ${line}, ${details}",
                ("desc", desc)( "line", line_num )( "details", er.to_detail_string()));
        } catch( const std::exception& e ) {
            elog( "pubsub std exception, ${desc}, line ${line}, ${what}",
                ("desc", desc)( "line", line_num )( "what", e.what()));
        } catch( ... ) {
            elog( "pubsub unknown exception, ${desc}, line ${line_nun}", ("desc", desc)( "line_num", line_num ));
        }
    } catch (...) {
        std::cerr << "Exception attempting to handle exception for " << desc << " " << line_num << std::endl;
    }

    if( shutdown ) {
        // shutdown if pubsub plugin failed to provide opportunity to fix issue and restart
        app().quit();
    }
}

void pubsub_plugin_impl::process_accepted_transaction( const chain::transaction_metadata_ptr& t ) {
    try {
        if( m_start_block_reached ) {
            _process_accepted_transaction( t );
        }
    } catch (fc::exception& e) {
        elog("FC Exception while processing accepted transaction metadata: ${e}", ("e", e.to_detail_string()));
    } catch (std::exception& e) {
        elog("STD Exception while processing accepted tranasction metadata: ${e}", ("e", e.what()));
    } catch (...) {
        elog("Unknown exception while processing accepted transaction metadata");
    }
}

void pubsub_plugin_impl::process_applied_transaction( const chain::transaction_trace_ptr& t ) {
    try {
        // always call since we need to capture setabi on accounts even if not storing transaction traces
        _process_applied_transaction( t );
    } catch (fc::exception& e) {
        elog("FC Exception while processing applied transaction trace: ${e}", ("e", e.to_detail_string()));
    } catch (std::exception& e) {
        elog("STD Exception while processing applied transaction trace: ${e}", ("e", e.what()));
    } catch (...) {
        elog("Unknown exception while processing applied transaction trace");
    }
}

void pubsub_plugin_impl::process_irreversible_block(const chain::block_state_ptr& bs) {
    try {
        if( m_start_block_reached ) {
            _process_irreversible_block( bs );
        }
    } catch (fc::exception& e) {
        elog("FC Exception while processing irreversible block: ${e}", ("e", e.to_detail_string()));
    } catch (std::exception& e) {
        elog("STD Exception while processing irreversible block: ${e}", ("e", e.what()));
    } catch (...) {
        elog("Unknown exception while processing irreversible block");
    }
}

void pubsub_plugin_impl::process_accepted_block( const chain::block_state_ptr& bs ) {
    try {
        if( m_start_block_reached ) {
            _process_accepted_block( bs );
        }
    } catch (fc::exception& e) {
        elog("FC Exception while processing accepted block trace ${e}", ("e", e.to_string()));
    } catch (std::exception& e) {
        elog("STD Exception while processing accepted block trace ${e}", ("e", e.what()));
    } catch (...) {
        elog("Unknown exception while processing accepted block trace");
    }
}

void pubsub_plugin_impl::_process_accepted_transaction( const chain::transaction_metadata_ptr& t ) {

}

bool pubsub_plugin_impl::add_action_trace( std::vector<ordered_action_result> &actions, const chain::action_trace& atrace,
                                        const chain::transaction_trace_ptr& t,
                                        bool executed, const std::chrono::milliseconds& now,
                                        bool& write_ttrace, uint8_t depth )
{
    auto& chain = m_chain_plug->chain();
    const auto abi_serializer_max_time = m_chain_plug->get_abi_serializer_max_time();

    // if( executed && atrace.receipt.receiver == chain::config::system_account_name ) {
    //     update_account( atrace, atrace.act );
    // }

    bool added = false;
    const bool in_filter = (m_store_action_traces || m_store_transaction_traces) && m_start_block_reached &&
                        filter_include( atrace.receipt.receiver, atrace.act.name, atrace.act.authorization );
    write_ttrace |= in_filter;
    if( m_start_block_reached && m_store_action_traces && in_filter ) {
        const chain::base_action_trace& base = atrace; // without inline action traces
        // filter out non-transfer transactions
        try {
            auto transfer = fc::raw::unpack<pubsub_message::transfer_args>(base.act.data);
            actions.emplace_back( 
                ordered_action_result{
                    atrace.receipt.global_sequence,
                    0, // FIXME: 
                    chain.pending_block_state()->block_num, 
                    chain.pending_block_time(),
                    chain.to_variant_with_abi(base, abi_serializer_max_time),
                    depth
                }
            );

            added = true;
        } catch (...) {
            // not transfer action
            // if( m_unpack_failed++ % 1000 == 0 ) {
            //     elog("#### failed to unpack transfer action in ${txid} total: ${errs}", ("txid", t->id.str())("errs", m_unpack_failed)); // FIXME: 
            // }
        }
    }

    if (!m_output_inline_actions)
        return added;
    // FIXME: split inline transactions, a single transfer action will have more than 
    // one record due to notification medchanism.
    for( const auto& iline_atrace : atrace.inline_traces ) {
        added |= add_action_trace( actions, iline_atrace, t, executed, now, write_ttrace, ++depth );
    }

    return added;
}

void pubsub_plugin_impl::_process_applied_transaction( const chain::transaction_trace_ptr& t ) 
{
    auto& chain = m_chain_plug->chain();
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::microseconds{fc::time_point::now().time_since_epoch().count()});

    bool write_atraces = false;
    bool write_ttrace = false; // filters apply to transaction_traces as well
    bool executed = t->receipt.valid() && t->receipt->status == chain::transaction_receipt_header::executed;

    actions_result_ptr result = std::make_shared<actions_result>();
    auto block_num = chain.pending_block_state()->block_num;
    result->last_irreversible_block = block_num; // FIXME: 

    // if ((!m_activated) && (result->last_irreversible_block >= m_block_offset)) {
    //     m_activated = true;
    // }
    
    // if (!m_activated) {
    //     return;
    // }

    for( const auto& atrace : t->action_traces ) {
        try {
            write_atraces |= add_action_trace( result->actions, atrace, t, executed, now, write_ttrace, 0 );
        } catch(...) {
            handle_pubsub_exception("add action traces", __LINE__);
        }
    }

    if( !m_start_block_reached ) return; //< add_action_trace calls update_account which must be called always

    if( write_atraces ) {
        try {
            if (result->actions.size() > 0) {
                #ifdef TEST_ENV
                std::string tx_str = fc::json::to_pretty_string(*result);
                idump((tx_str));
                #else
                std::string tx_str = fc::json::to_string(*result);
                message_ptr tx_msg = std::make_shared<std::string>(tx_str);
                m_log->queue_size = m_applied_message_consumer->push(tx_msg);
                #endif
                m_log->latest_tx_block_num = block_num;
                m_log->count++;
                m_log->timestamp = fc::string(fc::time_point::now());
            }
        } catch( ... ) {
            handle_pubsub_exception( "publish action traces", __LINE__ );
        }
    }
}

void pubsub_plugin_impl::_process_accepted_block( const chain::block_state_ptr& bs ) {
    if (!m_accept_block) {
        return;
    }

    on_block(bs);
}

void pubsub_plugin_impl::_process_irreversible_block(const chain::block_state_ptr& bs) {

    const auto block_id = bs->block->id();
    const auto block_id_str = block_id.str();
    const auto block_num = bs->block->block_num();
    m_log->lib = block_num;

    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
         std::chrono::microseconds{fc::time_point::now().time_since_epoch().count()});

    if( m_store_transactions ) {
        bool transactions_in_block = false;
        // fill block information
        m_log->latest_block_num = block_num;
        block_result_ptr br = std::make_shared<block_result>();
        // const auto block_id = block_id.str();
        const auto prev_block_id = bs->block->previous.str();
        const auto timestamp = std::chrono::milliseconds{
                                std::chrono::seconds{bs->block->timestamp.operator fc::time_point().sec_since_epoch()}}.count();
        const auto transaction_merkle_root = bs->block->transaction_mroot.str();
        const auto transaction_count = bs->block->transactions.size(); 
        const auto producer = bs->block->producer.to_string();

        br->block_num = block_num;
        br->block_id = block_id_str;
        br->prev_block_id = prev_block_id;
        br->timestamp = timestamp;
        br->transaction_merkle_root = transaction_merkle_root;
        br->transaction_count = transaction_count;
        br->producer = producer;

        for( const auto& receipt : bs->block->transactions ) {
            string trx_id_str;
            std::vector<pubsub_message::transfer_action> transfer_actions;
            if( receipt.trx.contains<packed_transaction>() ) {
                const auto& pt = receipt.trx.get<packed_transaction>();
                // get id via get_raw_transaction() as packed_transaction.id() mutates internal transaction state
                const auto& raw = pt.get_raw_transaction();
                const auto& trx = fc::raw::unpack<transaction>( raw );
                if( !filter_include( trx ) ) continue;

                const auto& id = trx.id();
                trx_id_str = id.str();
                // extract eosio.token:transfer
                auto mtrx = std::make_shared<chain::transaction_metadata>(pt);
                parse_transfer_actions(mtrx, transfer_actions);
            } else {
                const auto& id = receipt.trx.get<transaction_id_type>();
                trx_id_str = id.str();
                if( m_unexpected_txid++ % 1000 == 0 ) {
                    elog("#### unexpected receipt in trx: ${txid} total: ${errs}", ("txid", trx_id_str)("errs", m_unexpected_txid)); // FIXME: 
                }
            }

            const auto cpu_usage_us = receipt.cpu_usage_us;
            const auto net_usage_words = receipt.net_usage_words;

            br->transactions.emplace_back(transaction_result{
                trx_id_str,
                receipt.status,
                receipt.cpu_usage_us,
                receipt.net_usage_words,
                std::move(transfer_actions)
            });

            transactions_in_block = true;
        }

        if( transactions_in_block ) {
            try {
                #ifdef TEST_ENV
                const std::string &block_str = fc::json::to_pretty_string(*br);
                idump((block_str));
                #else
                const std::string &block_str = fc::json::to_string(*br);
                message_ptr block_msg = std::make_shared<std::string>(block_str);
                m_log->queue_size = m_applied_message_consumer->push(block_msg);
                #endif
                m_log->count++;
                m_log->timestamp = fc::string(fc::time_point::now());
                
            } catch( ... ) {
                handle_pubsub_exception( "push irreversible transaction", __LINE__ );
            }
        }
    }
}

void pubsub_plugin_impl::on_block(const chain::block_state_ptr& b)
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

void pubsub_plugin_impl::push_block(const chain::signed_block_ptr& block) 
{
    if (!m_store_transactions) {
        return;
    }

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

    bool transactions_in_block = false; 
    for( const auto& receipt : block->transactions ) {
        string trx_id_str;
        std::vector<pubsub_message::transfer_action> transfer_actions;
        if( receipt.trx.contains<packed_transaction>() ) {
            const auto& pt = receipt.trx.get<packed_transaction>();
            // get id via get_raw_transaction() as packed_transaction.id() mutates internal transaction state
            const auto& raw = pt.get_raw_transaction();
            const auto& trx = fc::raw::unpack<transaction>( raw );
            if( !filter_include( trx ) ) continue;

            const auto& id = trx.id();
            trx_id_str = id.str();
            // extract eosio.token:transfer
            auto mtrx = std::make_shared<chain::transaction_metadata>(pt);
            parse_transfer_actions(mtrx, transfer_actions);
        } else {
            const auto& id = receipt.trx.get<transaction_id_type>();
            trx_id_str = id.str();
            if( m_unexpected_txid++ % 1000 == 0 ) {
                elog("#### unexpected receipt in trx: ${txid} total: ${errs}", ("txid", trx_id_str)("errs", m_unexpected_txid)); // FIXME: 
            }
        }

        const auto cpu_usage_us = receipt.cpu_usage_us;
        const auto net_usage_words = receipt.net_usage_words;

        br->transactions.emplace_back(transaction_result{
            trx_id_str,
            receipt.status,
            receipt.cpu_usage_us,
            receipt.net_usage_words,
            std::move(transfer_actions)
        });

        transactions_in_block = true;
    }

    if( transactions_in_block ) {
        try {
            #ifdef TEST_ENV
            const std::string &block_str = fc::json::to_pretty_string(*br);
            idump((block_str));
            #else
            const std::string &block_str = fc::json::to_string(*br);
            message_ptr block_msg = std::make_shared<std::string>(block_str);
            m_log->queue_size = m_applied_message_consumer->push(block_msg);
            #endif
            m_log->count++;
            m_log->timestamp = fc::string(fc::time_point::now());
            
        } catch( ... ) {
            handle_pubsub_exception( "push irreversible transaction", __LINE__ );
        }
    }
}

void pubsub_plugin_impl::parse_transfer_actions(const chain::transaction_metadata_ptr& tm, std::vector<pubsub_message::transfer_action> &results)
{
    string trx_id_str = tm->id.str();
    try {
        for (const auto &act: tm->trx.actions) {
            if (filter_include( act.account, act.name, act.authorization)) {
                try {
                    auto transfer = fc::raw::unpack<pubsub_message::transfer_args>(act.data);
                    results.emplace_back(transfer_action{
                        act.account,
                        act.name,
                        transfer.from,
                        transfer.to,
                        transfer.quantity,
                        transfer.memo
                    });
                } catch (...) {
                    // not transfer action
                    if( m_unpack_failed++ % 1000 == 0 ) {
                        elog("#### failed to unpack transfer action in ${txid} total: ${errs}", ("txid", trx_id_str)("errs", m_unpack_failed)); // FIXME: 
                    }
                }
                
            }
        }

        for (const auto &act: tm->trx.context_free_actions) {
            if (filter_include( act.account, act.name, act.authorization)) {
                try {
                    auto transfer = fc::raw::unpack<pubsub_message::transfer_args>(act.data);
                    results.emplace_back(transfer_action{
                        act.account,
                        act.name,
                        transfer.from,
                        transfer.to,
                        transfer.quantity,
                        transfer.memo
                    });
                } catch (...) {
                    // not transfer action
                    if( m_unpack_failed++ % 1000 == 0 ) {
                        elog("#### failed to unpack transfer action in ${txid} total: ${errs}", ("txid", trx_id_str)("errs", m_unpack_failed)); // FIXME: 
                    }
                }
                
            }
        }
    } catch (...) {
        elog("failed to parse transfer action for txid:${txid}", ("txid", trx_id_str));
    }
}

void pubsub_plugin_impl::on_transaction(const chain::transaction_trace_ptr& trace) {
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
            result->actions.emplace_back( 
                ordered_action_result{
                    at.receipt.global_sequence,
                    account_action_seq,
                    chain.pending_block_state()->block_num, 
                    chain.pending_block_time(),
                    chain.to_variant_with_abi(at, abi_serializer_max_time),
                    0
                }
            );

        }
    }

    if (result->actions.size() > 0) {
        #ifdef TEST_ENV
        std::string tx_str = fc::json::to_pretty_string(*result);
        #else
        std::string tx_str = fc::json::to_string(*result);
        #endif
        message_ptr tx_msg = std::make_shared<std::string>(tx_str);
        m_log->latest_tx_block_num = block_num;
        m_log->queue_size = m_applied_message_consumer->push(tx_msg);
        m_log->count++;
    }
}

pubsub_plugin_impl::pubsub_plugin_impl()
{
    m_chain_plug = app().find_plugin<chain_plugin>();
    FC_ASSERT(m_chain_plug);

    m_activated = false;
    m_block_margin = 0;
    m_block_offset = 0;
    m_unexpected_txid = 0;
    m_unpack_failed = 0;
    
    m_log = std::make_shared<pubsub_runtime::pubsub_log>();
    FC_ASSERT(m_log);
    m_log->tag = "plugin";
    m_log->lib = 0;
    m_log->count = 0;
    m_log->latest_block_num = 0;
    m_log->latest_tx_block_num = 0;
    m_log->queue_size = 0;
}

pubsub_plugin_impl::~pubsub_plugin_impl() {
    if (!m_startup) {
        try {
            ilog( "pubsub_plugin shutdown in process please be patient this can take a few minutes" );
            // done = true;
        } catch( std::exception& e ) {
            elog( "Exception on pubsub_plugin shutdown of consume thread: ${e}", ("e", e.what()));
        }
    }
}

void pubsub_plugin_impl::wipe() {
    m_be->wipe();
    ilog("pubsub wipe");
}

void pubsub_plugin_impl::init() {
    ilog("init pubsub");
    m_applied_message_consumer = std::make_unique<consumer<pubsub_message::message_ptr>>(std::make_unique<applied_message>(m_be));

    m_startup = false;
}


// pubsub plugins
pubsub_plugin::pubsub_plugin()
:my(new pubsub_plugin_impl)
{
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
            (PUBSUB_ACCEPT_BLOCK, bpo::value<bool>()->default_value(false),
            "Fetch LIB on accepting blocks")
            (PUBSUB_OUTPUT_INLINE_ACTIONS, bpo::value<bool>()->default_value(true),
            "Output inline actions")
            (PUBSUB_BLOCK_START, bpo::value<uint32_t>()->default_value(0),
            "If specified then only abi data pushed to pubsub until specified block is reached.")
            (PUBSUB_UPDATE_VIA_BLOCK_NUM, bpo::value<bool>()->default_value(false),
            "Update blocks/block_state with latest via block number so that duplicates are overwritten.")
            (PUBSUB_STORE_BLOCKS, bpo::value<bool>()->default_value(true),
                "Enables storing blocks in pubsub.")
            (PUBSUB_STORE_BLOCK_STATES, bpo::value<bool>()->default_value(true),
                "Enables storing block state in pubsub.")
            (PUBSUB_STORE_TRANSACTIONS, bpo::value<bool>()->default_value(true),
                "Enables storing transactions in pubsub.")
            (PUBSUB_STORE_TRANSACTION_TRACES, bpo::value<bool>()->default_value(true),
                "Enables storing transaction traces in pubsub.")
            (PUBSUB_STORE_ACTION_TRACES, bpo::value<bool>()->default_value(true),
                "Enables storing action traces in pubsub.")
            (PUBSUB_FILTER_ON, bpo::value<vector<string>>()->composing(),
                "Track actions which match receiver:action:actor. Receiver, Action, & Actor may be blank to include all. i.e. eosio:: or :transfer:  Use * or leave unspecified to include all.")
            (PUBSUB_FILTER_OUT, bpo::value<vector<string>>()->composing(),
                "Do not track actions which match receiver:action:actor. Receiver, Action, & Actor may be blank to exclude all.")
            ;
}

void pubsub_plugin::plugin_initialize(const variables_map& options)
{
    try {
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

        my->m_block_margin = block_margin;
        my->m_block_offset = block_offset;

        my->m_be = std::make_shared<backend>(uri_str, topic_str, topic_partition);

        if (options.at("replay-blockchain").as<bool>() ||
            options.at("hard-replay-blockchain").as<bool>() ||
            options.at("delete-all-blocks").as<bool>()) {
            ilog("Resync requested: wiping backend");
            
            my->wipe_data_on_startup = true;
        }

        // my->abi_serializer_max_time = app().get_plugin<chain_plugin>().get_abi_serializer_max_time();

        if( options.count(PUBSUB_ACCEPT_BLOCK)) {
            my->m_accept_block = options.at( PUBSUB_ACCEPT_BLOCK ).as<bool>();
        }

        if( options.count(PUBSUB_OUTPUT_INLINE_ACTIONS)) {
            my->m_output_inline_actions = options.at( PUBSUB_OUTPUT_INLINE_ACTIONS ).as<bool>();
        }

        if( options.count( PUBSUB_BLOCK_START )) {
            my->m_start_block_num = options.at( PUBSUB_BLOCK_START ).as<uint32_t>();
        }
        if( options.count( PUBSUB_STORE_BLOCKS )) {
            my->m_store_blocks = options.at( PUBSUB_STORE_BLOCKS ).as<bool>();
        }
        if( options.count( PUBSUB_STORE_BLOCK_STATES )) {
            my->m_store_block_states = options.at( PUBSUB_STORE_BLOCK_STATES ).as<bool>();
        }
        if( options.count( PUBSUB_STORE_TRANSACTIONS )) {
            my->m_store_transactions = options.at( PUBSUB_STORE_TRANSACTIONS ).as<bool>();
        }
        if( options.count( PUBSUB_STORE_TRANSACTION_TRACES )) {
            my->m_store_transaction_traces = options.at( PUBSUB_STORE_TRANSACTION_TRACES ).as<bool>();
        }
        if( options.count( PUBSUB_STORE_ACTION_TRACES )) {
            my->m_store_action_traces = options.at( PUBSUB_STORE_ACTION_TRACES ).as<bool>();
        }
        if( options.count( PUBSUB_FILTER_ON )) {
            auto fo = options.at( PUBSUB_FILTER_ON ).as<vector<string>>();
            my->m_filter_on_star = false;
            for( auto& s : fo ) {
               if( s == "*" ) {
                  my->m_filter_on_star = true;
                  break;
               }
               std::vector<std::string> v;
               boost::split( v, s, boost::is_any_of( ":" ));
               EOS_ASSERT( v.size() == 3, fc::invalid_arg_exception, "Invalid value ${s} for --pubsub-filter-on", ("s", s));
               filter_entry fe{v[0], v[1], v[2]};
               my->m_filter_on.insert( fe );
            }
        } else {
            my->m_filter_on_star = true;
        }
        if( options.count( PUBSUB_FILTER_OUT )) {
            auto fo = options.at( PUBSUB_FILTER_OUT ).as<vector<string>>();
            for( auto& s : fo ) {
               std::vector<std::string> v;
               boost::split( v, s, boost::is_any_of( ":" ));
               EOS_ASSERT( v.size() == 3, fc::invalid_arg_exception, "Invalid value ${s} for --pubsub-filter-out", ("s", s));
               filter_entry fe{v[0], v[1], v[2]};
               my->m_filter_out.insert( fe );
            }
        }
        if( options.count( "producer-name") ) {
            wlog( "pubsub plugin not recommended on producer node" );
            my->m_is_producer = true;
        }

        if( my->m_start_block_num == 0 ) {
            my->m_start_block_reached = true;
        }

        // hook up to signals on controller
        chain_plugin* chain_plug = app().find_plugin<chain_plugin>();
        EOS_ASSERT( chain_plug, chain::missing_chain_plugin_exception, "");
        auto& chain = chain_plug->chain();
        my->m_chain_id.emplace( chain.get_chain_id());

        my->accepted_block_connection.emplace( chain.accepted_block.connect( [&]( const chain::block_state_ptr& bs ) {
            my->accepted_block( bs );
        } ));
        my->irreversible_block_connection.emplace(
               chain.irreversible_block.connect( [&]( const chain::block_state_ptr& bs ) {
                  my->applied_irreversible_block( bs );
               } ));
        my->accepted_transaction_connection.emplace(
               chain.accepted_transaction.connect( [&]( const chain::transaction_metadata_ptr& t ) {
                  my->accepted_transaction( t );
               } ));
        my->applied_transaction_connection.emplace(
               chain.applied_transaction.connect( [&]( const chain::transaction_trace_ptr& t ) {
                  my->applied_transaction( t );
               } ));

        if( my->wipe_data_on_startup ) {
            my->wipe();
        }
        my->init();
    } FC_LOG_AND_RETHROW()
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

    ret.plugin = *my->m_log;
    ret.kafka = *my->m_be->m_log;

    return ret;
}

void pubsub_plugin::plugin_shutdown()
{
    ilog("pubsub_plugin shutdown ...");
    my->accepted_block_connection.reset();
    my->irreversible_block_connection.reset();
    my->accepted_transaction_connection.reset();
    my->applied_transaction_connection.reset();
    my->m_be = nullptr;
    my->m_applied_message_consumer = nullptr;

    my.reset();
    ilog("pubsub_plugin shutdown ok");
}

#undef INVOKE_R_V
#undef CALL

} // namespace eosio

