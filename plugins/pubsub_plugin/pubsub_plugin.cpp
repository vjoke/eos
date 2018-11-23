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

#define TEST_ENV

namespace {
const char* PUBSUB_URI_OPTION = "pubsub-uri";
const char* PUBSUB_TOPIC_OPTION = "pubsub-topic";
const char* PUBSUB_PARTITION_OPTION = "pubsub-partition";
const char* PUBSUB_BLOCK_MARGIN_OPTION = "pubsub-block-margin";
const char* PUBSUB_BLOCK_OFFSET_OPTION = "pubsub-block-offset";
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
                          bool& write_ttrace );
    bool filter_include( const account_name& receiver, const action_name& act_name,
                        const vector<chain::permission_level>& authorization ) const;
    bool filter_include( const transaction& trx ) const;

    void init();
    void wipe();

    bool configured{false};
    bool wipe_data_on_startup{false};
    uint32_t start_block_num = 0;
    std::atomic_bool start_block_reached{false};

    bool is_producer = false;
    bool filter_on_star = true;
    std::set<filter_entry> filter_on;
    std::set<filter_entry> filter_out;
    bool update_blocks_via_block_num = false;
    bool store_blocks = true;
    bool store_block_states = true;
    bool store_transactions = true;
    bool store_transaction_traces = true;
    bool store_action_traces = true;
    std::atomic_bool startup{true};
    fc::optional<chain::chain_id_type> chain_id;
    fc::microseconds abi_serializer_max_time;

    int64_t m_block_offset;
    int64_t m_block_margin;
    
    bool m_activated = false;
    std::unique_ptr<consumer<pubsub_message::message_ptr>> m_applied_message_consumer;

    chain_plugin*  m_chain_plug;
    std::shared_ptr<backend> m_be;

    pubsub_runtime::pubsub_log_ptr m_log;

    void parse_transfer_actions(const chain::transaction_metadata_ptr& tm, std::vector<pubsub_message::transfer_args> &results);

private:
    void on_transaction(const chain::transaction_trace_ptr& t);
    void on_block(const chain::block_state_ptr& b);
    void push_block(const chain::signed_block_ptr& block);
};

bool pubsub_plugin_impl::filter_include( const account_name& receiver, const action_name& act_name,
                                           const vector<chain::permission_level>& authorization ) const
{
   bool include = false;
   if( filter_on_star ) {
      include = true;
   } else {
      auto itr = std::find_if( filter_on.cbegin(), filter_on.cend(), [&receiver, &act_name]( const auto& filter ) {
         return filter.match( receiver, act_name, 0 );
      } );
      if( itr != filter_on.cend() ) {
         include = true;
      } else {
         for( const auto& a : authorization ) {
            auto itr = std::find_if( filter_on.cbegin(), filter_on.cend(), [&receiver, &act_name, &a]( const auto& filter ) {
               return filter.match( receiver, act_name, a.actor );
            } );
            if( itr != filter_on.cend() ) {
               include = true;
               break;
            }
         }
      }
   }

   if( !include ) { return false; }
   if( filter_out.empty() ) { return true; }

   auto itr = std::find_if( filter_out.cbegin(), filter_out.cend(), [&receiver, &act_name]( const auto& filter ) {
      return filter.match( receiver, act_name, 0 );
   } );
   if( itr != filter_out.cend() ) { return false; }

   for( const auto& a : authorization ) {
      auto itr = std::find_if( filter_out.cbegin(), filter_out.cend(), [&receiver, &act_name, &a]( const auto& filter ) {
         return filter.match( receiver, act_name, a.actor );
      } );
      if( itr != filter_out.cend() ) { return false; }
   }

   return true;
}

bool pubsub_plugin_impl::filter_include( const transaction& trx ) const
{
   if( !filter_on_star || !filter_out.empty() ) {
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
      if( store_transactions ) {
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
      if( !is_producer && !t->producer_block_id.valid() )
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
        if( store_blocks || store_block_states || store_transactions ) {
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
        if( !start_block_reached ) {
            if( bs->block_num >= start_block_num ) {
                start_block_reached = true;
            }
        }
        if( store_blocks || store_block_states ) {
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
        if( start_block_reached ) {
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
        if( start_block_reached ) {
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
        if( start_block_reached ) {
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
                                        bool& write_ttrace )
{
    auto& chain = m_chain_plug->chain();
    const auto abi_serializer_max_time = m_chain_plug->get_abi_serializer_max_time();

    // if( executed && atrace.receipt.receiver == chain::config::system_account_name ) {
    //     update_account( atrace, atrace.act );
    // }

    bool added = false;
    const bool in_filter = (store_action_traces || store_transaction_traces) && start_block_reached &&
                        filter_include( atrace.receipt.receiver, atrace.act.name, atrace.act.authorization );
    write_ttrace |= in_filter;
    if( start_block_reached && store_action_traces && in_filter ) {
        actions.emplace_back( 
            ordered_action_result{
                atrace.receipt.global_sequence,
                0, // FIXME: 
                chain.pending_block_state()->block_num, 
                chain.pending_block_time(),
                chain.to_variant_with_abi(atrace, abi_serializer_max_time)
            }
        );

        // const chain::base_action_trace& base = atrace; // without inline action traces

        // auto v = to_variant_with_abi( base );
        // string json = fc::json::to_string( v );
        // try {
        //     const auto& value = bsoncxx::from_json( json );
        //     action_traces_doc.append( bsoncxx::builder::concatenate_doc{value.view()} );
        // } catch( bsoncxx::exception& ) {
        //     try {
        //         json = fc::prune_invalid_utf8( json );
        //         const auto& value = bsoncxx::from_json( json );
        //         action_traces_doc.append( bsoncxx::builder::concatenate_doc{value.view()} );
        //         action_traces_doc.append( kvp( "non-utf8-purged", b_bool{true} ) );
        //     } catch( bsoncxx::exception& e ) {
        //         elog( "Unable to convert action trace JSON to MongoDB JSON: ${e}", ("e", e.what()) );
        //         elog( "  JSON: ${j}", ("j", json) );
        //     }
        // }
        // if( t->receipt.valid() ) {
        //     action_traces_doc.append( kvp( "trx_status", std::string( t->receipt->status ) ) );
        // }
        // action_traces_doc.append( kvp( "createdAt", b_date{now} ) );

        added = true;
    }

    // TODO: split inline transactions 
    // for( const auto& iline_atrace : atrace.inline_traces ) {
    //     added |= add_action_trace( actions, iline_atrace, t, executed, now, write_ttrace );
    // }

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
            write_atraces |= add_action_trace( result->actions, atrace, t, executed, now, write_ttrace );
        } catch(...) {
            handle_pubsub_exception("add action traces", __LINE__);
        }
    }

    if( !start_block_reached ) return; //< add_action_trace calls update_account which must be called always

    // transaction trace insert

    // insert action_traces
    if( write_atraces ) {
        try {
            if (result->actions.size() > 0) {
                std::string tx_str = fc::json::to_pretty_string(*result);
                #ifdef TEST_ENV
                idump((tx_str));
                #else
                message_ptr tx_msg = std::make_shared<std::string>(tx_str);
                m_log->queue_size = m_applied_message_consumer->push(tx_msg);
                #endif
                m_log->latest_tx_block_num = block_num;
                m_log->count++;
                m_log->timestamp = fc::string(fc::time_point::now());
            }
            //  if( !bulk_action_traces.execute() ) {
            //     EOS_ASSERT( false, chain::mongo_db_insert_fail,
            //                 "Bulk action traces insert failed for transaction trace: ${id}", ("id", t->id) );
            //  }
        } catch( ... ) {
            handle_pubsub_exception( "publish action traces", __LINE__ );
        }
    }
}

void pubsub_plugin_impl::_process_accepted_block( const chain::block_state_ptr& bs ) {
    // TODO: 
}

void pubsub_plugin_impl::_process_irreversible_block(const chain::block_state_ptr& bs) {

    const auto block_id = bs->block->id();
    const auto block_id_str = block_id.str();
    const auto block_num = bs->block->block_num();
    m_log->lib = block_num;

    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
         std::chrono::microseconds{fc::time_point::now().time_since_epoch().count()});

    if( store_transactions ) {
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
            std::vector<pubsub_message::transfer_args> transfer_actions;
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
                elog("#### unexpected receipt in trx: ${txid}", ("txid", trx_id_str)); // FIXME: 
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
                const std::string &block_str = fc::json::to_pretty_string(*br);
                message_ptr block_msg = std::make_shared<std::string>(block_str);
                #ifdef TEST_ENV
                idump((block_str));
                #else
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
            elog("#### unexpected receipt in trx: ${txid}", ("txid", trx_id.str())); // FIXME: 
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

void pubsub_plugin_impl::parse_transfer_actions(const chain::transaction_metadata_ptr& tm, std::vector<pubsub_message::transfer_args> &results)
{
    try {
        for (const auto &act: tm->trx.actions) {
            if (act.account == N(eosio.token) && act.name == N(transfer)) {
                // auto transfer = act.data_as<pubsub_message::transfer_args>();
                auto transfer = fc::raw::unpack<pubsub_message::transfer_args>(act.data);
                results.emplace_back(std::move(transfer));
            }
        }

        for (const auto &act: tm->trx.context_free_actions) {
            if (act.account == N(eosio.token) && act.name == N(transfer)) {
                // auto transfer = act.data_as<pubsub_message::transfer_args>();
                auto transfer = fc::raw::unpack<pubsub_message::transfer_args>(act.data);
                results.emplace_back(std::move(transfer));
            }
        }
    } catch (...) {
        elog("failed to parse transfer action for txid:${txid}", ("txid", tm->id.str()));
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
                    chain.to_variant_with_abi(at, abi_serializer_max_time)
                }
            );

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

pubsub_plugin_impl::pubsub_plugin_impl()
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

pubsub_plugin_impl::~pubsub_plugin_impl() {
    if (!startup) {
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

    startup = false;
}


// pubsub plugins
pubsub_plugin::pubsub_plugin()
:my(new pubsub_plugin_impl)
{
    // m_chain_plug = app().find_plugin<chain_plugin>();
    // FC_ASSERT(m_chain_plug);

    // m_activated = false;
    // m_block_margin = 0;
    // m_block_offset = 0;
    
    // m_log = std::make_shared<pubsub_runtime::pubsub_log>();
    // FC_ASSERT(m_log);
    // m_log->tag = "plugin";
    // m_log->lib = 0;
    // m_log->count = 0;
    // m_log->latest_block_num = 0;
    // m_log->latest_tx_block_num = 0;
    // m_log->queue_size = 0;

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
            ("pubsub-block-start", bpo::value<uint32_t>()->default_value(0),
            "If specified then only abi data pushed to pubsub until specified block is reached.")
         
            ("pubsub-update-via-block-num", bpo::value<bool>()->default_value(false),
            "Update blocks/block_state with latest via block number so that duplicates are overwritten.")
            ("pubsub-store-blocks", bpo::value<bool>()->default_value(true),
                "Enables storing blocks in pubsub.")
            ("pubsub-store-block-states", bpo::value<bool>()->default_value(true),
                "Enables storing block state in pubsub.")
            ("pubsub-store-transactions", bpo::value<bool>()->default_value(true),
                "Enables storing transactions in pubsub.")
            ("pubsub-store-transaction-traces", bpo::value<bool>()->default_value(true),
                "Enables storing transaction traces in pubsub.")
            ("pubsub-store-action-traces", bpo::value<bool>()->default_value(true),
                "Enables storing action traces in pubsub.")
            ("pubsub-filter-on", bpo::value<vector<string>>()->composing(),
                "Track actions which match receiver:action:actor. Receiver, Action, & Actor may be blank to include all. i.e. eosio:: or :transfer:  Use * or leave unspecified to include all.")
            ("pubsub-filter-out", bpo::value<vector<string>>()->composing(),
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

        my->abi_serializer_max_time = app().get_plugin<chain_plugin>().get_abi_serializer_max_time();

        if( options.count( "pubsub-block-start" )) {
            my->start_block_num = options.at( "pubsub-block-start" ).as<uint32_t>();
        }
        if( options.count( "pubsub-store-blocks" )) {
            my->store_blocks = options.at( "pubsub-store-blocks" ).as<bool>();
        }
        if( options.count( "pubsub-store-block-states" )) {
            my->store_block_states = options.at( "pubsub-store-block-states" ).as<bool>();
        }
        if( options.count( "pubsub-store-transactions" )) {
            my->store_transactions = options.at( "pubsub-store-transactions" ).as<bool>();
        }
        if( options.count( "pubsub-store-transaction-traces" )) {
            my->store_transaction_traces = options.at( "pubsub-store-transaction-traces" ).as<bool>();
        }
        if( options.count( "pubsub-store-action-traces" )) {
            my->store_action_traces = options.at( "pubsub-store-action-traces" ).as<bool>();
        }
        if( options.count( "pubsub-filter-on" )) {
            auto fo = options.at( "pubsub-filter-on" ).as<vector<string>>();
            my->filter_on_star = false;
            for( auto& s : fo ) {
               if( s == "*" ) {
                  my->filter_on_star = true;
                  break;
               }
               std::vector<std::string> v;
               boost::split( v, s, boost::is_any_of( ":" ));
               EOS_ASSERT( v.size() == 3, fc::invalid_arg_exception, "Invalid value ${s} for --pubsub-filter-on", ("s", s));
               filter_entry fe{v[0], v[1], v[2]};
               my->filter_on.insert( fe );
            }
        } else {
            my->filter_on_star = true;
        }
        if( options.count( "pubsub-filter-out" )) {
            auto fo = options.at( "pubsub-filter-out" ).as<vector<string>>();
            for( auto& s : fo ) {
               std::vector<std::string> v;
               boost::split( v, s, boost::is_any_of( ":" ));
               EOS_ASSERT( v.size() == 3, fc::invalid_arg_exception, "Invalid value ${s} for --pubsub-filter-out", ("s", s));
               filter_entry fe{v[0], v[1], v[2]};
               my->filter_out.insert( fe );
            }
        }
        if( options.count( "producer-name") ) {
            wlog( "pubsub plugin not recommended on producer node" );
            my->is_producer = true;
        }

        if( my->start_block_num == 0 ) {
            my->start_block_reached = true;
        }

        // hook up to signals on controller
        chain_plugin* chain_plug = app().find_plugin<chain_plugin>();
        EOS_ASSERT( chain_plug, chain::missing_chain_plugin_exception, "");
        auto& chain = chain_plug->chain();
        my->chain_id.emplace( chain.get_chain_id());

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

