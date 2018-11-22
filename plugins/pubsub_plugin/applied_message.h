/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */

#pragma once

#include "consumer_core.h"

#include <memory>
#include <eosio/pubsub_plugin/pubsub_plugin.hpp>
#include "backend.h"

namespace eosio {

class applied_message : public consumer_core<pubsub_message::message_ptr>
{
public:
    applied_message(std::shared_ptr<backend> be);

    void consume(const std::vector<pubsub_message::message_ptr>& results) override;

private:
    std::shared_ptr<backend> m_be;
};

} // namespace

