#include "applied_message.h"

namespace eosio {

applied_message::applied_message(std::shared_ptr<backend> be):
    m_be(be)
{
}

void applied_message::consume(const std::vector<pubsub_message::message_ptr>& results)
{
    for (const auto& result : results) {
        m_be->publish(*result);
    }
}

} // namespace
