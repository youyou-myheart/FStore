#pragma once
#include "chunk.pb.h"
#include "chunk_state.hpp"
#include <stdexcept>

namespace chunkstore::detail {
inline rpc::v1::CloseReason close_reason(State state) {
    switch (state) {
    case State::Open: return rpc::v1::NOT_CLOSED;
    case State::Frozen: return rpc::v1::USER_FREEZE;
    case State::Sealed: return rpc::v1::PG_REPAIR;
    }
    throw std::invalid_argument("invalid chunk state");
}
template<class Message> void set_wire_state(Message& message, State state) {
    message.set_frozen(is_closed(state)); // Legacy field means immutable, not who closed it.
    message.set_close_reason(close_reason(state));
}
template<class Message> State wire_state(const Message& message) {
    if (!message.has_close_reason()) return message.frozen() ? State::Frozen : State::Open;
    State state;
    switch (message.close_reason()) {
    case rpc::v1::NOT_CLOSED: state = State::Open; break;
    case rpc::v1::USER_FREEZE: state = State::Frozen; break;
    case rpc::v1::PG_REPAIR: state = State::Sealed; break;
    default: throw std::invalid_argument("unknown chunk close reason");
    }
    if (message.frozen() != is_closed(state)) throw std::invalid_argument("inconsistent frozen flag and close reason");
    return state;
}
}
