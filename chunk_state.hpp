#pragma once
#include <filesystem>
#include <string_view>

namespace chunkstore {
// The first close operation determines the state of this file version.
enum class State { Open, Frozen, Sealed };
constexpr bool is_closed(State state) { return state == State::Frozen || state == State::Sealed; }
constexpr std::string_view state_name(State state) {
    switch (state) {
    case State::Open: return "open";
    case State::Frozen: return "frozen";
    case State::Sealed: return "sealed";
    }
    return "invalid";
}
inline constexpr char close_reason_attribute[] = "user.chunkstore.close_reason";
// Call only for a .frozen file. Missing attributes mean legacy user Freeze.
State load_closed_state(int fd);
State load_closed_state(const std::filesystem::path& path);
void save_closed_state(int fd, State state);
}
