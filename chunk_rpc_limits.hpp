#pragma once

#include <cstddef>

namespace chunkstore {
inline constexpr std::size_t max_rpc_payload = 8 * 1024 * 1024;
inline constexpr unsigned max_gap_wait_ms = 60000;
}
