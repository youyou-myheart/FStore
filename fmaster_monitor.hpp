#pragma once

#include "fmaster_store.hpp"

#include <chrono>
#include <memory>

namespace chunkstore::master {

// The store outlives the monitor. Shutdown waits for bounded in-flight probes.
class Monitor {
public:
    Monitor(Store& store, std::chrono::milliseconds interval, std::chrono::milliseconds rpc_timeout);
    ~Monitor();
    Monitor(const Monitor&) = delete;
    Monitor& operator=(const Monitor&) = delete;
    // Queue an immediate probe, reusing any request already in flight.
    std::uint32_t request_probe(const std::string& server_id = {});

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace chunkstore::master
