#pragma once
#include "fmaster_store.hpp"
#include <chrono>
#include <memory>
namespace chunkstore::master {
class RecoveryMonitor {
public:
    RecoveryMonitor(Store& store, std::chrono::milliseconds grace = std::chrono::minutes(10),
                    std::chrono::milliseconds interval = std::chrono::seconds(1),
                    std::chrono::milliseconds timeout = std::chrono::minutes(5));
    ~RecoveryMonitor();
private:
    struct Impl; std::unique_ptr<Impl> impl_;
};
}
