#pragma once

#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace chunkstore::detail {
struct ReplicaPoolStats {
    std::uint64_t hits = 0;
    std::uint64_t misses = 0;
    std::size_t cached_clients = 0;
};

// Internal cache shared by SDK handles. The factory is called only on misses,
// outside the mutex; tests can use lightweight clients without any network.
template<class Client>
class ReplicaClientPool {
public:
    static constexpr std::size_t capacity = 256;

    template<class Factory>
    std::shared_ptr<Client> get(const std::vector<std::string>& endpoints, Factory&& factory) {
        // Replica order determines the preferred read replica. Length prefixes
        // keep endpoint lists distinct, including IPv6 endpoint strings.
        std::string key;
        for (const auto& endpoint : endpoints) key += std::to_string(endpoint.size()) + ":" + endpoint;
        {
            std::lock_guard lock(mutex_);
            if (const auto found = clients_.find(key); found != clients_.end()) {
                ++stats_.hits;
                return found->second;
            }
            ++stats_.misses;
        }
        auto client = factory(endpoints);
        if (!client) throw std::logic_error("replica client factory returned null");
        std::lock_guard lock(mutex_);
        if (const auto found = clients_.find(key); found != clients_.end()) return found->second;
        insertion_order_.push_back(key);
        try { clients_.emplace(key, client); }
        catch (...) { insertion_order_.pop_back(); throw; }
        if (clients_.size() > capacity) {
            clients_.erase(insertion_order_.front());
            insertion_order_.pop_front();
        }
        return client;
    }

    ReplicaPoolStats stats() const {
        std::lock_guard lock(mutex_);
        auto result = stats_;
        result.cached_clients = clients_.size();
        return result;
    }

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<Client>> clients_;
    std::deque<std::string> insertion_order_;
    ReplicaPoolStats stats_;
};
} // namespace chunkstore::detail
