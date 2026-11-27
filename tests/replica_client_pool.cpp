#include "replica_client_pool.hpp"
#include "unit.hpp"

#include <atomic>
#include <barrier>
#include <chrono>
#include <future>

using unit::check;
namespace {
struct Client { std::vector<std::string> endpoints; };
using Pool = chunkstore::detail::ReplicaClientPool<Client>;
const auto make = [](const auto& endpoints) { return std::make_shared<Client>(Client{endpoints}); };
std::vector<std::string> endpoints(unsigned id) { return {"127.0.0.1:" + std::to_string(10000 + id)}; }

void reuse_and_keys() {
    Pool pool;
    const std::vector<std::string> ordered{"127.0.0.1:1", "[::1]:2"};
    auto first = pool.get(ordered, make);
    check(pool.get(ordered, make) == first, "identical endpoint order did not reuse client");
    auto reversed = pool.get({ordered[1], ordered[0]}, make);
    check(reversed != first && reversed->endpoints.front() == ordered[1], "preferred read replica was reordered");
    check(pool.get({"a", "bc"}, make) != pool.get({"ab", "c"}, make), "endpoint list encoding collides");
    const auto stats = pool.stats();
    check(stats.hits == 1 && stats.misses == 4 && stats.cached_clients == 4, "incorrect cache counters");
}

void bounded_fifo() {
    Pool pool;
    auto first = pool.get(endpoints(0), make);
    std::weak_ptr<Client> second;
    for (unsigned i = 1; i < 256; ++i) {
        auto client = pool.get(endpoints(i), make);
        if (i == 1) second = client;
    }
    check(!second.expired(), "resident client was not retained by the cache");
    // A hit must not turn the documented FIFO policy into LRU.
    check(pool.get(endpoints(0), make) == first, "cache lost a resident client");
    pool.get(endpoints(256), make);
    check(!second.expired(), "eviction removed the wrong client");
    auto replacement = pool.get(endpoints(0), make);
    check(replacement != first && first->endpoints == endpoints(0), "eviction invalidated a live handle or failed to evict");
    check(second.expired(), "evicted client leaked without a live handle");
    check(pool.stats().cached_clients == 256, "cache exceeded capacity");
}

void failures_do_not_poison_or_evict() {
    Pool pool;
    std::weak_ptr<Client> first = pool.get(endpoints(0), make);
    for (unsigned i = 1; i < 256; ++i) pool.get(endpoints(i), make);
    const auto failure = [](const auto&) -> std::shared_ptr<Client> { throw std::runtime_error("construction failed"); };
    check(std::string(unit::expect<std::runtime_error>([&] { pool.get(endpoints(300), failure); }).what()) == "construction failed",
          "construction error was lost");
    unit::expect<std::logic_error>([&] {
        pool.get(endpoints(300), [](const auto&) { return std::shared_ptr<Client>{}; });
    });
    check(!first.expired() && pool.stats().cached_clients == 256, "failed miss evicted a healthy cached client");
    auto retry = pool.get(endpoints(300), make);
    check(retry && first.expired(), "failed creation poisoned a retry or changed FIFO bookkeeping");
    check(pool.get(endpoints(300), failure) == retry, "cache hit invoked the factory");
    check(pool.stats().misses == 259, "failed cache queries missing from statistics");
}

void independent_pools_and_lifetime() {
    std::shared_ptr<Client> retained;
    std::weak_ptr<Client> weak;
    {
        Pool first, second;
        retained = first.get(endpoints(1), make); weak = retained;
        check(second.get(endpoints(1), make) != retained, "separate client pools share configuration unexpectedly");
    }
    check(!weak.expired() && retained->endpoints == endpoints(1), "pool destruction invalidated a live handle");
    retained.reset();
    check(weak.expired(), "client outlived both cache and handles");
}

void concurrent_same_key() {
    Pool pool;
    std::barrier constructing(8);
    std::atomic<unsigned> calls{0};
    std::vector<std::future<std::shared_ptr<Client>>> work;
    for (unsigned i = 0; i < 8; ++i) work.push_back(std::async(std::launch::async, [&] {
        return pool.get(endpoints(5), [&](const auto& targets) {
            ++calls;
            auto client = make(targets);
            constructing.arrive_and_wait(); // All eight misses race publication.
            return client;
        });
    }));
    auto canonical = work.front().get();
    for (std::size_t i = 1; i < work.size(); ++i) check(work[i].get() == canonical, "racing misses returned different clients");
    const auto stats = pool.stats();
    check(calls == 8 && stats.misses == 8 && stats.cached_clients == 1, "racing misses inserted duplicate entries");
    check(pool.get(endpoints(5), make) == canonical, "published race winner was not cached");
}

void slow_factory_does_not_block_hits() {
    Pool pool;
    auto cached = pool.get(endpoints(1), make);
    std::promise<void> entered, resume;
    auto resume_future = resume.get_future().share();
    auto miss = std::async(std::launch::async, [&] {
        return pool.get(endpoints(2), [&](const auto& targets) {
            entered.set_value(); resume_future.wait(); return make(targets);
        });
    });
    entered.get_future().wait();
    auto hit = std::async(std::launch::async, [&] { return pool.get(endpoints(1), make); });
    const bool ready = hit.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
    resume.set_value(); // Always release, even when the assertion will fail.
    check(hit.get() == cached && miss.get(), "concurrent hit or construction failed");
    check(ready, "client construction held the cache lock and blocked a hit");
}

void concurrent_eviction() {
    Pool pool;
    std::vector<std::future<void>> work;
    for (unsigned worker = 0; worker < 8; ++worker) work.push_back(std::async(std::launch::async, [&, worker] {
        for (unsigned i = 0; i < 512; ++i) {
            const auto key = endpoints(worker * 1000 + i);
            auto client = pool.get(key, make);
            check(client->endpoints == key, "concurrent eviction returned the wrong client");
        }
    }));
    for (auto& worker : work) worker.get();
    const auto stats = pool.stats();
    check(stats.cached_clients == 256 && stats.misses == 4096 && stats.hits == 0, "concurrent eviction broke cache bounds or counters");
}
}
int main() {
    return unit::run({{"reuse_and_keys", reuse_and_keys}, {"bounded_fifo", bounded_fifo},
        {"failure_retry", failures_do_not_poison_or_evict}, {"handle_lifetime", independent_pools_and_lifetime},
        {"concurrent_same_key", concurrent_same_key}, {"nonblocking_hit", slow_factory_does_not_block_hits},
        {"concurrent_eviction", concurrent_eviction}});
}
