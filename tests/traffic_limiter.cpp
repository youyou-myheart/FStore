#include "traffic_limiter.hpp"

#include <cmath>
#include <future>
#include <iostream>
#include <numeric>
#include <stdexcept>

using namespace std::chrono_literals;
using chunkstore::TrafficLane;
using chunkstore::TrafficLimiter;

void check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

int main() {
    constexpr auto rate = 16 * 1024 * 1024;
    TrafficLimiter limiter(rate);
    std::stop_source cancel;
    // Use different request sizes so request-count scheduling cannot pass.
    const std::array<std::uint64_t, 5> sizes{16384, 1ULL << 32, 1 << 20, 8 << 20, 1 << 19};
    for (unsigned lane = 0; lane < sizes.size(); ++lane) {
        const auto count = lane == 0 ? 4096u : (lane == 1 ? 1u : 64u);
        for (unsigned n = 0; n < count; ++n) {
            limiter.enqueue(static_cast<TrafficLane>(lane), sizes[lane], [](bool) {}, cancel.get_token());
        }
    }
    const auto before = limiter.stats();
    const auto start = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(1500ms);
    const auto after = limiter.stats();
    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    const std::array<double, 5> shares{.8, .1, .1*2/7, .1*4/7, .1/7};
    std::array<std::uint64_t, 5> charged{};
    for (unsigned i = 0; i < 5; ++i) {
        charged[i] = after.granted[i] - before.granted[i];
        check(before.pending[i] && after.pending[i], "load was not continuously backlogged");
    }
    const auto total = std::accumulate(charged.begin(), charged.end(), std::uint64_t{});
    check(total <= rate * elapsed + 131072, "aggregate cap exceeded");
    check(total > rate * elapsed * .7, "excessive idle capacity");
    for (unsigned i = 0; i < shares.size(); ++i) {
        check(std::abs(charged[i] - shares[i] * total) < 3 * 65536, "incorrect byte share");
        std::cout << 100.0 * charged[i] / total << " ";
    }
    std::cout << "% (online, trash, normal, high, low)\n";
    cancel.request_stop();

    TrafficLimiter solo(rate);
    const auto single_start = std::chrono::steady_clock::now();
    check(solo.acquire(TrafficLane::CopyLow, rate / 4), "low priority acquire failed");
    const auto single_elapsed = std::chrono::steady_clock::now() - single_start;
    check(single_elapsed > 220ms && single_elapsed < 650ms, "idle-share borrowing or cap failed");
    using Clock = std::chrono::steady_clock;
    std::promise<bool> expired;
    auto expiry = expired.get_future();
    solo.enqueue(TrafficLane::Online, rate * 2, [](bool) {});
    solo.enqueue(TrafficLane::Online, 4096, [&](bool ok) { expired.set_value(ok); },
                 {}, Clock::now() + 50ms);
    check(expiry.wait_for(500ms) == std::future_status::ready && !expiry.get(),
          "queued deadline blocked behind a large request");
    std::stop_source stop;
    auto pending = std::async(std::launch::async, [&] {
        return solo.acquire(TrafficLane::Trash, 1ULL << 40, stop.get_token());
    });
    stop.request_stop();
    check(pending.wait_for(1s) == std::future_status::ready && !pending.get(), "cleanup cancellation stalled");
}
