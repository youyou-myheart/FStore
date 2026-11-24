#pragma once

#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <set>
#include <thread>

namespace chunkstore {

// The last three lanes are children of the replication class.
enum class TrafficLane : unsigned { Online, Trash, CopyNormal, CopyHigh, CopyLow };
constexpr unsigned lane_index(TrafficLane lane) { return static_cast<unsigned>(lane); }

class TrafficLimiter {
public:
    static constexpr std::uint64_t default_rate = 256ULL << 20;
    explicit TrafficLimiter(std::uint64_t bytes_per_second);
    ~TrafficLimiter();
    TrafficLimiter(const TrafficLimiter&) = delete;
    TrafficLimiter& operator=(const TrafficLimiter&) = delete;

    // Completion runs outside the scheduler lock and must not perform blocking I/O.
    // false indicates shutdown or cancellation.
    void enqueue(TrafficLane lane, std::uint64_t bytes, std::function<void(bool)> completion,
                 std::stop_token cancel = {},
                 std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::time_point::max());
    bool acquire(TrafficLane lane, std::uint64_t bytes, std::stop_token cancel = {});

    struct Stats {
        std::array<std::uint64_t, 5> granted{};
        std::array<std::uint64_t, 5> pending{};
        std::uint64_t bytes_per_second{};
    };
    Stats stats() const;
    std::uint64_t rate() const { return rate_; }

private:
    struct Request {
        std::uint64_t remaining;
        std::function<void(bool)> completion;
        std::stop_token cancel;
        std::chrono::steady_clock::time_point deadline;
    };
    void run(std::stop_token stop);
    unsigned select() const;
    const std::uint64_t rate_;
    mutable std::mutex mutex_;
    std::condition_variable_any changed_;
    std::array<std::deque<Request>, 5> queues_;
    std::multiset<std::chrono::steady_clock::time_point> deadlines_;
    // Virtual service = charged bytes / class weight, scaled to whole integers.
    std::array<std::uint64_t, 3> service_{};
    std::array<std::uint64_t, 3> copy_service_{};
    std::uint64_t floor_ = 0;
    std::uint64_t copy_floor_ = 0;
    std::array<std::uint64_t, 5> granted_{};
    std::jthread dispatcher_;
};
}
