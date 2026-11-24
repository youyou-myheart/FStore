#pragma once

#include "mpsc_inbox.hpp"

#include <array>
#include <chrono>
#include <atomic>
#include <cstdint>
#include <memory>
#include <functional>
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

    // Completion may run inline on admission and must not perform blocking I/O.
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
    using Clock = std::chrono::steady_clock;
    struct Request {
        TrafficLane lane;
        std::uint64_t remaining;
        std::function<void(bool)> completion;
        std::stop_token cancel;
        Clock::time_point deadline;
        Request* next = nullptr;
    };
    struct Queue {
        Request* head = nullptr;
        Request* tail = nullptr;
    };
    void run(std::stop_token stop);
    void receive();
    void finish(Request* request, bool granted);
    void prune(Clock::time_point now);
    Clock::time_point charge(std::uint64_t bytes, Clock::time_point now);
    bool try_immediate(unsigned lane, std::uint64_t bytes, Clock::time_point now);
    unsigned select() const;
    const std::uint64_t rate_;
    const std::int64_t burst_ns_;
    std::atomic<std::int64_t> finish_ns_;
    MpscInbox<Request> incoming_;
    std::atomic<unsigned> backlog_{0};
    std::atomic<bool> cancellation_{false};
    // Only the dispatcher touches the FIFOs, deadlines and virtual service.
    std::array<Queue, 5> queues_{};
    Clock::time_point expiry_ = Clock::time_point::max();
    // Virtual service = charged bytes / class weight, scaled to whole integers.
    std::array<std::uint64_t, 3> service_{};
    std::array<std::uint64_t, 3> copy_service_{};
    std::uint64_t floor_ = 0;
    std::uint64_t copy_floor_ = 0;
    std::array<std::atomic<std::uint64_t>, 5> granted_{};
    std::array<std::atomic<std::uint64_t>, 5> pending_{};
    std::jthread dispatcher_;
};
}
