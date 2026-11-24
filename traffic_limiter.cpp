#include "traffic_limiter.hpp"

#include <algorithm>
#include <future>
#include <stdexcept>

namespace chunkstore {
namespace {
using Clock = std::chrono::steady_clock;
using Ns = std::chrono::nanoseconds;
constexpr std::uint64_t quantum = 64 * 1024;
constexpr std::uint64_t minimum_cost = 4 * 1024;
constexpr std::array<unsigned, 3> class_stride{1, 8, 8};
constexpr std::array<unsigned, 3> copy_stride{2, 1, 4}; // normal, high, low

std::int64_t cost_ns(std::uint64_t bytes, std::uint64_t rate) {
    const auto scaled = bytes * 1'000'000'000ULL; // bytes <= quantum
    return scaled / rate + (scaled % rate != 0);
}
}

TrafficLimiter::TrafficLimiter(std::uint64_t rate)
    : rate_(rate), burst_ns_(rate ? cost_ns(quantum, rate) : 0),
      finish_ns_(std::chrono::duration_cast<Ns>(Clock::now().time_since_epoch()).count() - burst_ns_) {
    if (!rate) throw std::invalid_argument("traffic rate must be positive");
    dispatcher_ = std::jthread([this](std::stop_token stop) { run(stop); });
}

TrafficLimiter::~TrafficLimiter() = default;

// Both inline grants and queued quanta debit the same token bucket. Its virtual
// finish time can lag real time by at most one quantum when charging a request.
Clock::time_point TrafficLimiter::charge(std::uint64_t bytes, Clock::time_point now) {
    const auto ns = std::chrono::duration_cast<Ns>(now.time_since_epoch()).count();
    const auto cost = cost_ns(bytes, rate_);
    auto before = finish_ns_.load(std::memory_order_relaxed);
    for (;;) {
        const auto after = std::max(before, ns - burst_ns_) + cost;
        if (after > ns) return Clock::time_point(Ns(after));
        if (finish_ns_.compare_exchange_weak(before, after, std::memory_order_relaxed))
            return Clock::time_point::min();
    }
}

bool TrafficLimiter::try_immediate(unsigned lane, std::uint64_t bytes, Clock::time_point now) {
    if (bytes > quantum || backlog_.load(std::memory_order_acquire) != 0 ||
        charge(bytes, now) != Clock::time_point::min()) return false;
    granted_[lane].fetch_add(bytes, std::memory_order_relaxed);
    return true;
}

void TrafficLimiter::enqueue(TrafficLane lane, std::uint64_t bytes,
                             std::function<void(bool)> completion, std::stop_token cancel,
                             Clock::time_point deadline) {
    const auto i = lane_index(lane);
    if (i >= queues_.size()) throw std::out_of_range("invalid traffic lane");
    const auto now = Clock::now();
    if (cancel.stop_requested() || deadline <= now) {
        completion(false);
        return;
    }
    bytes = std::max(bytes, minimum_cost);
    if (try_immediate(i, bytes, now)) {
        completion(true);
        return;
    }
    auto* request = new Request{lane, bytes, std::move(completion), std::move(cancel), deadline};
    backlog_.fetch_add(1, std::memory_order_acq_rel);
    pending_[i].fetch_add(1, std::memory_order_relaxed);
    incoming_.push(request);
}

bool TrafficLimiter::acquire(TrafficLane lane, std::uint64_t bytes, std::stop_token cancel) {
    if (cancel.stop_requested()) return false;
    const auto i = lane_index(lane);
    if (i >= queues_.size()) throw std::out_of_range("invalid traffic lane");
    if (try_immediate(i, std::max(bytes, minimum_cost), Clock::now())) return true;
    auto promise = std::make_shared<std::promise<bool>>();
    auto result = promise->get_future();
    std::stop_callback notify_cancel(cancel, [this] {
        cancellation_.store(true, std::memory_order_release);
        incoming_.notify();
    });
    enqueue(lane, bytes, [promise](bool ok) { promise->set_value(ok); }, cancel);
    return result.get();
}

void TrafficLimiter::receive() {
    auto* first = incoming_.take();
    while (first) {
        auto* request = first;
        first = first->next;
        request->next = nullptr;
        if (request->cancel.stop_requested()) {
            finish(request, false);
            continue;
        }
        const auto i = lane_index(request->lane);
        auto& queue = queues_[i];
        if (queue.tail) {
            queue.tail->next = request;
        } else {
            queue.head = request;
            if (i >= 2) copy_service_[i - 2] = std::max(copy_floor_, copy_service_[i - 2]);
            auto& service = service_[std::min(i, 2u)];
            service = std::max(floor_, service);
        }
        queue.tail = request;
        expiry_ = std::min(expiry_, request->deadline);
    }
}

void TrafficLimiter::finish(Request* request, bool granted) {
    std::unique_ptr<Request> owner(request);
    pending_[lane_index(request->lane)].fetch_sub(1, std::memory_order_relaxed);
    backlog_.fetch_sub(1, std::memory_order_release);
    request->completion(granted);
}

void TrafficLimiter::prune(Clock::time_point now) {
    expiry_ = Clock::time_point::max();
    for (auto& queue : queues_) {
        Request* previous = nullptr;
        for (auto* item = queue.head; item;) {
            auto* next = item->next;
            if (item->deadline <= now || item->cancel.stop_requested()) {
                if (previous) previous->next = next;
                else queue.head = next;
                if (!next) queue.tail = previous;
                finish(item, false);
            } else {
                expiry_ = std::min(expiry_, item->deadline);
                previous = item;
            }
            item = next;
        }
    }
}

unsigned TrafficLimiter::select() const {
    unsigned best = queues_.size();
    for (unsigned i = 0; i < queues_.size(); ++i) {
        if (!queues_[i].head) continue;
        if (queues_[i].head->cancel.stop_requested()) return i;
        if (best == queues_.size()) {
            best = i;
            continue;
        }
        const auto group = std::min(i, 2u);
        const auto leader = std::min(best, 2u);
        if (service_[group] < service_[leader] ||
            (group == 2 && leader == 2 && copy_service_[i - 2] < copy_service_[best - 2])) best = i;
    }
    return best;
}

void TrafficLimiter::run(std::stop_token stop) {
    std::stop_callback wake_on_stop(stop, [this] { incoming_.notify(); });
    while (!stop.stop_requested()) {
        receive();
        const auto now = Clock::now();
        if (expiry_ <= now || (cancellation_.load(std::memory_order_acquire) &&
                              cancellation_.exchange(false, std::memory_order_acq_rel))) prune(now);
        const auto i = select();
        if (i == queues_.size()) {
            incoming_.wait(Clock::time_point::max());
            continue;
        }
        auto& queue = queues_[i];
        auto* request = queue.head;
        const bool cancelled = request->cancel.stop_requested();
        if (!cancelled) {
            const auto bytes = std::min(quantum, request->remaining);
            const auto refill = charge(bytes, now);
            if (refill != Clock::time_point::min()) {
                incoming_.wait(std::min(refill, expiry_));
                continue;
            }
            const auto group = std::min(i, 2u);
            floor_ = service_[group];
            service_[group] += bytes * class_stride[group];
            if (group == 2) {
                copy_floor_ = copy_service_[i - 2];
                copy_service_[i - 2] += bytes * copy_stride[i - 2];
            }
            granted_[i].fetch_add(bytes, std::memory_order_relaxed);
            request->remaining -= bytes;
            if (request->remaining) continue;
        }
        queue.head = request->next;
        if (!queue.head) queue.tail = nullptr;
        finish(request, !cancelled);
    }
    receive();
    for (auto& queue : queues_) {
        while (auto* request = queue.head) {
            queue.head = request->next;
            finish(request, false);
        }
    }
}

TrafficLimiter::Stats TrafficLimiter::stats() const {
    Stats result{{}, {}, rate_};
    for (unsigned i = 0; i < queues_.size(); ++i) {
        result.granted[i] = granted_[i].load(std::memory_order_relaxed);
        result.pending[i] = pending_[i].load(std::memory_order_relaxed);
    }
    return result;
}
}
