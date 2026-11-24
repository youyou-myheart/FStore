#include "traffic_limiter.hpp"

#include <algorithm>
#include <future>
#include <stdexcept>

namespace chunkstore {
namespace {
using Clock = std::chrono::steady_clock;
constexpr std::uint64_t quantum = 64 * 1024;
constexpr std::uint64_t minimum_cost = 4 * 1024;
constexpr std::array<unsigned, 3> class_stride{1, 8, 8};
constexpr std::array<unsigned, 3> copy_stride{2, 1, 4}; // normal, high, low
}

TrafficLimiter::TrafficLimiter(std::uint64_t rate) : rate_(rate) {
    if (!rate) throw std::invalid_argument("traffic rate must be positive");
    dispatcher_ = std::jthread([this](std::stop_token stop) { run(stop); });
}

TrafficLimiter::~TrafficLimiter() = default;

void TrafficLimiter::enqueue(TrafficLane lane, std::uint64_t bytes,
                             std::function<void(bool)> completion, std::stop_token cancel,
                             Clock::time_point deadline) {
    const auto i = lane_index(lane);
    {
        std::lock_guard lock(mutex_);
        auto& queue = queues_.at(i);
        if (queue.empty()) {
            if (i >= 2) {
                copy_service_[i - 2] = std::max(copy_floor_, copy_service_[i - 2]);
            }
            auto& service = service_[std::min(i, 2u)];
            service = std::max(floor_, service);
        }
        auto timer = deadlines_.end();
        if (deadline != Clock::time_point::max()) timer = deadlines_.insert(deadline);
        try {
            queue.push_back({std::max(bytes, minimum_cost), std::move(completion), cancel, deadline});
        } catch (...) {
            if (timer != deadlines_.end()) deadlines_.erase(timer);
            throw;
        }
    }
    changed_.notify_one();
}

bool TrafficLimiter::acquire(TrafficLane lane, std::uint64_t bytes, std::stop_token cancel) {
    if (cancel.stop_requested()) return false;
    auto promise = std::make_shared<std::promise<bool>>();
    auto result = promise->get_future();
    std::stop_callback notify_cancel(cancel, [this] {
        std::lock_guard lock(mutex_);
        changed_.notify_one();
    });
    enqueue(lane, bytes, [promise](bool ok) { promise->set_value(ok); }, cancel);
    return result.get();
}

unsigned TrafficLimiter::select() const {
    unsigned best = queues_.size();
    for (unsigned i = 0; i < queues_.size(); ++i) {
        if (queues_[i].empty()) continue;
        if (queues_[i].front().cancel.stop_requested()) return i;
        if (best == queues_.size()) {
            best = i;
            continue;
        }
        const auto group = std::min(i, 2u);
        const auto leader = std::min(best, 2u);
        if (service_[group] < service_[leader] ||
            (group == 2 && leader == 2 && copy_service_[i - 2] < copy_service_[best - 2])) {
            best = i;
        }
    }
    return best;
}

void TrafficLimiter::run(std::stop_token stop) {
    using namespace std::chrono;
    // At most one quantum of idle credit. Accumulate continuously at the configured rate.
    auto updated = Clock::now();
    double credit = quantum;
    std::unique_lock lock(mutex_);
    while (!stop.stop_requested()) {
        const auto expiry = deadlines_.empty() ? Clock::time_point::max() : *deadlines_.begin();
        if (expiry <= Clock::now()) {
            // An append behind a large FIFO head must still time out on schedule.
            for (auto& queue : queues_) {
                const auto it = std::find_if(queue.begin(), queue.end(), [expiry](const Request& r) {
                    return r.deadline == expiry;
                });
                if (it == queue.end()) continue;
                auto callback = std::move(it->completion);
                queue.erase(it);
                deadlines_.erase(deadlines_.begin());
                lock.unlock();
                callback(false);
                lock.lock();
                break;
            }
            continue;
        }
        const auto i = select();
        if (i == queues_.size()) {
            changed_.wait(lock, stop, [this] { return select() < queues_.size(); });
            continue;
        }
        auto& req = queues_[i].front();
        const auto now = Clock::now();
        credit = std::min<double>(quantum, credit + duration<double>(now - updated).count() * rate_);
        updated = now;
        const auto bytes = std::min(quantum, req.remaining);
        const bool cancelled = req.cancel.stop_requested();
        if (!cancelled && credit < bytes) {
            // Re-elect after a wake so a newly active class can use its share.
            const auto refill = now + ceil<Clock::duration>(duration<double>((bytes - credit) / rate_));
            changed_.wait_until(lock, stop, std::min(refill, expiry), [&] {
                return select() != i || req.cancel.stop_requested() ||
                       (!deadlines_.empty() && *deadlines_.begin() < expiry);
            });
            continue;
        }
        if (!cancelled) {
            const auto group = std::min(i, 2u);
            floor_ = service_[group];
            service_[group] += bytes * class_stride[group];
            if (group == 2) {
                copy_floor_ = copy_service_[i - 2];
                copy_service_[i - 2] += bytes * copy_stride[i - 2];
            }
            granted_[i] += bytes;
            credit -= bytes;
            req.remaining -= bytes;
            if (req.remaining) continue;
        }

        if (req.deadline != Clock::time_point::max()) deadlines_.erase(deadlines_.find(req.deadline));
        auto callback = std::move(req.completion);
        queues_[i].pop_front();
        lock.unlock();
        callback(!cancelled);
        lock.lock();
    }
    for (auto& queue : queues_) {
        while (!queue.empty()) {
            auto callback = std::move(queue.front().completion);
            queue.pop_front();
            lock.unlock();
            callback(false);
            lock.lock();
        }
    }
}

TrafficLimiter::Stats TrafficLimiter::stats() const {
    std::lock_guard lock(mutex_);
    Stats result{granted_, {}, rate_};
    for (unsigned i = 0; i < queues_.size(); ++i) result.pending[i] = queues_[i].size();
    return result;
}
}
