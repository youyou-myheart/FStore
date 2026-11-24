#pragma once

#include <atomic>
#include <chrono>
#include <semaphore>

namespace chunkstore {

// Producers transfer ownership of nodes with a Node* next field. One consumer
// detaches a batch and reverses it to recover successful CAS (FIFO) order.
template<class Node>
class MpscInbox {
    using Clock = std::chrono::steady_clock;
    static_assert(std::atomic<Node*>::is_always_lock_free);
public:
    void push(Node* node) noexcept {
        auto* head = head_.load(std::memory_order_relaxed);
        do {
            node->next = head;
        } while (!head_.compare_exchange_weak(head, node, std::memory_order_release,
                                              std::memory_order_relaxed));
        if (!head) notify();
    }

    bool has_pending() const noexcept {
        return head_.load(std::memory_order_acquire) != nullptr;
    }

    Node* take() noexcept {
        if (!has_pending()) return nullptr;
        auto* batch = head_.exchange(nullptr, std::memory_order_acquire);
        Node* first = nullptr;
        while (batch) {
            auto* next = batch->next;
            batch->next = first;
            first = batch;
            batch = next;
        }
        return first;
    }

    // Also used for cancellation/shutdown. Coalesce signals without losing a
    // wake between the consumer's final check and its timed or indefinite wait.
    void notify() noexcept {
        if (!notified_.exchange(true, std::memory_order_acq_rel)) wake_.release();
    }

    void wait(Clock::time_point until) {
        if (until == Clock::time_point::max()) wake_.acquire();
        else if (!wake_.try_acquire_until(until)) return;
        notified_.exchange(false, std::memory_order_acq_rel);
    }

private:
    std::atomic<Node*> head_{nullptr};
    std::atomic<bool> notified_{false};
    std::binary_semaphore wake_{0};
};

} // namespace chunkstore
