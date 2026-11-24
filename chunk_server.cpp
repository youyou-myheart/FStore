#include "chunk_store.hpp"
#include "mpsc_inbox.hpp"
#include "chunk_repair.hpp"
#include "chunk_rpc_limits.hpp"
#include "chunk.pb.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <brpc/protocol.h>
#include <bthread/processor.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <deque>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <cstdlib>
#include <iostream>
#include <list>
#include <mutex>
#include <optional>
#include <span>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <vector>

namespace {
namespace rpc = chunkstore::rpc::v1;
using chunkstore::TrafficLane;
using PollTime = std::chrono::microseconds;

struct WorkerPolling {
    PollTime budget{}; // zero sleeps immediately, max polls continuously
    unsigned workers = 0;
};

unsigned parse_unsigned(std::string_view text, std::string_view option) {
    unsigned value;
    auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (ec != std::errc{} || end != text.data() + text.size())
        throw std::invalid_argument("expected an unsigned integer for " + std::string(option));
    return value;
}

WorkerPolling parse_polling(std::span<char*> args, unsigned worker_count) {
    std::optional<PollTime> budget;
    std::optional<unsigned> count;
    for (std::string_view arg : args) {
        constexpr std::string_view mode_flag = "--worker-poll=";
        constexpr std::string_view count_flag = "--poll-workers=";
        if (arg.starts_with(mode_flag) && !budget) {
            const auto mode = arg.substr(mode_flag.size());
            if (mode == "sleep") budget = PollTime::zero();
            else if (mode == "busy") budget = PollTime::max();
            else if (mode.ends_with("us"))
                budget = PollTime(parse_unsigned(mode.substr(0, mode.size() - 2), mode_flag));
            else throw std::invalid_argument("--worker-poll expects sleep, busy or a duration such as 20us");
        } else if (arg.starts_with(count_flag) && !count) {
            count = parse_unsigned(arg.substr(count_flag.size()), count_flag);
            if (*count == 0 || *count > worker_count)
                throw std::invalid_argument("--poll-workers must be between 1 and " + std::to_string(worker_count));
        } else {
            throw std::invalid_argument("unknown or repeated server option: " + std::string(arg));
        }
    }
    if (budget.value_or(PollTime::zero()) == PollTime::zero()) {
        if (count) throw std::invalid_argument("--poll-workers requires a positive polling budget or busy mode");
        return {};
    }
    return {*budget, count.value_or(1)};
}

class CountedDone final : public google::protobuf::Closure {
public:
    CountedDone(std::atomic<unsigned>& count, google::protobuf::Closure* done)
        : count_(count), done_(done) {}
    static constexpr std::uint64_t one_request = 1ULL << 32;
    void routed(std::atomic<unsigned>& load, std::atomic<std::uint64_t>* owner) {
        load_ = &load;
        owner_ = owner;
        load_->fetch_add(1, std::memory_order_relaxed);
    }
    void Run() override {
        if (load_) load_->fetch_sub(1, std::memory_order_relaxed);
        if (owner_) owner_->fetch_sub(one_request, std::memory_order_release);
        auto* done = done_;
        --count_;
        delete this;
        done->Run();
    }
private:
    std::atomic<unsigned>* load_ = nullptr;
    std::atomic<std::uint64_t>* owner_ = nullptr;
    std::atomic<unsigned>& count_;
    google::protobuf::Closure* done_;
};

void fail(rpc::Status& status, rpc::Code code, const char* message) {
    status.set_code(code);
    status.set_message(message);
}

template<class Operation>
void complete(google::protobuf::Closure* done, rpc::Status& status, Operation operation) {
    brpc::ClosureGuard guard(done);
    try {
        operation();
    } catch (const chunkstore::RestoreConflict& e) {
        fail(status, rpc::RESTORE_CONFLICT, e.what());
    } catch (const chunkstore::RepairConflict& e) {
        fail(status, rpc::REPAIR_CONFLICT, e.what());
    } catch (const chunkstore::ChunkFrozen&) {
        fail(status, rpc::FROZEN, "chunk is frozen");
    } catch (const chunkstore::WriteUnavailable& e) {
        fail(status, rpc::WRITE_UNAVAILABLE, e.what());
    } catch (const chunkstore::ChunkFull& e) {
        fail(status, rpc::CHUNK_FULL, e.what());
    } catch (const chunkstore::CapacityMismatch& e) {
        fail(status, rpc::CAPACITY_MISMATCH, e.what());
    } catch (const chunkstore::LengthMismatch& e) {
        fail(status, rpc::LENGTH_MISMATCH, e.what());
    } catch (const std::invalid_argument& e) {
        fail(status, rpc::INVALID_ARGUMENT, e.what());
    } catch (const std::out_of_range&) {
        fail(status, rpc::OUT_OF_RANGE, "read exceeds published chunk length");
    } catch (const std::length_error&) {
        fail(status, rpc::TOO_LARGE, "chunk size limit exceeded");
    } catch (const std::system_error& e) {
        if (e.code() == std::errc::no_such_file_or_directory) {
            fail(status, rpc::NOT_FOUND, "chunk not found");
        } else {
            fail(status, rpc::STORAGE_ERROR, "storage I/O failed");
        }
    } catch (const std::exception&) {
        fail(status, rpc::STORAGE_ERROR, "service operation failed");
    }
}

class Service final : public rpc::ChunkService {
public:
    Service(chunkstore::ChunkStore& store, unsigned workers, unsigned concurrency, WorkerPolling polling)
        : store_(store), repairs_(store), online_limit_(concurrency), waiting_limit_(concurrency / 2),
          worker_count_(workers), polling_(polling), queues_(std::make_unique<Worker[]>(workers)) {
        for (unsigned i = 0; i < senders_.size(); ++i) {
            senders_[i] = std::jthread([this, i](std::stop_token stop) {
                auto& queue = sends_[i];
                std::unique_lock lock(sender_mutex_);
                while (sender_available_.wait(lock, stop, [&] { return !queue.empty(); })) {
                    auto job = std::move(queue.front());
                    queue.pop_front();
                    lock.unlock();
                    job();
                    lock.lock();
                }
            });
        }
        workers_.reserve(workers);
        for (unsigned i = 0; i < workers; ++i) {
            workers_.emplace_back([this, i](std::stop_token stop) {
                work(queues_[i], stop, i < polling_.workers ? polling_.budget : PollTime::zero());
            });
        }
    }

    void GetTrafficStats(google::protobuf::RpcController* controller, const rpc::TrafficStatsRequest*,
                         rpc::TrafficStatsResponse* response, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        if (!accept_protocol(*static_cast<brpc::Controller*>(controller))) return;
        const auto stats = store_.traffic().stats();
        response->set_bytes_per_second(stats.bytes_per_second);
        const std::array lanes{response->mutable_online(), response->mutable_trash(),
                               response->mutable_copy_normal(), response->mutable_copy_high(),
                               response->mutable_copy_low()};
        for (std::size_t i = 0; i < lanes.size(); ++i) {
            lanes[i]->set_granted_bytes(stats.granted[i]);
            lanes[i]->set_pending(stats.pending[i]);
        }
    }

    void RepairChunk(google::protobuf::RpcController* controller, const rpc::RepairRequest* request,
                     rpc::RepairResponse* response, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        if (!accept_protocol(*static_cast<brpc::Controller*>(controller))) {
            return;
        }
        auto& status = *response->mutable_status();
        if (!rpc::CopyPriority_IsValid(request->priority())) {
            fail(status, rpc::INVALID_ARGUMENT, "invalid replication priority");
            return;
        }
        try {
            std::lock_guard lock(sender_mutex_);
            auto& queue = sends_[request->priority()];
            if (queue.size() >= 4) {
                fail(status, rpc::WRITE_UNAVAILABLE, "replication priority queue is full");
                return;
            }
            queue.emplace_back([this, request, response, done, &status] {
                complete(done, status, [&] { chunkstore::push_replica(store_, *request, *response); });
            });
            guard.release();
            sender_available_.notify_all();
        } catch (const std::exception& e) {
            fail(status, rpc::REPAIR_FAILED, e.what());
        }
    }

    void ReceiveRepairBlock(google::protobuf::RpcController* controller, const rpc::RepairBlock* block,
                            rpc::RepairResponse* response, google::protobuf::Closure* done) override {
        if (!rpc::CopyPriority_IsValid(block->priority())) {
            brpc::ClosureGuard guard(done);
            fail(*response->mutable_status(), rpc::INVALID_ARGUMENT, "invalid replication priority");
            return;
        }
        const auto bytes = static_cast<brpc::Controller*>(controller)->request_attachment().size();
        submit(controller, done, *response->mutable_status(), [this, controller, block, response] {
            auto& call = *static_cast<brpc::Controller*>(controller);
            response->set_length(repairs_.receive(*block, call.request_attachment()));
            response->set_suggested_block_bytes(chunkstore::repair_block_size(store_.traffic()));
        }, &block->chunk_id(), chunkstore::copy_lane(block->priority()), bytes);
    }

    void CreateChunk(google::protobuf::RpcController* controller, const rpc::CreateRequest* request,
                     rpc::CreateResponse* response, google::protobuf::Closure* done) override {
        submit(controller, done, *response->mutable_status(), [this, request, response] {
            response->set_frozen(store_.create_chunk(request->chunk_id(), request->capacity()) ==
                                 chunkstore::State::Frozen);
            response->set_capacity(request->capacity());
        }, &request->chunk_id());
    }

    void Append(google::protobuf::RpcController* controller, const rpc::AppendRequest* request,
                rpc::AppendResponse* response, google::protobuf::Closure* done) override {
        auto& status = *response->mutable_status();
        brpc::ClosureGuard guard(done);
        auto& call = *static_cast<brpc::Controller*>(controller);
        if (!accept_protocol(call)) {
            return;
        }
        const auto& data = call.request_attachment();
        if (request->length() > chunkstore::max_rpc_payload ||
            data.size() > chunkstore::max_rpc_payload) {
            fail(status, rpc::TOO_LARGE, "append exceeds RPC payload limit");
            return;
        }
        if (request->length() != data.size()) {
            fail(status, rpc::INVALID_ARGUMENT, "append attachment length mismatch");
            return;
        }
        if (!request->has_offset() || !request->has_wait_timeout_ms() || data.empty() ||
            request->wait_timeout_ms() > chunkstore::max_gap_wait_ms) {
            fail(status, rpc::INVALID_ARGUMENT, "append requires offset, timeout (0..60000 ms), and data");
            return;
        }
        const auto timeout = request->wait_timeout_ms();
        const auto deadline = timeout == 0 ? Clock::time_point::max() :
            Clock::now() + std::chrono::milliseconds(timeout);
        auto* counted = reserve(guard, done, status, TrafficLane::Online);
        if (counted) admit(guard, status, &request->chunk_id(),
                        Job{{}, &data, response, counted, request->offset(), deadline},
                        counted, TrafficLane::Online, data.size());
    }

    void ReadChunk(google::protobuf::RpcController* controller, const rpc::ReadRequest* request,
                   rpc::ReadResponse* response, google::protobuf::Closure* done) override {
        auto& status = *response->mutable_status();
        submit(controller, done, status, [this, controller, request, response, &status] {
            if (request->length() > chunkstore::max_rpc_payload) {
                fail(status, rpc::TOO_LARGE, "read exceeds RPC payload limit");
                return;
            }
            const auto length = static_cast<std::size_t>(request->length());
            auto* brpc_controller = static_cast<brpc::Controller*>(controller);
            if (length == 0) {
                store_.read_chunk(request->chunk_id(), request->offset(), std::span<char>{});
                return;
            }
            auto data = std::make_unique_for_overwrite<char[]>(length);
            store_.read_chunk(request->chunk_id(), request->offset(), std::span(data.get(), length));
            if (brpc_controller->response_attachment().append_user_data(
                    data.get(), length, [](void* p) { delete[] static_cast<char*>(p); }) != 0) {
                fail(status, rpc::STORAGE_ERROR, "could not attach read data");
                return;
            }
            data.release();
        }, nullptr, TrafficLane::Online, std::min<std::uint64_t>(request->length(), chunkstore::max_rpc_payload));
    }

    void DeleteChunk(google::protobuf::RpcController* controller, const rpc::ChunkRequest* request,
                     rpc::DeleteResponse* response, google::protobuf::Closure* done) override {
        submit(controller, done, *response->mutable_status(), [this, request, response] {
            response->set_deleted_at_ns(store_.delete_chunk(request->chunk_id()));
            repairs_.discard(request->chunk_id());
        }, &request->chunk_id());
    }

    void RestoreChunk(google::protobuf::RpcController* controller, const rpc::RestoreRequest* request,
                      rpc::RestoreResponse* response, google::protobuf::Closure* done) override {
        submit(controller, done, *response->mutable_status(), [this, request, response] {
            response->set_frozen(store_.restore_chunk(request->chunk_id(), request->deleted_at_ns()) ==
                                 chunkstore::State::Frozen);
        }, &request->chunk_id());
    }

    void FreezeChunk(google::protobuf::RpcController* controller, const rpc::ChunkRequest* request,
                     rpc::FreezeResponse* response, google::protobuf::Closure* done) override {
        submit(controller, done, *response->mutable_status(), [this, request, response] {
            response->set_length(store_.freeze_chunk(request->chunk_id()));
        }, &request->chunk_id());
    }

    void FreezeChunkAtLength(google::protobuf::RpcController* controller,
                             const rpc::FreezeAtLengthRequest* request, rpc::FreezeResponse* response,
                             google::protobuf::Closure* done) override {
        submit(controller, done, *response->mutable_status(), [this, request, response] {
            response->set_length(store_.freeze_chunk(request->chunk_id(), request->expected_length()));
        }, &request->chunk_id());
    }

private:
    static constexpr std::size_t max_group_requests = 32;

    using Clock = std::chrono::steady_clock;

    struct Job {
        std::function<void()> operation;
        const butil::IOBuf* data = nullptr;
        rpc::AppendResponse* response = nullptr;
        google::protobuf::Closure* done = nullptr;
        std::uint64_t offset = 0;
        Clock::time_point deadline = Clock::time_point::max();
        rpc::Code rejection = rpc::OK;
    };

    struct WriteQueue;
    struct Ready {
        std::function<void()> read;
        const std::string* chunk_id = nullptr;
        WriteQueue* writes = nullptr;
        Clock::time_point deadline = Clock::time_point::max();
    };

    struct WriteQueue {
        std::list<Job> pending;
        std::optional<std::list<Ready>::iterator> parked;
    };

    struct Submission {
        const std::string* id;
        Job job;
        rpc::Status* status;
        google::protobuf::Closure* done;
        Submission* next = nullptr;
    };

    struct alignas(64) Worker {
        std::atomic<unsigned> load{0};
        chunkstore::MpscInbox<Submission> inbox;
        std::list<Ready> ready;
        std::list<Ready> parked;
        std::unordered_map<std::string, WriteQueue> writes;
    };

    static bool accept_protocol(brpc::Controller& call) {
        // bRPC recognizes HTTP even when enabled_protocols lists only baidu_std.
        if (call.request_protocol() == brpc::PROTOCOL_BAIDU_STD) {
            return true;
        }
        call.SetFailed("ChunkService requires baidu_std");
        return false;
    }

    template<class Operation>
    void submit(google::protobuf::RpcController* controller, google::protobuf::Closure* done,
                rpc::Status& status, Operation operation, const std::string* id = nullptr,
                TrafficLane lane = TrafficLane::Online, std::uint64_t bytes = 0) {
        brpc::ClosureGuard guard(done);
        if (!accept_protocol(*static_cast<brpc::Controller*>(controller))) {
            return;
        }
        auto* counted = reserve(guard, done, status, lane);
        if (!counted) return;
        try {
            admit(guard, status, id, Job{[counted, &status, operation = std::move(operation)] {
                complete(counted, status, operation);
            }}, counted, lane, bytes);
        } catch (const std::bad_alloc&) {
            fail(status, rpc::WRITE_UNAVAILABLE, "could not allocate storage operation");
        }
    }

    CountedDone* reserve(brpc::ClosureGuard& guard, google::protobuf::Closure* done,
                                       rpc::Status& status, TrafficLane lane) {
        auto& counter = in_flight_[chunkstore::lane_index(lane)];
        const auto limit = lane == TrafficLane::Online ? online_limit_ : 4u;
        if (counter.fetch_add(1) >= limit) {
            --counter;
            fail(status, rpc::WRITE_UNAVAILABLE, "traffic class concurrency limit reached");
            return nullptr;
        }
        auto* wrapper = new (std::nothrow) CountedDone(counter, done);
        if (!wrapper) {
            --counter;
            fail(status, rpc::WRITE_UNAVAILABLE, "could not allocate RPC completion");
            return nullptr;
        }
        guard.release();
        guard.reset(wrapper);
        return wrapper;
    }

    unsigned least_loaded(unsigned start) const {
        // Prefer an idle poller for new work. If it is in I/O or owns a parked
        // append, keep the whole disk pool available. Live write leases bypass
        // this selection and continue to use their original worker.
        for (unsigned n = 0; n < polling_.workers; ++n) {
            const unsigned i = (start + n) % polling_.workers;
            if (queues_[i].load.load(std::memory_order_relaxed) == 0) return i;
        }
        unsigned best = start;
        auto minimum = queues_[best].load.load(std::memory_order_relaxed);
        for (unsigned n = 1; minimum && n < worker_count_; ++n) {
            const unsigned i = (start + n) % worker_count_;
            const auto load = queues_[i].load.load(std::memory_order_relaxed);
            if (load < minimum) {
                best = i;
                minimum = load;
            }
        }
        return best;
    }

    unsigned route(const std::string* id, CountedDone& done) {
        if (!id) {
            const auto start = next_read_.fetch_add(1, std::memory_order_relaxed) % worker_count_;
            const auto i = least_loaded(start);
            done.routed(queues_[i].load, nullptr);
            return i;
        }
        const auto hash = std::hash<std::string_view>{}(*id);
        auto& owner = owners_[hash % owners_.size()];
        auto current = owner.load(std::memory_order_relaxed);
        // Low bits name the worker, high bits count all jobs until completion,
        // including parked appends. An idle bucket can migrate around blocked I/O.
        for (;;) {
            const bool idle = current < CountedDone::one_request;
            const auto i = idle ? least_loaded(hash % worker_count_) : static_cast<unsigned>(current);
            const auto next = (idle ? i : current) + CountedDone::one_request;
            if (owner.compare_exchange_weak(current, next, std::memory_order_acq_rel)) {
                done.routed(queues_[i].load, &owner);
                return i;
            }
        }
    }

    void admit(brpc::ClosureGuard& guard, rpc::Status& status, const std::string* id, Job job,
               CountedDone* done, TrafficLane lane, std::uint64_t bytes) {
        const auto deadline = job.deadline;
        try {
            store_.traffic().enqueue(lane, bytes, [this, id, &status, done, deadline, job = std::move(job)](bool ok) mutable {
                brpc::ClosureGuard admitted(done);
                if (ok) {
                    auto* item = new (std::nothrow) Submission{id, std::move(job), &status, done};
                    if (!item) {
                        fail(status, rpc::WRITE_UNAVAILABLE, "could not queue storage operation");
                        return;
                    }
                    const auto index = route(id, *done);
                    admitted.release();
                    queues_[index].inbox.push(item);
                } else if (deadline <= Clock::now()) fail(status, rpc::GAP_TIMEOUT, "append expired in traffic queue");
                else fail(status, rpc::WRITE_UNAVAILABLE, "traffic scheduler stopped");
            }, {}, deadline);
            guard.release();
        } catch (const std::bad_alloc&) {
            fail(status, rpc::WRITE_UNAVAILABLE, "could not queue traffic");
        }
    }

    void enqueue(Worker& worker, Submission& item) {
        brpc::ClosureGuard guard(item.done);
        const auto* id = item.id;
        auto& job = item.job;
        try {
            if (id == nullptr) {
                if (worker.ready.empty()) {
                    guard.release();
                    job.operation();
                    return;
                }
                worker.ready.push_back(Ready{std::move(job.operation)});
            } else {
                auto [entry, inserted] = worker.writes.try_emplace(*id);
                try {
                    auto& queue = entry->second;
                    const auto previous_size = queue.pending.size();
                    queue.pending.push_back(std::move(job));
                    if (inserted) {
                        worker.ready.push_back(Ready{{}, &entry->first, &queue});
                    } else if (queue.parked) {
                        wake(worker, *queue.parked, previous_size);
                    }
                } catch (...) {
                    if (inserted) {
                        worker.writes.erase(entry);
                    }
                    throw;
                }
            }
            guard.release();
        } catch (const std::exception&) {
            fail(*item.status, rpc::WRITE_UNAVAILABLE, "could not queue storage operation");
            return;
        }
    }

    void wake(Worker& worker, std::list<Ready>::iterator task, std::size_t waiting) {
        waiting_count_.fetch_sub(waiting, std::memory_order_relaxed);
        task->writes->parked.reset();
        worker.ready.splice(worker.ready.end(), worker.parked, task);
    }

    static void reject(std::list<Job>& rejected, std::list<Job>& pending,
                       std::list<Job>::iterator job, rpc::Code code) {
        job->rejection = code;
        rejected.splice(rejected.end(), pending, job);
    }

    std::size_t select(std::list<Job>& pending, std::span<Job> group,
                       chunkstore::AppendWindow window, std::list<Job>& rejected) {
        const auto now = Clock::now();
        std::size_t count = 0;
        std::size_t bytes = 0;
        while (count < group.size()) {
            auto next = pending.end();
            auto control = pending.end();
            for (auto it = pending.begin(); it != pending.end();) {
                auto job = it++;
                if (job->data == nullptr) {
                    control = job;
                    break;
                }

                const auto size = job->data->size();
                if (job->deadline <= now) {
                    reject(rejected, pending, job, rpc::GAP_TIMEOUT);
                } else if (job->offset < window.offset) {
                    reject(rejected, pending, job, rpc::OFFSET_CONFLICT);
                } else if (job->offset > window.capacity || size > window.capacity - job->offset) {
                    reject(rejected, pending, job, rpc::CHUNK_FULL);
                } else if (job->offset == window.offset) {
                    next = job;
                    break;
                }
            }
            if (next == pending.end()) {
                if (count == 0 && control != pending.end()) {
                    group[count++] = std::move(*control);
                    pending.erase(control);
                }
                break;
            }
            const auto size = next->data->size();
            if (size > chunkstore::max_rpc_payload - bytes) {
                break;
            }
            bytes += size;
            window.offset += size;
            group[count++] = std::move(*next);
            pending.erase(next);
        }
        return count;
    }

    void park(Worker& worker, std::list<Ready>& running, std::list<Job>& rejected) {
        auto& task = running.front();
        auto& pending = task.writes->pending;
        auto deadline = Clock::time_point::max();
        for (auto it = pending.begin(); it != pending.end();) {
            auto job = it++;
            if (job->deadline == Clock::time_point::max()) {
                reject(rejected, pending, job, rpc::GAP_TIMEOUT);
            } else if (waiting_count_.fetch_add(1, std::memory_order_relaxed) >= waiting_limit_) {
                waiting_count_.fetch_sub(1, std::memory_order_relaxed);
                reject(rejected, pending, job, rpc::WRITE_UNAVAILABLE);
            } else {
                deadline = std::min(deadline, job->deadline);
            }
        }
        if (pending.empty()) {
            worker.writes.erase(*task.chunk_id);
            running.clear();
            return;
        }

        task.deadline = deadline;
        task.writes->parked = running.begin();
        const auto position = std::find_if(worker.parked.begin(), worker.parked.end(),
            [deadline](const Ready& item) { return item.deadline > deadline; });
        worker.parked.splice(position, running);
    }

    static void finish_rejected(const std::list<Job>& rejected, std::exception_ptr failure) {
        for (const auto& job : rejected) {
            auto& status = *job.response->mutable_status();
            complete(job.done, status, [&] {
                if (job.rejection == rpc::OK) {
                    std::rethrow_exception(failure);
                } else if (job.rejection == rpc::GAP_TIMEOUT) {
                    fail(status, job.rejection, "append expired before its offset became writable");
                } else if (job.rejection == rpc::OFFSET_CONFLICT) {
                    fail(status, job.rejection, "append offset overlaps existing bytes");
                } else if (job.rejection == rpc::CHUNK_FULL) {
                    fail(status, job.rejection, "append exceeds chunk capacity");
                } else {
                    fail(status, job.rejection, "out-of-order append buffer is full");
                }
            });
        }
    }

    static void wait_for_work(Worker& worker, std::stop_token stop, PollTime budget) {
        const auto deadline = worker.parked.empty() ? Clock::time_point::max() : worker.parked.front().deadline;
        if (budget == PollTime::zero()) {
            worker.inbox.wait(deadline);
            return;
        }
        auto spin_until = deadline;
        if (budget != PollTime::max()) spin_until = std::min(deadline, Clock::now() + budget);
        while (!worker.inbox.has_pending() && !stop.stop_requested()) {
            if (spin_until != Clock::time_point::max() && Clock::now() >= spin_until) break;
            cpu_relax();
        }
        if (spin_until == deadline) return;
        // A signal can already be posted for a batch consumed while spinning.
        // Recheck work after consuming it without restarting the spin budget.
        while (!worker.inbox.has_pending() && !stop.stop_requested() && Clock::now() < deadline)
            worker.inbox.wait(deadline);
    }

    void work(Worker& worker, std::stop_token stop, PollTime budget) {
        // The ready node stays alive while a worker owns the Chunk.
        std::list<Ready> running;
        std::array<Job, max_group_requests> group;
        std::stop_callback wake_on_stop(stop, [&] { worker.inbox.notify(); });
        while (!stop.stop_requested()) {
            for (auto* item = worker.inbox.take(); item;) {
                std::unique_ptr<Submission> current(item);
                item = item->next;
                enqueue(worker, *current);
            }
            if (!worker.parked.empty() && worker.parked.front().deadline <= Clock::now())
                wake(worker, worker.parked.begin(), worker.parked.front().writes->pending.size());
            if (worker.ready.empty()) {
                wait_for_work(worker, stop, budget);
                continue;
            }
            running.splice(running.end(), worker.ready, worker.ready.begin());
            auto& task = running.front();
            if (task.writes == nullptr) {
                task.read();
                running.clear();
                continue;
            }

            auto& pending = task.writes->pending;
            std::size_t count = 0;
            std::list<Job> rejected;
            std::exception_ptr failure;
            if (pending.front().data == nullptr) {
                group[count++] = std::move(pending.front());
                pending.pop_front();
            } else {
                chunkstore::AppendWindow window{};
                try {
                    window = store_.append_window(*task.chunk_id);
                } catch (...) {
                    failure = std::current_exception();
                }
                if (failure) {
                    auto end = std::find_if(pending.begin(), pending.end(),
                        [](const Job& job) { return job.data == nullptr; });
                    rejected.splice(rejected.end(), pending, pending.begin(), end);
                } else {
                    count = select(pending, group, window, rejected);
                    if (count == 0) {
                        park(worker, running, rejected);
                        finish_rejected(rejected, failure);
                        continue;
                    }
                }
            }

            finish_rejected(rejected, failure);
            if (count != 0) {
                if (group.front().data == nullptr) {
                    group.front().operation();
                } else {
                    append_group(*task.chunk_id, std::span(group).first(count));
                }
                for (auto& job : std::span(group).first(count)) {
                    job = {};
                }
            }
            if (pending.empty()) {
                worker.writes.erase(*task.chunk_id);
                running.clear();
            } else {
                worker.ready.splice(worker.ready.end(), running);
            }
        }
    }

    void append_group(std::string_view id, std::span<Job> group) {
        std::array<chunkstore::AppendGroupItem, max_group_requests> requests;
        std::exception_ptr failure;
        try {
            std::size_t block_count = 0;
            for (const auto& job : group) {
                block_count += job.data->backing_block_num();
            }
            std::array<std::string_view, max_group_requests> small_buffers;
            std::vector<std::string_view> large_buffers;
            std::span<std::string_view> buffers(small_buffers);
            if (block_count > buffers.size()) {
                large_buffers.resize(block_count);
                buffers = large_buffers;
            }
            for (std::size_t i = 0; i < group.size(); ++i) {
                const auto& data = *group[i].data;
                const auto count = data.backing_block_num();
                requests[i].buffers = buffers.first(count);
                for (std::size_t block_index = 0; block_index < count; ++block_index) {
                    const auto block = data.backing_block(block_index);
                    buffers[block_index] = std::string_view(block.data(), block.size());
                }
                buffers = buffers.subspan(count);
            }
            store_.append_group(id, std::span(requests).first(group.size()));
        } catch (...) {
            failure = std::current_exception();
        }
        for (std::size_t i = 0; i < group.size(); ++i) {
            const auto& job = group[i];
            auto& response = *job.response;
            complete(job.done, *response.mutable_status(), [&] {
                if (failure) {
                    std::rethrow_exception(failure);
                }
                if (const auto& result = requests[i].result) {
                    response.set_offset(result->offset);
                    response.set_length(result->length);
                } else {
                    fail(*response.mutable_status(), rpc::CHUNK_FULL, "append exceeds chunk capacity");
                }
            });
        }
    }

    chunkstore::ChunkStore& store_;
    chunkstore::RepairReceiver repairs_;
    std::mutex sender_mutex_;
    std::condition_variable_any sender_available_;
    std::array<std::deque<std::function<void()>>, 3> sends_;
    std::array<std::jthread, 3> senders_;
    std::array<std::atomic<unsigned>, 5> in_flight_{};
    const unsigned online_limit_;
    const std::size_t waiting_limit_;
    std::atomic<std::size_t> waiting_count_{0};
    const unsigned worker_count_;
    const WorkerPolling polling_;
    std::unique_ptr<Worker[]> queues_;
    std::array<std::atomic<std::uint64_t>, 1024> owners_{};
    std::atomic<unsigned> next_read_{0};
    std::vector<std::jthread> workers_;
};
}

int main(int argc, char** argv) {
    int options_begin = 2;
    while (options_begin < argc && !std::string_view(argv[options_begin]).starts_with("--")) ++options_begin;
    if (argc < 2 || options_begin > 5) {
        std::cerr << "usage: chunk_server DATA_DIR [ADDRESS:PORT [MAX_CONCURRENCY [RATE_MIB_S]]]"
                     " [--worker-poll=sleep|<N>us|busy] [--poll-workers=N]\n";
        return EXIT_FAILURE;
    }
    const char* endpoint = options_begin > 2 ? argv[2] : "127.0.0.1:8080";
    int concurrency = 64;
    if (options_begin > 3) {
        const std::string_view value(argv[3]);
        const auto [end, ec] = std::from_chars(value.data(), value.data() + value.size(), concurrency);
        if (ec != std::errc{} || end != value.data() + value.size() || concurrency < 1) {
            std::cerr << "invalid max concurrency\n";
            return EXIT_FAILURE;
        }
    }
    std::uint64_t rate = chunkstore::TrafficLimiter::default_rate;
    if (options_begin == 5) {
        std::uint64_t mib = 0;
        const std::string_view value(argv[4]);
        const auto [end, ec] = std::from_chars(value.data(), value.data() + value.size(), mib);
        if (ec != std::errc{} || end != value.end() || !mib || mib > (UINT64_MAX >> 20)) {
            std::cerr << "invalid traffic rate in MiB/s\n";
            return EXIT_FAILURE;
        }
        rate = mib << 20;
    }
    try {
        // A worker can sleep in fdatasync. Give unrelated chunk IDs more lanes
        // than CPU cores, bounded by the configured number of outstanding RPCs.
        const auto workers = std::min(static_cast<unsigned>(concurrency),
            4 * std::clamp(std::thread::hardware_concurrency(), 4u, 16u));
        const auto polling = parse_polling(std::span(argv + options_begin, argc - options_begin), workers);
        chunkstore::ChunkStore store(argv[1], rate);
        Service service(store, workers, static_cast<unsigned>(concurrency), polling);
        brpc::FLAGS_max_body_size = chunkstore::max_rpc_payload + 64 * 1024;
        brpc::Server server;
        if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            return EXIT_FAILURE;
        }
        brpc::ServerOptions options;
        options.has_builtin_services = false;
        options.enabled_protocols = "baidu_std";
        // Admission caps are separate for online RPCs, repair commands and inbound blocks.
        options.max_concurrency = 0;
        if (server.Start(endpoint, &options) != 0) {
            return EXIT_FAILURE;
        }
        server.RunUntilAskedToQuit();
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return EXIT_FAILURE;
    }
}
