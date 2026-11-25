#include "fmaster_monitor.hpp"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <google/protobuf/util/message_differencer.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <limits>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace chunkstore::master {

struct Monitor::Impl {
    using Clock = std::chrono::steady_clock;

    struct Probe : google::protobuf::Closure {
        Impl& owner;
        wire::ServerHeartbeatRequest request;
        wire::ServerHeartbeatResponse response;
        brpc::Controller controller;
        brpc::CallId call_id{};
        std::atomic<bool> ready{false};
        bool started = false;

        Probe(Impl& monitor, const wire::FServer& server, std::uint64_t revision)
            : owner(monitor) {
            *request.mutable_server() = server;
            request.set_config_revision(revision);
        }

        void launch(wire::FServerStatusService_Stub& stub) {
            try {
                call_id = controller.call_id();
                stub.Heartbeat(&controller, &request, &response, this);
                started = true;
            } catch (...) {
                ready.store(true, std::memory_order_release);
                throw;
            }
        }

        void join() {
            if (started) {
                brpc::Join(call_id);
                started = false;
            }
        }
        void cancel() {
            if (started && !ready.load(std::memory_order_acquire)) brpc::StartCancel(call_id);
        }
        ~Probe() override {
            cancel();
            join();
        }

        void Run() override {
            try {
                if (controller.Failed()) throw std::runtime_error(controller.ErrorText());
                if (!response.has_status()) throw std::runtime_error("missing probe response status");
                if (response.status().code() != wire::OK) {
                    throw std::runtime_error("FServer probe error " +
                                             wire::Code_Name(response.status().code()) + ": " +
                                             response.status().message());
                }
                if (!response.has_report()) throw std::runtime_error("missing probe report");
                const auto& report = response.report();
                if (report.server_id() != request.server().id() ||
                    report.config_revision() != request.config_revision()) {
                    throw std::runtime_error("probe response identity or configuration revision mismatch");
                }
                // The store compares resolved endpoints, allowing configured
                // hostnames to match the server's canonical address.
                owner.store.record_heartbeat(report);
            } catch (const std::exception& error) {
                owner.record_failure(request.server(), request.config_revision(), error.what());
            } catch (...) {
                owner.record_failure(request.server(), request.config_revision(), "unknown probe failure");
            }
            ready.store(true, std::memory_order_release);
        }
    };

    struct Target {
        std::string endpoint;
        brpc::Channel channel;
        wire::FServerStatusService_Stub stub{&channel};
        // Destroy the request and join its callback before destroying the channel.
        std::unique_ptr<Probe> pending;

        Target(const std::string& address, int timeout) : endpoint(address) {
            brpc::ChannelOptions options;
            options.protocol = brpc::PROTOCOL_BAIDU_STD;
            options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
            options.connect_timeout_ms = timeout;
            options.timeout_ms = timeout;
            options.max_retry = 0;
            if (channel.Init(endpoint.c_str(), &options) != 0) {
                throw std::runtime_error("could not initialize FServer probe channel");
            }
        }
    };

    Impl(Store& registry, std::chrono::milliseconds poll_interval,
         std::chrono::milliseconds probe_timeout)
        : store(registry), interval(poll_interval) {
        if (interval.count() <= 0 || interval.count() > std::numeric_limits<int>::max() ||
            probe_timeout.count() <= 0 || probe_timeout.count() > std::numeric_limits<int>::max()) {
            throw std::invalid_argument("monitor interval and RPC timeout must be in 1..INT_MAX ms");
        }
        rpc_timeout_ms = static_cast<int>(probe_timeout.count());
        worker = std::jthread([this](std::stop_token stop) { run(stop); });
    }

    void log_failure(const char* error) noexcept {
        const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
            Clock::now().time_since_epoch()).count();
        auto next = next_log_ms.load(std::memory_order_relaxed);
        if (now >= next && next_log_ms.compare_exchange_strong(next, now + 10000, std::memory_order_relaxed)) {
            std::fprintf(stderr, "FMaster monitor: %s\n", error);
        }
    }

    void record_failure(const wire::FServer& server, std::uint64_t revision, const char* error) noexcept {
        try {
            store.record_probe_failure(server.id(), server.endpoint(), revision, error);
        } catch (const std::exception& failure) {
            log_failure(failure.what());
        } catch (...) {
            log_failure("could not record probe failure");
        }
    }

    std::uint32_t request_probe(const std::string& server_id) {
        wire::ListResponse snapshot;
        store.list(snapshot, server_id);
        const auto count = snapshot.topology().servers_size();
        if (!server_id.empty() && count == 0) {
            throw Error(wire::NOT_FOUND, "FServer is not configured: " + server_id);
        }
        {
            std::lock_guard lock(wait_mutex);
            for (const auto& server : snapshot.topology().servers()) {
                requested_ids.insert(server.id());
            }
        }
        timer.notify_one();
        return static_cast<std::uint32_t>(count);
    }

    void sweep(std::stop_token stop, const std::unordered_set<std::string>* selected) {
        wire::ListResponse snapshot;
        store.list(snapshot);
        std::unordered_set<std::string_view> configured;
        configured.reserve(snapshot.topology().servers_size());
        for (const auto& server : snapshot.topology().servers()) {
            configured.insert(server.id());
            if (stop.stop_requested()) return;
            if (selected && !selected->contains(server.id())) continue;
            try {
                auto& target = targets[server.id()];
                if (!target || target->endpoint != server.endpoint()) {
                    target = std::make_unique<Target>(server.endpoint(), rpc_timeout_ms);
                }
                if (target->pending) {
                    if (!google::protobuf::util::MessageDifferencer::Equals(
                            target->pending->request.server(), server)) {
                        target->pending->cancel();
                    } else if (!target->pending->ready.load(std::memory_order_acquire)) {
                        continue;
                    }
                    target->pending->join();
                    target->pending.reset();
                }
                target->pending = std::make_unique<Probe>(*this, server, snapshot.topology().revision());
                target->pending->launch(target->stub);
            } catch (const std::exception& error) {
                record_failure(server, snapshot.topology().revision(), error.what());
            }
        }
        std::erase_if(targets, [&](const auto& entry) { return !configured.contains(entry.first); });
    }

    void run(std::stop_token stop) noexcept {
        auto next_regular = Clock::now();
        while (!stop.stop_requested()) {
            std::unordered_set<std::string> selected;
            bool regular;
            {
                std::unique_lock lock(wait_mutex);
                timer.wait_until(lock, stop, next_regular, [&] { return !requested_ids.empty(); });
                if (stop.stop_requested()) break;
                const auto now = Clock::now();
                regular = now >= next_regular;
                if (regular) {
                    // Preserve the periodic cadence even if manual requests are
                    // frequent, skipping elapsed ticks after a slow sweep.
                    next_regular += interval * ((now - next_regular) / interval + 1);
                    requested_ids.clear();
                } else {
                    selected.swap(requested_ids);
                }
            }
            try {
                sweep(stop, regular ? nullptr : &selected);
            } catch (const std::exception& error) {
                log_failure(error.what());
            } catch (...) {
                log_failure("unknown monitoring failure");
            }
        }
        // Cancel every outstanding request before waiting for any callback.
        for (auto& [id, target] : targets) {
            if (target && target->pending) target->pending->cancel();
        }
        for (auto& [id, target] : targets) {
            if (target && target->pending) target->pending->join();
        }
    }

    Store& store;
    const std::chrono::milliseconds interval;
    int rpc_timeout_ms;
    std::unordered_map<std::string, std::unique_ptr<Target>> targets;
    std::atomic<std::int64_t> next_log_ms{0};
    std::mutex wait_mutex;
    std::condition_variable_any timer;
    std::unordered_set<std::string> requested_ids;
    // Destroyed first, joining the thread before it can access any other members.
    std::jthread worker;
};

Monitor::Monitor(Store& store, std::chrono::milliseconds interval, std::chrono::milliseconds rpc_timeout)
    : impl_(std::make_unique<Impl>(store, interval, rpc_timeout)) {}
Monitor::~Monitor() = default;

std::uint32_t Monitor::request_probe(const std::string& server_id) {
    return impl_->request_probe(server_id);
}

} // namespace chunkstore::master
