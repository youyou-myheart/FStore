#include "fmaster_recovery.hpp"
#include "chunk.pb.h"
#include "chunk_wire_state.hpp"
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <algorithm>
#include <climits>
#include <condition_variable>
#include <cstdio>
#include <mutex>
#include <thread>
#include <vector>

namespace chunkstore::master {
namespace data = ::chunkstore::rpc::v1;
namespace {
bool contains(const wire::PlacementGroup& pg, const std::string& id) {
    return std::any_of(pg.replicas().begin(), pg.replicas().end(), [&](const auto& node) { return node.server_id() == id; });
}
std::uint64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}
void equal_inventory(const data::PGInventoryResponse& a, const data::PGInventoryResponse& b, bool require_closed = true,
                     bool digest = false, bool match_reason = false) {
    if (a.chunk_count() != b.chunk_count() || a.chunks_size() != b.chunks_size() ||
        a.has_next_chunk() != b.has_next_chunk() || a.next_chunk() != b.next_chunk()) throw std::runtime_error("PG inventories disagree; automatic recovery is blocked");
    for (int i = 0; i < a.chunks_size(); ++i) {
        if (a.chunks(i).chunk_id() != b.chunks(i).chunk_id()) throw std::runtime_error("PG Chunk membership differs between replicas");
        const auto left = detail::wire_state(a.chunks(i)), right = detail::wire_state(b.chunks(i));
        if (require_closed && (!is_closed(left) || !is_closed(right))) throw std::runtime_error("PG contains an unsealed chunk");
        if (match_reason && left != right) throw std::runtime_error("copied chunk close reason differs from source");
        if (digest && (a.chunks(i).length() != b.chunks(i).length() || a.chunks(i).capacity() != b.chunks(i).capacity() ||
                       a.chunks(i).crc32c() != b.chunks(i).crc32c())) throw std::runtime_error("PG chunk contents differ between surviving replicas; recovery is blocked");
    }
}
}
struct RecoveryMonitor::Impl {
    Store& store;
    std::chrono::milliseconds grace, interval, timeout;
    std::jthread thread;
    Impl(Store& value, std::chrono::milliseconds delay, std::chrono::milliseconds period, std::chrono::milliseconds rpc_timeout)
        : store(value), grace(delay), interval(period), timeout(rpc_timeout) {
        if (grace.count() <= 0 || interval.count() <= 0 || timeout.count() <= 0 || timeout.count() > INT_MAX) throw std::invalid_argument("invalid recovery timeouts");
        thread = std::jthread([this](std::stop_token stop) { run(stop); });
    }
    template<class Request, class Response>
    Response call(const std::string& endpoint, const Request& request,
                  void (data::ChunkService_Stub::*method)(google::protobuf::RpcController*, const Request*, Response*, google::protobuf::Closure*),
                  std::stop_token stop, bool copying = false) {
        if (stop.stop_requested()) throw std::runtime_error("recovery monitor stopping");
        brpc::Channel channel; brpc::ChannelOptions options;
        options.protocol = brpc::PROTOCOL_BAIDU_STD; options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
        options.max_retry = 0; options.connect_timeout_ms = 1000;
        options.timeout_ms = copying ? static_cast<int>(timeout.count()) : std::min(5000, static_cast<int>(timeout.count()));
        if (channel.Init(endpoint.c_str(), &options) != 0) throw std::runtime_error("invalid recovery endpoint");
        data::ChunkService_Stub stub(&channel); brpc::Controller controller; Response response;
        const auto id = controller.call_id(); std::stop_callback cancel(stop, [id] { brpc::StartCancel(id); });
        (stub.*method)(&controller, &request, &response, nullptr);
        if (controller.Failed()) throw std::runtime_error(endpoint + ": " + controller.ErrorText());
        if (!response.has_status() || response.status().code() != data::OK) throw std::runtime_error(endpoint + ": " + response.status().message());
        return response;
    }
    void fence(const wire::PGRecovery& job, const wire::ReplicaTarget& node, data::PGFenceMode mode, std::stop_token stop) {
        data::PGFenceRequest request; request.set_pg_id(job.pg_id()); request.set_recovery_token(job.id());
        request.set_expected_epoch(contains(job.old_pg(), node.server_id()) ? job.old_pg().epoch() : 0);
        if (mode == data::PG_TARGET) {
            // A spare may have hosted this PG in an earlier, completed recovery.
            data::PGInventoryRequest query; query.set_pg_id(job.pg_id()); query.set_limit(1);
            const auto current = call(node.endpoint(), query, &data::ChunkService_Stub::GetPGInventory, stop);
            request.set_expected_epoch(current.pg_epoch());
        }
        request.set_mode(mode); call(node.endpoint(), request, &data::ChunkService_Stub::FencePG, stop);
    }
    data::PGInventoryResponse inventory(const wire::PGRecovery& job, const wire::ReplicaTarget& node, std::stop_token stop) {
        data::PGInventoryRequest request; request.set_pg_id(job.pg_id()); request.set_after_chunk(job.cursor()); request.set_limit(16);
        auto result = call(node.endpoint(), request, &data::ChunkService_Stub::GetPGInventory, stop);
        if (result.recovery_token() != job.id() || (result.mode() != data::PG_SOURCE && result.mode() != data::PG_TARGET)) {
            throw std::runtime_error("PG recovery fence changed during transfer");
        }
        return result;
    }
    data::PGInventoryResponse seal(const wire::PGRecovery& job, const wire::ReplicaTarget& node, bool inspect, std::stop_token stop) {
        data::SealPGRequest request; request.set_pg_id(job.pg_id()); request.set_recovery_token(job.id());
        request.set_after_chunk(job.cursor()); request.set_limit(16); request.set_inspect_only(inspect);
        auto result = call(node.endpoint(), request, &data::ChunkService_Stub::SealPG, stop, true);
        if (result.recovery_token() != job.id() || result.mode() != data::PG_SOURCE) throw std::runtime_error("PG sealing fence changed");
        return result;
    }
    void step(const wire::PGRecovery& job, std::stop_token stop) {
        std::vector<wire::ReplicaTarget> sources, targets;
        for (const auto& node : job.new_pg().replicas()) {
            (contains(job.old_pg(), node.server_id()) ? sources : targets).push_back(node);
        }
        if (sources.empty() || targets.empty()) throw std::runtime_error("invalid PG recovery membership");
        if (job.phase() == wire::PREPARING) {
            for (const auto& node : sources) fence(job, node, data::PG_SOURCE, stop);
            for (const auto& node : targets) fence(job, node, data::PG_TARGET, stop);
            store.advance_recovery(job.id(), job.phase(), wire::SEALING);
        } else if (job.phase() == wire::SEALING) {
            // All sources have stopped appends and namespace mutations. Compare
            // stable bytes before making an open replica visible as sealed.
            const auto canonical = seal(job, sources.front(), true, stop);
            for (std::size_t i = 1; i < sources.size(); ++i) equal_inventory(canonical, seal(job, sources[i], true, stop), false, true);
            for (const auto& source : sources) {
                const auto sealed = seal(job, source, false, stop);
                equal_inventory(canonical, sealed, false); equal_inventory(sealed, sealed);
            }
            store.advance_recovery(job.id(), job.phase(), canonical.has_next_chunk() ? wire::SEALING : wire::COPYING,
                                   canonical.has_next_chunk() ? canonical.next_chunk() : "");
        } else if (job.phase() == wire::COPYING || job.phase() == wire::VERIFYING) {
            auto canonical = inventory(job, sources.front(), stop);
            equal_inventory(canonical, canonical); // A single surviving source must also be immutable.
            for (std::size_t i = 1; i < sources.size(); ++i) equal_inventory(canonical, inventory(job, sources[i], stop));
            std::uint64_t chunks = 0, bytes = 0;
            for (const auto& target : targets) {
                if (job.phase() == wire::COPYING) {
                    data::RepairPGRequest request; request.set_pg_id(job.pg_id()); request.set_recovery_token(job.id());
                    request.set_destination(target.endpoint()); request.set_after_chunk(job.cursor()); request.set_limit(16);
                    auto result = call(sources.front().endpoint(), request, &data::ChunkService_Stub::RepairPG, stop, true);
                    if (result.copied_chunks() != static_cast<unsigned>(canonical.chunks_size()) || result.has_next_chunk() != canonical.has_next_chunk() ||
                        result.next_chunk() != canonical.next_chunk()) throw std::runtime_error("invalid PG copy acknowledgement");
                    chunks += result.copied_chunks(); bytes += result.copied_bytes();
                } else equal_inventory(canonical, inventory(job, target, stop), true, false, true);
            }
            const auto next = canonical.has_next_chunk() ? job.phase() :
                job.phase() == wire::COPYING ? wire::VERIFYING : wire::ACTIVATING;
            store.advance_recovery(job.id(), job.phase(), next, canonical.has_next_chunk() ? canonical.next_chunk() : "", chunks, bytes);
        } else if (job.phase() == wire::ACTIVATING) {
            for (const auto& node : job.new_pg().replicas()) fence(job, node, data::PG_ACTIVE, stop);
            store.advance_recovery(job.id(), job.phase(), wire::RECOVERED);
        }
    }
    void run(std::stop_token stop) {
        std::mutex mutex; std::condition_variable_any timer; std::unique_lock lock(mutex);
        while (!stop.stop_requested()) {
            try {
                store.plan_recoveries(now_ms(), grace.count());
                wire::ListRecoveriesRequest request; request.set_active_only(true); request.set_limit(1000);
                wire::ListRecoveriesResponse response; store.list_recoveries(request, response);
                for (const auto& job : response.recoveries()) {
                    if (stop.stop_requested()) break;
                    try { step(job, stop); }
                    catch (const std::exception& error) { if (!stop.stop_requested()) store.recovery_error(job.id(), error.what()); }
                }
            } catch (const std::exception& error) { std::fprintf(stderr, "FMaster recovery: %s\n", error.what()); }
            timer.wait_for(lock, stop, interval, [] { return false; });
        }
    }
};
RecoveryMonitor::RecoveryMonitor(Store& store, std::chrono::milliseconds grace, std::chrono::milliseconds interval, std::chrono::milliseconds timeout)
    : impl_(std::make_unique<Impl>(store, grace, interval, timeout)) {}
RecoveryMonitor::~RecoveryMonitor() = default;
}
