#pragma once
#include "fmaster_store.hpp"

namespace fmaster_test {
namespace master = chunkstore::master;
namespace wire = fstore::master::v1;
inline void node(master::Store& store, unsigned id, const std::string& dc) {
    wire::SetServerRequest server;
    server.set_id("s" + std::to_string(id));
    server.set_endpoint("127.0.0.1:" + std::to_string(8100 + id));
    server.set_datacenter(dc);
    store.set_server(server);
    wire::SetDiskRequest disk;
    disk.set_server_id(server.id());
    disk.mutable_disk()->set_id("d");
    disk.mutable_disk()->set_path("/data/" + server.id());
    const auto revision = store.set_disk(disk);
    wire::HeartbeatReport report;
    report.set_server_id(server.id());
    report.set_endpoint(server.endpoint());
    report.set_config_revision(revision);
    auto* observed = report.add_disks();
    observed->set_id("d");
    observed->set_path(disk.disk().path());
    observed->set_healthy(true);
    observed->set_total_bytes(1000000);
    observed->set_free_bytes(900000);
    observed->set_available_bytes(800000);
    store.record_heartbeat(report);
}

inline wire::CreateSpaceRequest creation(const std::string& name, wire::PlacementPolicy::Mode mode) {
    wire::CreateSpaceRequest request;
    request.set_name(name);
    request.mutable_spec()->set_replicas(3);
    request.mutable_spec()->set_chunk_size(65536);
    request.mutable_spec()->mutable_placement_policy()->set_mode(mode);
    return request;
}

inline wire::ListPGsResponse groups(master::Store& store, const std::string& name, unsigned start, unsigned count) {
    wire::ListPGsRequest request;
    request.set_name(name); request.set_start(start); request.set_limit(count); request.set_include_inactive(true);
    wire::ListPGsResponse result;
    store.list_pgs(request, result);
    return result;
}

inline std::uint64_t blob(master::Store& store, const std::string& name) {
    wire::AllocateBlobsRequest request;
    request.set_name(name);
    wire::AllocateBlobsResponse response;
    store.allocate_blobs(request, response);
    return response.blobs(0).seq_id();
}

inline wire::AllocateChunksResponse chunks(master::Store& store, const std::string& name,
                                    std::uint64_t sequence, std::uint64_t start, unsigned count) {
    wire::AllocateChunksRequest request;
    request.set_name(name); request.set_count(count); request.set_seq_id(sequence); request.set_start_cursor(start);
    wire::AllocateChunksResponse result;
    store.allocate_chunks(request, result);
    return result;
}

inline wire::AllocateChunksResponse allocate(master::Store& store, const std::string& name, unsigned count) {
    return chunks(store, name, blob(store, name), 0, count);
}

} // namespace fmaster_test
