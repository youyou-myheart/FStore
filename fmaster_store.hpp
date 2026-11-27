#pragma once

#include "fmaster.pb.h"

#include <chrono>
#include <filesystem>
#include <memory>
#include <stdexcept>

namespace chunkstore::master {
namespace wire = ::fstore::master::v1;

class Error : public std::runtime_error {
public:
    Error(wire::Code code, const std::string& message) : std::runtime_error(message), code_(code) {}
    wire::Code code() const noexcept { return code_; }
private:
    wire::Code code_;
};

// Bootstrap is used only when the data directory has no persisted configuration.
// Heartbeat state is transient and starts unknown after a master restart.
class Store {
public:
    Store(const std::filesystem::path& directory, const std::filesystem::path& bootstrap = {},
          std::chrono::milliseconds heartbeat_timeout = std::chrono::seconds(10),
          std::chrono::milliseconds heartbeat_interval = std::chrono::seconds(2));
    ~Store();
    Store(const Store&) = delete;
    Store& operator=(const Store&) = delete;

    void list(wire::ListResponse& response, const std::string& server_id = {}) const;
    std::uint64_t set_server(const wire::SetServerRequest& request);
    std::uint64_t set_disk(const wire::SetDiskRequest& request);
    std::uint64_t set_server_state(const wire::SetServerStateRequest& request);
    std::uint64_t set_disk_state(const wire::SetDiskStateRequest& request);
    std::uint64_t remove_server(const wire::RemoveServerRequest& request);
    std::uint64_t remove_disk(const wire::RemoveDiskRequest& request);
    std::uint64_t apply_config(const wire::ApplyConfigRequest& request);
    void validate_config(const wire::ValidateConfigRequest& request, wire::ValidateConfigResponse& response) const;
    void create_space(const wire::CreateSpaceRequest&, wire::SpaceResponse&);
    void bind_pgs(const wire::BindPGsRequest&, wire::SpaceResponse&);
    void unbind_pgs(const wire::BindPGsRequest&, wire::SpaceResponse&);
    void list_pg_pool(const wire::ListPGPoolRequest&, wire::ListPGPoolResponse&) const;
    void list_spaces(const wire::ListSpacesRequest&, wire::ListSpacesResponse&) const;
    void list_pgs(const wire::ListPGsRequest&, wire::ListPGsResponse&) const;
    void allocate_chunks(const wire::AllocateChunksRequest&, wire::AllocateChunksResponse&);
    void allocate_blobs(const wire::AllocateBlobsRequest&, wire::AllocateBlobsResponse&);
    void locate_chunk(const wire::LocateChunkRequest&, wire::LocateChunkResponse&) const;
    void list_recoveries(const wire::ListRecoveriesRequest&, wire::ListRecoveriesResponse&) const;
    void plan_recoveries(std::uint64_t now_ms, std::uint64_t grace_ms = 600000, std::uint32_t scan_limit = 256);
    void advance_recovery(std::uint64_t id, wire::RecoveryPhase expected, wire::RecoveryPhase next,
                          const std::string& cursor = {}, std::uint64_t chunks = 0, std::uint64_t bytes = 0);
    void recovery_error(std::uint64_t id, const std::string& error);
    void retirement_directives(const std::string& server, wire::ServerHeartbeatRequest&) const;
    void record_heartbeat(const wire::HeartbeatReport& report);
    void record_probe_failure(const std::string& server_id, const std::string& endpoint,
                              std::uint64_t config_revision, const std::string& error);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace chunkstore::master
