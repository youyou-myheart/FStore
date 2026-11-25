#pragma once

#include "fmaster.pb.h"

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>

namespace chunkstore::master {
namespace wire = ::fstore::master::v1;

class RpcError : public std::runtime_error {
public:
    RpcError(int code, const std::string& message) : std::runtime_error(message), code_(code) {}
    int code() const noexcept { return code_; }
private:
    int code_;
};

class RemoteError : public std::runtime_error {
public:
    RemoteError(wire::Code code, const std::string& message) : std::runtime_error(message), code_(code) {}
    wire::Code code() const noexcept { return code_; }
private:
    wire::Code code_;
};

// Share one client across threads. Finish calls before destroying it.
class Client {
public:
    explicit Client(const std::string& endpoint, int connect_timeout_ms = 1000, int timeout_ms = 5000);
    ~Client();
    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    wire::ListResponse list(const std::string& server_id = {}) const;
    std::uint64_t set_server(const wire::SetServerRequest& request) const;
    std::uint64_t set_disk(const wire::SetDiskRequest& request) const;
    std::uint64_t set_server_state(const wire::SetServerStateRequest& request) const;
    std::uint64_t set_disk_state(const wire::SetDiskStateRequest& request) const;
    std::uint64_t remove_server(const wire::RemoveServerRequest& request) const;
    std::uint64_t remove_disk(const wire::RemoveDiskRequest& request) const;
    std::uint64_t apply_config(const wire::ApplyConfigRequest& request) const;
    wire::ValidateConfigResponse validate_config(const wire::ValidateConfigRequest& request) const;
    wire::ProbeNowResponse probe_now(const std::string& server_id = {}) const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace chunkstore::master
