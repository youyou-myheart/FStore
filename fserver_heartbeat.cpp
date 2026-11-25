#include "fserver_heartbeat.hpp"

#include <brpc/closure_guard.h>
#include <butil/endpoint.h>

#include <algorithm>
#include <cerrno>
#include <filesystem>
#include <fcntl.h>
#include <limits>
#include <set>
#include <stdexcept>
#include <system_error>
#include <sys/statvfs.h>
#include <unistd.h>

namespace chunkstore {
namespace {
namespace wire = ::fstore::master::v1;

void validate_id(const std::string& id) {
    if (id.empty() || id.size() > 64 ||
        id.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-") !=
            std::string::npos) {
        throw std::invalid_argument("IDs must contain 1..64 letters, digits, dots, underscores or hyphens");
    }
}

std::string advertised_endpoint(const std::string& endpoint) {
    butil::EndPoint address;
    if (endpoint.empty() || endpoint.size() > 1024 || endpoint.find('\0') != std::string::npos ||
        (butil::str2endpoint(endpoint.c_str(), &address) != 0 &&
         butil::hostname2endpoint(endpoint.c_str(), &address) != 0) || address.port <= 0) {
        throw std::invalid_argument("invalid FServer advertised endpoint");
    }
    if (address.ip == butil::IP_ANY) {
        throw std::invalid_argument("wildcard FServer listen addresses require --advertise=HOST:PORT with a reachable address");
    }
    return butil::endpoint2str(address).c_str();
}

void validate_disks(const wire::FServer& server) {
    std::set<std::string> ids, paths;
    for (const auto& disk : server.disks()) {
        validate_id(disk.id());
        if (!ids.insert(disk.id()).second) throw std::invalid_argument("duplicate disk ID");
        if (disk.path().empty() || disk.path().size() > 4096 ||
            disk.path().find('\0') != std::string::npos || !std::filesystem::path(disk.path()).is_absolute()) {
            throw std::invalid_argument("disk paths must be absolute and within 4096 bytes");
        }
        auto path = std::filesystem::path(disk.path()).lexically_normal();
        if (path != path.root_path() && path.filename().empty()) path = path.parent_path();
        if (!paths.insert(path.native()).second) throw std::invalid_argument("duplicate disk path");
    }
}

void probe_disk(const wire::Disk& disk, wire::DiskReport& report) {
    report.set_id(disk.id());
    report.set_path(disk.path());
    report.set_health(wire::DISK_ERROR);
    const int fd = ::open(disk.path().c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) {
        report.set_error("open directory: " + std::system_category().message(errno));
        return;
    }
    struct statvfs info{};
    const int result = ::fstatvfs(fd, &info);
    const int error = errno;
    ::close(fd);
    if (result < 0) {
        report.set_error("read disk capacity: " + std::system_category().message(error));
        return;
    }
    const std::uint64_t unit = info.f_frsize ? info.f_frsize : info.f_bsize;
    const std::uint64_t blocks = info.f_blocks;
    if (unit == 0 || blocks > std::numeric_limits<std::uint64_t>::max() / unit) {
        report.set_error("disk capacity cannot be represented in bytes");
        return;
    }
    const auto free_blocks = std::min<std::uint64_t>(info.f_bfree, blocks);
    const auto available_blocks = std::min<std::uint64_t>(info.f_bavail, free_blocks);
    report.set_total_bytes(blocks * unit);
    report.set_free_bytes(free_blocks * unit);
    report.set_available_bytes(available_blocks * unit);
    report.set_healthy(true);
    report.set_health(wire::DISK_HEALTHY);
}
}

FServerStatusService::FServerStatusService(const std::string& server_id, const std::string& endpoint)
    : server_id_(server_id), endpoint_(advertised_endpoint(endpoint)) {
    validate_id(server_id_);
}

void FServerStatusService::Heartbeat(google::protobuf::RpcController*,
                                    const wire::ServerHeartbeatRequest* request,
                                    wire::ServerHeartbeatResponse* response,
                                    google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    auto* status = response->mutable_status();
    try {
        if (request->ByteSizeLong() > 16 * 1024 * 1024 || !request->has_server()) {
            throw std::invalid_argument("heartbeat requires a server configuration within 16 MiB");
        }
        validate_id(request->server().id());
        const auto endpoint = advertised_endpoint(request->server().endpoint());
        if (request->server().id() != server_id_ || endpoint != endpoint_) {
            status->set_code(wire::CONFLICT);
            status->set_message("heartbeat server identity does not match this FServer");
            return;
        }
        validate_disks(request->server());
        wire::HeartbeatReport report;
        report.set_server_id(server_id_);
        report.set_endpoint(endpoint_);
        report.set_config_revision(request->config_revision());
        for (const auto& disk : request->server().disks()) probe_disk(disk, *report.add_disks());
        response->mutable_report()->Swap(&report);
        status->set_code(wire::OK);
    } catch (const std::invalid_argument& error) {
        status->set_code(wire::INVALID_ARGUMENT);
        status->set_message(error.what());
    } catch (const std::exception& error) {
        status->set_code(wire::STORAGE_ERROR);
        status->set_message(error.what());
    }
}

} // namespace chunkstore
