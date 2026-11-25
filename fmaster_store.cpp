#include "fmaster_store.hpp"

#include <butil/crc32c.h>
#include <butil/endpoint.h>
#include <google/protobuf/text_format.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <mutex>
#include <optional>
#include <set>
#include <string_view>
#include <sys/file.h>
#include <sys/stat.h>
#include <system_error>
#include <unistd.h>
#include <unordered_map>

namespace chunkstore::master {
namespace {
namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;
constexpr std::size_t max_config_bytes = 16 * 1024 * 1024;
constexpr std::string_view snapshot_magic = "FMASTER1";

[[noreturn]] void system_failure(const char* operation) {
    throw std::system_error(errno, std::generic_category(), operation);
}

struct File {
    explicit File(int descriptor) : fd(descriptor) {
        if (fd < 0) system_failure("open master file");
    }
    ~File() { ::close(fd); }
    File(const File&) = delete;
    File& operator=(const File&) = delete;
    int fd;
};

void sync_file(int fd) {
    int result;
    do { result = ::fsync(fd); } while (result != 0 && errno == EINTR);
    if (result != 0) system_failure("sync master file");
}

void sync_directory(const fs::path& directory) {
    File file(::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    sync_file(file.fd);
}

void prepare_directory(const fs::path& directory) {
    std::vector<fs::path> missing;
    for (auto path = fs::absolute(directory); !fs::exists(path); path = path.parent_path()) {
        missing.push_back(path);
    }
    fs::create_directories(directory);
    for (const auto& path : missing) sync_directory(path.parent_path());
}

std::optional<std::string> read_file(const fs::path& path, std::size_t limit, bool optional = false) {
    const int descriptor = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (descriptor < 0 && optional && errno == ENOENT) return std::nullopt;
    File file(descriptor);
    struct stat info{};
    if (::fstat(file.fd, &info) != 0) system_failure("stat master file");
    if (!S_ISREG(info.st_mode) || info.st_size < 0 || static_cast<std::uint64_t>(info.st_size) > limit) {
        throw Error(wire::INVALID_ARGUMENT, "master configuration must be a regular file within 16 MiB");
    }
    std::string contents(static_cast<std::size_t>(info.st_size), '\0');
    std::size_t offset = 0;
    while (offset < contents.size()) {
        const auto count = ::read(file.fd, contents.data() + offset, contents.size() - offset);
        if (count < 0 && errno == EINTR) continue;
        if (count < 0) system_failure("read master file");
        if (count == 0) throw Error(wire::STORAGE_ERROR, "master configuration was truncated while reading");
        offset += static_cast<std::size_t>(count);
    }
    char extra;
    ssize_t count;
    do { count = ::read(file.fd, &extra, 1); } while (count < 0 && errno == EINTR);
    if (count < 0) system_failure("read master file");
    if (count != 0) throw Error(wire::STORAGE_ERROR, "master configuration changed while reading");
    return contents;
}

void validate_id(std::string_view id) {
    if (id.empty() || id.size() > 64 || !std::all_of(id.begin(), id.end(), [](unsigned char c) {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                   (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.';
        })) {
        throw Error(wire::INVALID_ARGUMENT, "IDs must contain 1..64 ASCII letters, digits, dots, underscores or hyphens");
    }
}

std::string endpoint_key(const std::string& endpoint, bool resolve_hostname = true) {
    butil::EndPoint address;
    if (endpoint.empty() || endpoint.size() > 1024 || endpoint.find('\0') != std::string::npos) {
        throw Error(wire::INVALID_ARGUMENT, "invalid FServer endpoint: " + endpoint);
    }
    if (butil::str2endpoint(endpoint.c_str(), &address) == 0 && address.port > 0 && address.port <= 65535) {
        return butil::endpoint2str(address).c_str();
    }
    const auto separator = endpoint.rfind(':');
    const auto host = std::string_view(endpoint).substr(0, separator);
    unsigned port = 0;
    const char* begin = separator == std::string::npos ? endpoint.data() + endpoint.size() : endpoint.data() + separator + 1;
    const auto [end, error] = std::from_chars(begin, endpoint.data() + endpoint.size(), port);
    if (host.empty() || host.size() > 254 || error != std::errc{} || end != endpoint.data() + endpoint.size() ||
        port == 0 || port > 65535 || !std::all_of(host.begin(), host.end(), [](unsigned char c) {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                   (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.';
        })) {
        throw Error(wire::INVALID_ARGUMENT, "invalid FServer endpoint: " + endpoint);
    }
    if (!resolve_hostname) {
        std::string key(host);
        for (auto& c : key) if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
        return key + ":" + std::to_string(port);
    }
    if (butil::hostname2endpoint(endpoint.c_str(), &address) != 0 || address.port <= 0 || address.port > 65535) {
        throw Error(wire::INVALID_ARGUMENT, "invalid FServer endpoint: " + endpoint);
    }
    return butil::endpoint2str(address).c_str();
}

std::string normalized_path(const std::string& value) {
    const fs::path path(value);
    if (value.empty() || value.size() > 4096 || value.find('\0') != std::string::npos || !path.is_absolute()) {
        throw Error(wire::INVALID_ARGUMENT, "disk path must be an absolute path within 4096 bytes");
    }
    auto normalized = path.lexically_normal();
    if (normalized != normalized.root_path() && normalized.filename().empty()) normalized = normalized.parent_path();
    return normalized.native();
}

void normalize(wire::Topology& topology, bool resolve_hostnames = true) {
    if (topology.ByteSizeLong() > max_config_bytes) throw Error(wire::INVALID_ARGUMENT, "configuration exceeds 16 MiB");
    std::set<std::string> servers, endpoints;
    for (auto& server : *topology.mutable_servers()) {
        validate_id(server.id());
        if (!servers.insert(server.id()).second) throw Error(wire::INVALID_ARGUMENT, "duplicate FServer ID: " + server.id());
        if (!endpoints.insert(endpoint_key(server.endpoint(), resolve_hostnames)).second) {
            throw Error(wire::INVALID_ARGUMENT, "duplicate FServer endpoint: " + server.endpoint());
        }
        std::set<std::string> disks, paths;
        for (auto& disk : *server.mutable_disks()) {
            validate_id(disk.id());
            disk.set_path(normalized_path(disk.path()));
            if (!disks.insert(disk.id()).second) throw Error(wire::INVALID_ARGUMENT, "duplicate disk ID: " + disk.id());
            if (!paths.insert(disk.path()).second) throw Error(wire::INVALID_ARGUMENT, "duplicate disk path: " + disk.path());
        }
        std::sort(server.mutable_disks()->begin(), server.mutable_disks()->end(),
                  [](const wire::Disk& a, const wire::Disk& b) { return a.id() < b.id(); });
    }
    std::sort(topology.mutable_servers()->begin(), topology.mutable_servers()->end(),
              [](const wire::FServer& a, const wire::FServer& b) { return a.id() < b.id(); });
}

auto find_server(wire::Topology& topology, std::string_view id) {
    return std::find_if(topology.mutable_servers()->begin(), topology.mutable_servers()->end(),
                        [&](const wire::FServer& server) { return server.id() == id; });
}

auto find_disk(wire::FServer& server, std::string_view id) {
    return std::find_if(server.mutable_disks()->begin(), server.mutable_disks()->end(),
                        [&](const wire::Disk& disk) { return disk.id() == id; });
}

void append_u32(std::string& bytes, std::uint32_t value) {
    for (int shift = 24; shift >= 0; shift -= 8) bytes.push_back(static_cast<char>(value >> shift));
}

std::uint32_t read_u32(std::string_view bytes) {
    std::uint32_t value = 0;
    for (unsigned char byte : bytes) value = (value << 8) | byte;
    return value;
}

std::string encode(const wire::Topology& topology) {
    const auto payload = topology.SerializeAsString();
    if (payload.size() > max_config_bytes) throw Error(wire::INVALID_ARGUMENT, "configuration exceeds 16 MiB");
    std::string bytes(snapshot_magic);
    append_u32(bytes, static_cast<std::uint32_t>(payload.size()));
    append_u32(bytes, butil::crc32c::Value(payload.data(), payload.size()));
    bytes += payload;
    return bytes;
}

wire::Topology decode(const std::string& bytes) {
    if (bytes.size() < 16 || std::string_view(bytes).substr(0, 8) != snapshot_magic ||
        read_u32(std::string_view(bytes).substr(8, 4)) != bytes.size() - 16) {
        throw Error(wire::STORAGE_ERROR, "invalid master snapshot header or length");
    }
    const auto payload = std::string_view(bytes).substr(16);
    if (read_u32(std::string_view(bytes).substr(12, 4)) != butil::crc32c::Value(payload.data(), payload.size())) {
        throw Error(wire::STORAGE_ERROR, "master snapshot checksum mismatch");
    }
    wire::Topology topology;
    if (!topology.ParseFromArray(payload.data(), static_cast<int>(payload.size())) || topology.revision() == 0) {
        throw Error(wire::STORAGE_ERROR, "invalid master snapshot payload");
    }
    // Persisted endpoints were resolved and checked before the snapshot was
    // written. Recovery must also work while the hostname resolver is down.
    normalize(topology, false);
    return topology;
}

std::uint64_t wall_milliseconds() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}
} // namespace

struct Store::Impl {
    struct LiveServer {
        std::optional<Clock::time_point> seen;
        wire::ServerStatus status;
    };

    Impl(const fs::path& directory, const fs::path& bootstrap,
         std::chrono::milliseconds timeout, std::chrono::milliseconds interval)
        : root(directory), heartbeat_timeout(timeout), heartbeat_interval(interval) {
        if (timeout.count() <= 0 || interval.count() <= 0 || interval >= timeout ||
            interval.count() > std::numeric_limits<std::uint32_t>::max()) {
            throw Error(wire::INVALID_ARGUMENT, "heartbeat interval must be positive and below heartbeat timeout");
        }
        prepare_directory(root);
        root = fs::canonical(root);
        directory_file = std::make_unique<File>(::open(root.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
        lock_file = std::make_unique<File>(::open((root / ".lock").c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600));
        if (::flock(lock_file->fd, LOCK_EX | LOCK_NB) != 0) system_failure("lock master data directory");
        const auto saved = read_file(root / "topology.pb", max_config_bytes + 16, true);
        if (saved) {
            topology = decode(*saved);
            // Finish durability for a visible rename left by an interrupted sync.
            File snapshot(::open((root / "topology.pb").c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
            sync_file(snapshot.fd);
            sync_file(directory_file->fd);
        } else {
            if (!bootstrap.empty()) {
                const auto text = read_file(bootstrap, max_config_bytes);
                if (!google::protobuf::TextFormat::ParseFromString(*text, &topology)) {
                    throw Error(wire::INVALID_ARGUMENT, "invalid bootstrap textproto configuration");
                }
            }
            normalize(topology);
            topology.set_revision(1);
            persist(topology);
        }
        for (const auto& server : topology.servers()) changed_revisions.emplace(server.id(), topology.revision());
    }

    void ensure_available() const {
        if (storage_failed) throw Error(wire::STORAGE_ERROR, "master persistence failed; restart before serving configuration");
    }

    wire::FServer& checked_probe(const std::string& id, const std::string& endpoint,
                                std::uint64_t revision) {
        ensure_available();
        validate_id(id);
        const auto server = find_server(topology, id);
        if (server == topology.mutable_servers()->end()) throw Error(wire::NOT_FOUND, "FServer is not configured");
        if (revision < changed_revisions.at(id) || revision > topology.revision()) {
            throw Error(wire::CONFLICT, "probe configuration no longer matches FServer");
        }
        // A monitor captures the configured endpoint before launching. Exact
        // matches also let failed DNS probes record their error without DNS.
        if (endpoint != server->endpoint() && endpoint_key(endpoint) != endpoint_key(server->endpoint())) {
            throw Error(wire::CONFLICT, "probe endpoint does not match configured FServer");
        }
        return *server;
    }

    void persist(const wire::Topology& candidate) {
        const auto bytes = encode(candidate);
        const auto temporary = root / ".topology.tmp";
        try {
            File file(::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW, 0600));
            std::size_t offset = 0;
            while (offset < bytes.size()) {
                const auto count = ::write(file.fd, bytes.data() + offset, bytes.size() - offset);
                if (count < 0 && errno == EINTR) continue;
                if (count < 0) system_failure("write master snapshot");
                if (count == 0) throw Error(wire::STORAGE_ERROR, "master snapshot write made no progress");
                offset += static_cast<std::size_t>(count);
            }
            sync_file(file.fd);
            if (::rename(temporary.c_str(), (root / "topology.pb").c_str()) != 0) system_failure("publish master snapshot");
            sync_file(directory_file->fd);
        } catch (...) {
            storage_failed = true;
            throw;
        }
    }

    template<class Request, class Change>
    std::uint64_t mutate(const Request& request, Change change) {
        std::lock_guard lock(mutex);
        ensure_available();
        if (request.has_expected_revision() && request.expected_revision() != topology.revision()) {
            throw Error(wire::CONFLICT, "configuration revision changed; current revision " + std::to_string(topology.revision()));
        }
        wire::Topology candidate = topology;
        change(candidate);
        // Existing endpoints were validated when registered. State, disk and
        // removal changes must remain usable while a hostname is unavailable.
        const bool endpoints_changed = std::any_of(candidate.servers().begin(), candidate.servers().end(),
            [&](const wire::FServer& server) {
                const auto previous = find_server(topology, server.id());
                return previous == topology.mutable_servers()->end() || previous->endpoint() != server.endpoint();
            });
        normalize(candidate, endpoints_changed);
        candidate.set_revision(topology.revision());
        if (candidate.SerializeAsString() == topology.SerializeAsString()) return topology.revision();
        if (topology.revision() == std::numeric_limits<std::uint64_t>::max()) {
            throw Error(wire::CONFLICT, "configuration revision exhausted");
        }
        candidate.set_revision(topology.revision() + 1);
        // Build the replacement live-state map before publishing on disk, so
        // allocation failures cannot leave an acknowledged snapshot unpublished.
        std::unordered_map<std::string, LiveServer> retained;
        std::unordered_map<std::string, std::uint64_t> next_revisions;
        for (const auto& server : candidate.servers()) {
            const auto previous = find_server(topology, server.id());
            const auto live = live_servers.find(server.id());
            const bool unchanged = previous != topology.mutable_servers()->end() &&
                                   previous->SerializeAsString() == server.SerializeAsString();
            next_revisions.emplace(server.id(), unchanged ? changed_revisions.at(server.id()) : candidate.revision());
            if (unchanged && live != live_servers.end()) {
                retained.emplace(server.id(), live->second);
            }
        }
        persist(candidate);
        topology.Swap(&candidate);
        live_servers.swap(retained);
        changed_revisions.swap(next_revisions);
        return topology.revision();
    }

    fs::path root;
    std::unique_ptr<File> directory_file, lock_file;
    std::chrono::milliseconds heartbeat_timeout, heartbeat_interval;
    mutable std::mutex mutex;
    wire::Topology topology;
    std::unordered_map<std::string, LiveServer> live_servers;
    std::unordered_map<std::string, std::uint64_t> changed_revisions;
    bool storage_failed = false;
};

Store::Store(const fs::path& directory, const fs::path& bootstrap,
             std::chrono::milliseconds heartbeat_timeout, std::chrono::milliseconds heartbeat_interval)
    : impl_(std::make_unique<Impl>(directory, bootstrap, heartbeat_timeout, heartbeat_interval)) {}

Store::~Store() = default;

void Store::list(wire::ListResponse& response, const std::string& server_id) const {
    std::lock_guard lock(impl_->mutex);
    impl_->ensure_available();
    const wire::FServer* selected = nullptr;
    if (!server_id.empty()) {
        validate_id(server_id);
        const auto server = find_server(impl_->topology, server_id);
        if (server == impl_->topology.mutable_servers()->end()) throw Error(wire::NOT_FOUND, "FServer not found");
        selected = &*server;
    }
    response.Clear();
    response.mutable_status();
    if (selected) {
        response.mutable_topology()->set_revision(impl_->topology.revision());
        *response.mutable_topology()->add_servers() = *selected;
    } else {
        *response.mutable_topology() = impl_->topology;
    }
    const auto now = Clock::now();
    for (const auto& server : response.topology().servers()) {
        auto* status = response.add_servers();
        const auto live = impl_->live_servers.find(server.id());
        if (live != impl_->live_servers.end()) {
            *status = live->second.status;
            status->set_online(live->second.seen && now - *live->second.seen < impl_->heartbeat_timeout);
        }
        status->set_server_id(server.id());
        status->clear_disks();
        for (const auto& disk : server.disks()) {
            auto* report = status->add_disks();
            report->set_id(disk.id());
            report->set_path(disk.path());
            if (live == impl_->live_servers.end()) continue;
            const auto& observed = live->second.status.disks();
            const auto previous = std::find_if(observed.begin(), observed.end(), [&](const wire::DiskReport& item) {
                return item.id() == disk.id() && item.path() == disk.path();
            });
            if (previous == observed.end()) continue;
            *report = *previous;
            if (!status->online()) {
                report->set_healthy(false);
                report->set_health(wire::DISK_STALE);
            }
        }
    }
}

std::uint64_t Store::set_server(const wire::SetServerRequest& request) {
    return impl_->mutate(request, [&](wire::Topology& topology) {
        validate_id(request.id());
        const auto existing = find_server(topology, request.id());
        auto* server = existing == topology.mutable_servers()->end() ? topology.add_servers() : &*existing;
        server->set_id(request.id());
        server->set_endpoint(request.endpoint());
        server->set_disabled(request.disabled());
    });
}

std::uint64_t Store::set_disk(const wire::SetDiskRequest& request) {
    return impl_->mutate(request, [&](wire::Topology& topology) {
        validate_id(request.server_id());
        if (!request.has_disk()) throw Error(wire::INVALID_ARGUMENT, "disk configuration is required");
        const auto server = find_server(topology, request.server_id());
        if (server == topology.mutable_servers()->end()) throw Error(wire::NOT_FOUND, "FServer not found");
        const auto existing = find_disk(*server, request.disk().id());
        auto* disk = existing == server->mutable_disks()->end() ? server->add_disks() : &*existing;
        *disk = request.disk();
    });
}

std::uint64_t Store::set_server_state(const wire::SetServerStateRequest& request) {
    return impl_->mutate(request, [&](wire::Topology& topology) {
        validate_id(request.id());
        const auto server = find_server(topology, request.id());
        if (server == topology.mutable_servers()->end()) throw Error(wire::NOT_FOUND, "FServer not found");
        server->set_disabled(request.disabled());
    });
}

std::uint64_t Store::set_disk_state(const wire::SetDiskStateRequest& request) {
    return impl_->mutate(request, [&](wire::Topology& topology) {
        validate_id(request.server_id());
        validate_id(request.disk_id());
        const auto server = find_server(topology, request.server_id());
        if (server == topology.mutable_servers()->end()) throw Error(wire::NOT_FOUND, "FServer not found");
        const auto disk = find_disk(*server, request.disk_id());
        if (disk == server->mutable_disks()->end()) throw Error(wire::NOT_FOUND, "disk not found");
        disk->set_disabled(request.disabled());
    });
}

std::uint64_t Store::remove_server(const wire::RemoveServerRequest& request) {
    return impl_->mutate(request, [&](wire::Topology& topology) {
        validate_id(request.id());
        const auto server = find_server(topology, request.id());
        if (server != topology.mutable_servers()->end()) topology.mutable_servers()->erase(server);
    });
}

std::uint64_t Store::remove_disk(const wire::RemoveDiskRequest& request) {
    return impl_->mutate(request, [&](wire::Topology& topology) {
        validate_id(request.server_id());
        validate_id(request.disk_id());
        const auto server = find_server(topology, request.server_id());
        if (server == topology.mutable_servers()->end()) throw Error(wire::NOT_FOUND, "FServer not found");
        const auto disk = find_disk(*server, request.disk_id());
        if (disk != server->mutable_disks()->end()) server->mutable_disks()->erase(disk);
    });
}

std::uint64_t Store::apply_config(const wire::ApplyConfigRequest& request) {
    return impl_->mutate(request, [&](wire::Topology& topology) {
        if (!request.has_topology()) throw Error(wire::INVALID_ARGUMENT, "topology is required");
        topology = request.topology();
    });
}

void Store::validate_config(const wire::ValidateConfigRequest& request, wire::ValidateConfigResponse& response) const {
    if (!request.has_topology()) throw Error(wire::INVALID_ARGUMENT, "topology is required");
    auto topology = request.topology();
    normalize(topology);
    topology.set_revision(0);
    response.Clear();
    response.mutable_status();
    response.mutable_topology()->Swap(&topology);
}

void Store::record_heartbeat(const wire::HeartbeatReport& request) {
    std::lock_guard lock(impl_->mutex);
    auto& server = impl_->checked_probe(request.server_id(), request.endpoint(), request.config_revision());
    if (request.ByteSizeLong() > max_config_bytes) throw Error(wire::INVALID_ARGUMENT, "heartbeat exceeds 16 MiB");
    if (request.disks_size() != server.disks_size()) {
        throw Error(wire::INVALID_ARGUMENT, "heartbeat must report every configured disk exactly once");
    }
    Impl::LiveServer next;
    const auto observed_at = wall_milliseconds();
    std::set<std::string> reported;
    for (const auto& report : request.disks()) {
        const auto disk = find_disk(server, report.id());
        if (disk == server.mutable_disks()->end() || report.path() != disk->path() ||
            !reported.insert(report.id()).second || report.error().size() > 4096 ||
            report.available_bytes() > report.free_bytes() || report.free_bytes() > report.total_bytes()) {
            throw Error(wire::INVALID_ARGUMENT, "disk report does not match configured disk or capacity bounds");
        }
        auto* observed = next.status.add_disks();
        *observed = report;
        observed->set_health(report.healthy() ? wire::DISK_HEALTHY : wire::DISK_ERROR);
        observed->set_observed_unix_ms(observed_at);
        if (report.healthy()) observed->clear_error();
    }
    next.seen = Clock::now();
    next.status.set_server_id(server.id());
    next.status.set_online(true);
    next.status.set_last_heartbeat_unix_ms(observed_at);
    next.status.set_last_probe_unix_ms(observed_at);
    impl_->live_servers.insert_or_assign(server.id(), std::move(next));
}

void Store::record_probe_failure(const std::string& server_id, const std::string& endpoint,
                                 std::uint64_t config_revision, const std::string& error) {
    std::lock_guard lock(impl_->mutex);
    impl_->checked_probe(server_id, endpoint, config_revision);
    Impl::LiveServer next;
    const auto existing = impl_->live_servers.find(server_id);
    if (existing != impl_->live_servers.end()) next = existing->second;
    next.status.set_server_id(server_id);
    next.status.set_last_probe_unix_ms(wall_milliseconds());
    next.status.set_error(error.empty() ? "FServer probe failed" : error.substr(0, 4096));
    impl_->live_servers.insert_or_assign(server_id, std::move(next));
}

} // namespace chunkstore::master
