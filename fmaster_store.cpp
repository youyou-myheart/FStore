#include "fmaster_store.hpp"
#include "fmaster_placement.hpp"
#include "chunk_id.hpp"

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
#include <map>
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
        if (!server.datacenter().empty()) validate_id(server.datacenter());
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
        const auto spaces = read_file(root / "spaces.pb", max_config_bytes + 16, true);
        if (spaces) {
            const auto& bytes = *spaces;
            if (bytes.size() < 16 || (bytes.substr(0, 8) != "FSPACE01" && bytes.substr(0, 8) != "FSPACE02" &&
                                     bytes.substr(0, 8) != "FSPACE03" && bytes.substr(0, 8) != "FSPACE04") ||
                read_u32(std::string_view(bytes).substr(8, 4)) != bytes.size() - 16 ||
                read_u32(std::string_view(bytes).substr(12, 4)) != butil::crc32c::Value(bytes.data() + 16, bytes.size() - 16) ||
                !catalog.ParseFromArray(bytes.data() + 16, static_cast<int>(bytes.size() - 16))) {
                throw Error(wire::STORAGE_ERROR, "invalid space catalog snapshot");
            }
            if (bytes.substr(0, 8) == "FSPACE01" || bytes.substr(0, 8) == "FSPACE02") {
                if (!catalog.spaces().empty() || catalog.reserved_blob_sequence()) {
                    throw Error(wire::STORAGE_ERROR, "legacy Space/PG and Chunk ID layouts require explicit migration");
                }
                catalog.set_next_space_id(0);
            }
            placement::validate_catalog(catalog);
            placement::validate_topology(catalog, topology);
            if (bytes.substr(0, 8) != "FSPACE04") persist_catalog(catalog);
            File snapshot(::open((root / "spaces.pb").c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
            sync_file(snapshot.fd);
            sync_file(directory_file->fd);
        } else {
            catalog.set_revision(1);
            catalog.set_next_space_id(0);
            persist_catalog(catalog);
        }
        for (const auto& state : catalog.spaces()) {
            // Unused reserved IDs are deliberately skipped after every restart.
            cursors.emplace(state.space().id(), state.blob_sequences().empty() ? 0 : state.blob_sequences().rbegin()->end());
        }
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
        placement::validate_topology(catalog, candidate);
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

    void persist_catalog(const wire::SpaceCatalog& candidate) {
        const auto payload = candidate.SerializeAsString();
        if (payload.size() > max_config_bytes) throw Error(wire::INVALID_ARGUMENT, "space catalog exceeds 16 MiB");
        // Older masters must reject recovery routes instead of silently ignoring them.
        std::string bytes = "FSPACE04";
        append_u32(bytes, static_cast<std::uint32_t>(payload.size()));
        append_u32(bytes, butil::crc32c::Value(payload.data(), payload.size()));
        bytes += payload;
        try {
            File file(::open((root / ".spaces.tmp").c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW, 0600));
            std::size_t offset = 0;
            while (offset < bytes.size()) {
                const auto count = ::write(file.fd, bytes.data() + offset, bytes.size() - offset);
                if (count < 0 && errno == EINTR) continue;
                if (count < 0) system_failure("write space catalog");
                if (count == 0) throw Error(wire::STORAGE_ERROR, "space catalog write made no progress");
                offset += static_cast<std::size_t>(count);
            }
            sync_file(file.fd);
            if (::rename((root / ".spaces.tmp").c_str(), (root / "spaces.pb").c_str()) != 0) system_failure("publish space catalog");
            sync_file(directory_file->fd);
        } catch (...) {
            storage_failed = true;
            throw;
        }
    }

    template<class Request>
    void check_space_revision(const Request& request) const {
        ensure_available();
        if (request.has_expected_revision() && request.expected_revision() != catalog.revision()) {
            throw Error(wire::CONFLICT, "space catalog revision changed; current revision " + std::to_string(catalog.revision()));
        }
    }

    void publish_catalog(wire::SpaceCatalog& candidate) {
        if (catalog.revision() == UINT64_MAX) throw Error(wire::CONFLICT, "space revision exhausted");
        candidate.set_revision(catalog.revision() + 1);
        placement::validate_catalog(candidate);
        persist_catalog(candidate);
        catalog.Swap(&candidate);
    }

    wire::SpaceState& find_space(const std::string& name) {
        ensure_available();
        validate_id(name);
        for (auto& state : *catalog.mutable_spaces()) if (state.space().name() == name) return state;
        throw Error(wire::NOT_FOUND, "space not found: " + name);
    }

    bool eligible(const wire::FServer& server) const {
        if (server.disabled()) return false;
        const auto live = live_servers.find(server.id());
        if (live == live_servers.end() || !live->second.seen ||
            Clock::now() - *live->second.seen >= heartbeat_timeout || !live->second.status.error().empty()) return false;
        for (const auto& disk : server.disks()) {
            if (disk.disabled()) continue;
            for (const auto& report : live->second.status.disks()) {
                if (disk.id() == report.id() && disk.path() == report.path() && report.healthy()) return true;
            }
        }
        return false;
    }

    static void compact_ranges(wire::SpaceState& state) {
        std::vector<wire::PGRange> ranges(state.ranges().begin(), state.ranges().end());
        std::sort(ranges.begin(), ranges.end(), [](const auto& a, const auto& b) { return a.begin() < b.begin(); });
        state.clear_ranges();
        std::uint32_t bound = 0;
        for (const auto& range : ranges) {
            bound += range.end() - range.begin();
            if (!state.ranges().empty() && state.ranges().rbegin()->end() == range.begin()) {
                auto* previous = state.mutable_ranges(state.ranges_size() - 1);
                auto comparable = *previous;
                comparable.set_begin(range.begin());
                comparable.set_end(range.end());
                if (comparable.SerializeAsString() == range.SerializeAsString()) {
                    previous->set_end(range.end());
                    continue;
                }
            }
            *state.add_ranges() = range;
        }
        state.mutable_space()->set_bound_pgs(bound);
    }

    void bind_range(wire::SpaceState& state, std::uint32_t start, std::uint32_t count) const {
        if (!count || start >= placement::pg_count || count > placement::pg_count - start) {
            throw Error(wire::INVALID_ARGUMENT, "PG interval must be nonempty and within 0..9999999");
        }
        const auto end = start + count;
        for (const auto& owner : catalog.spaces()) {
            if (owner.space().id() == state.space().id()) continue;
            for (const auto& range : owner.ranges()) {
                if (range.begin() < end && start < range.end()) {
                    throw Error(wire::CONFLICT, "global PG already belongs to Space " + owner.space().name());
                }
            }
        }
        std::vector<std::pair<std::uint32_t, std::uint32_t>> gaps;
        auto cursor = start;
        for (const auto& range : state.ranges()) {
            if (range.end() <= cursor) continue;
            if (range.begin() >= end) break;
            if (range.begin() > cursor) gaps.emplace_back(cursor, range.begin());
            cursor = std::max(cursor, range.end());
            if (cursor >= end) break;
        }
        if (cursor < end) gaps.emplace_back(cursor, end);
        if (gaps.empty()) return;
        wire::PGRange pool;
        pool.set_algorithm_version(1);
        const auto& policy = state.space().spec().placement_policy();
        for (const auto& server : topology.servers()) {
            if (!eligible(server) || (policy.mode() != wire::PlacementPolicy::SINGLE_DATACENTER && server.datacenter().empty())) continue;
            if (!policy.datacenters().empty() &&
                std::find(policy.datacenters().begin(), policy.datacenters().end(), server.datacenter()) == policy.datacenters().end()) continue;
            auto* node = pool.add_candidates();
            node->set_server_id(server.id());
            node->set_endpoint(server.endpoint());
            node->set_datacenter(server.datacenter());
        }
        for (const auto& [begin, finish] : gaps) {
            pool.set_begin(begin); pool.set_end(finish);
            *state.add_ranges() = pool;
        }
        compact_ranges(state);
        for (const auto& gap : gaps) placement::group(state, gap.first);
    }

    void bind_initial(wire::SpaceState& state) const {
        std::vector<std::pair<std::uint32_t, std::uint32_t>> used;
        for (const auto& owner : catalog.spaces()) {
            for (const auto& range : owner.ranges()) used.emplace_back(range.begin(), range.end());
        }
        std::sort(used.begin(), used.end());
        used.emplace_back(placement::pg_count, placement::pg_count);
        std::uint32_t cursor = 0, remaining = 200;
        for (const auto& [begin, end] : used) {
            if (begin > cursor) {
                const auto take = std::min(remaining, begin - cursor);
                bind_range(state, cursor, take);
                remaining -= take;
                if (!remaining) return;
            }
            cursor = std::max(cursor, end);
        }
        throw Error(wire::CONFLICT, "global PG pool has fewer than 200 free PGs");
    }

    fs::path root;
    std::unique_ptr<File> directory_file, lock_file;
    std::chrono::milliseconds heartbeat_timeout, heartbeat_interval;
    mutable std::mutex mutex;
    wire::Topology topology;
    wire::SpaceCatalog catalog;
    std::unordered_map<std::uint32_t, std::uint64_t> cursors;
    std::unordered_map<std::string, LiveServer> live_servers;
    std::unordered_map<std::string, std::uint64_t> changed_revisions;
    bool storage_failed = false;
    std::size_t recovery_scan_space = 0;
    std::uint32_t recovery_scan_pg = 0;
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
        if (request.has_datacenter()) server->set_datacenter(request.datacenter());
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
    {
        std::lock_guard lock(impl_->mutex);
        impl_->ensure_available();
        placement::validate_topology(impl_->catalog, topology);
    }
    topology.set_revision(0);
    response.Clear();
    response.mutable_status();
    response.mutable_topology()->Swap(&topology);
}

void Store::create_space(const wire::CreateSpaceRequest& request, wire::SpaceResponse& response) {
    std::lock_guard lock(impl_->mutex);
    impl_->check_space_revision(request);
    validate_id(request.name());
    auto spec = request.spec();
    placement::normalize_spec(spec);
    for (const auto& state : impl_->catalog.spaces()) {
        if (state.space().name() != request.name()) continue;
        if (state.space().spec().SerializeAsString() != spec.SerializeAsString()) {
            throw Error(wire::CONFLICT, "space already exists with a different specification");
        }
        response.Clear();
        response.mutable_status();
        response.set_revision(impl_->catalog.revision());
        *response.mutable_space() = state.space();
        return;
    }
    if (impl_->catalog.next_space_id() > 255) throw Error(wire::CONFLICT, "space identifiers exhausted");
    auto candidate = impl_->catalog;
    auto* state = candidate.add_spaces();
    auto* space = state->mutable_space();
    space->set_name(request.name());
    space->set_id(static_cast<std::uint32_t>(candidate.next_space_id()));
    space->set_total_pgs(placement::pg_count);
    *space->mutable_spec() = spec;
    candidate.set_next_space_id(candidate.next_space_id() + 1);
    impl_->bind_initial(*state);
    wire::SpaceResponse result;
    result.mutable_status();
    *result.mutable_space() = *space;
    impl_->publish_catalog(candidate);
    result.set_revision(impl_->catalog.revision());
    response.Swap(&result);
}

void Store::bind_pgs(const wire::BindPGsRequest& request, wire::SpaceResponse& response) {
    std::lock_guard lock(impl_->mutex);
    impl_->check_space_revision(request);
    const auto old = impl_->find_space(request.name());
    auto updated = old;
    impl_->bind_range(updated, request.start(), request.count());
    if (updated.SerializeAsString() != old.SerializeAsString()) {
        auto candidate = impl_->catalog;
        for (auto& state : *candidate.mutable_spaces()) {
            if (state.space().id() == old.space().id()) { state = updated; break; }
        }
        impl_->publish_catalog(candidate);
    }
    response.Clear(); response.mutable_status();
    response.set_revision(impl_->catalog.revision());
    *response.mutable_space() = updated.space();
}

void Store::unbind_pgs(const wire::BindPGsRequest& request, wire::SpaceResponse& response) {
    std::lock_guard lock(impl_->mutex);
    impl_->check_space_revision(request);
    if (!request.count() || request.start() >= placement::pg_count || request.count() > placement::pg_count - request.start()) {
        throw Error(wire::INVALID_ARGUMENT, "PG interval must be nonempty and within 0..9999999");
    }
    const auto old = impl_->find_space(request.name());
    auto updated = old;
    updated.clear_ranges();
    const auto end = request.start() + request.count();
    bool changed = false;
    for (const auto& range : old.ranges()) {
        if (range.end() <= request.start() || range.begin() >= end) {
            *updated.add_ranges() = range;
            continue;
        }
        changed = true;
        if (range.begin() < request.start()) {
            auto* left = updated.add_ranges(); *left = range; left->set_end(request.start());
        }
        if (range.end() > end) {
            auto* right = updated.add_ranges(); *right = range; right->set_begin(end);
        }
    }
    if (changed) {
        if (!old.blob_sequences().empty()) throw Error(wire::CONFLICT, "Space has issued Blob sequences; unbinding requires data migration");
        for (const auto& pg : old.overrides()) if (pg.id() >= request.start() && pg.id() < end) {
            throw Error(wire::CONFLICT, "PG has durable recovery history; unbinding requires epoch migration");
        }
        Impl::compact_ranges(updated);
        auto candidate = impl_->catalog;
        for (auto& state : *candidate.mutable_spaces()) {
            if (state.space().id() == old.space().id()) { state = updated; break; }
        }
        impl_->publish_catalog(candidate);
    }
    response.Clear(); response.mutable_status();
    response.set_revision(impl_->catalog.revision());
    *response.mutable_space() = updated.space();
}

void Store::list_pg_pool(const wire::ListPGPoolRequest& request, wire::ListPGPoolResponse& response) const {
    std::lock_guard lock(impl_->mutex);
    impl_->ensure_available();
    const auto limit = request.limit() ? request.limit() : 100;
    if (request.start() > placement::pg_count || limit > 1000) throw Error(wire::INVALID_ARGUMENT, "invalid global PG page");
    response.Clear(); response.mutable_status();
    response.set_revision(impl_->catalog.revision()); response.set_total_pgs(placement::pg_count);
    std::uint32_t bound = 0;
    for (const auto& state : impl_->catalog.spaces()) bound += state.space().bound_pgs();
    response.set_bound_pgs(bound);
    const auto end = std::min(placement::pg_count, request.start() + limit);
    for (auto id = request.start(); id < end; ++id) {
        auto* item = response.add_pgs(); item->mutable_pg()->set_id(id);
        for (const auto& state : impl_->catalog.spaces()) {
            if (!placement::owns_pg(state, id)) continue;
            item->set_space_name(state.space().name());
            *item->mutable_pg() = placement::group(state, id);
            break;
        }
    }
    if (end < placement::pg_count) response.set_next_start(end);
}

void Store::list_spaces(const wire::ListSpacesRequest& request, wire::ListSpacesResponse& response) const {
    std::lock_guard lock(impl_->mutex);
    impl_->ensure_available();
    response.Clear();
    response.mutable_status();
    response.set_revision(impl_->catalog.revision());
    if (!request.name().empty()) *response.add_spaces() = impl_->find_space(request.name()).space();
    else for (const auto& state : impl_->catalog.spaces()) *response.add_spaces() = state.space();
}

void Store::list_pgs(const wire::ListPGsRequest& request, wire::ListPGsResponse& response) const {
    std::lock_guard lock(impl_->mutex);
    const auto& state = impl_->find_space(request.name());
    const auto limit = request.limit() ? request.limit() : 100;
    if (request.start() > placement::pg_count || limit > 1000) throw Error(wire::INVALID_ARGUMENT, "invalid PG page");
    response.Clear(); response.mutable_status();
    response.set_revision(impl_->catalog.revision());
    *response.mutable_space() = state.space();
    if (request.include_inactive()) {
        const auto end = std::min(placement::pg_count, request.start() + limit);
        for (auto id = request.start(); id < end; ++id) *response.add_pgs() = placement::group(state, id);
        if (end < placement::pg_count) response.set_next_start(end);
    } else {
        auto next = placement::next_cursor(state, request.start());
        while (next && *next < placement::pg_count && response.pgs_size() < static_cast<int>(limit)) {
            *response.add_pgs() = placement::group(state, static_cast<std::uint32_t>(*next));
            next = placement::next_cursor(state, *next + 1);
        }
        if (next && *next < placement::pg_count) response.set_next_start(static_cast<std::uint32_t>(*next));
    }
}

void Store::allocate_blobs(const wire::AllocateBlobsRequest& request, wire::AllocateBlobsResponse& response) {
    std::lock_guard lock(impl_->mutex);
    const auto& state = impl_->find_space(request.name());
    const auto count = request.count() ? request.count() : 1;
    if (count > 1024) throw Error(wire::INVALID_ARGUMENT, "Blob allocation count must be in 1..1024");
    if (!state.space().bound_pgs()) throw Error(wire::CONFLICT, "space has no bound PGs");
    const auto space_id = state.space().id();
    const auto reserved = state.blob_sequences().empty() ? 0 : state.blob_sequences().rbegin()->end();
    auto& cursor = impl_->cursors.try_emplace(space_id, reserved).first->second;
    const bool reserve = count > reserved - cursor;
    auto first = cursor;
    wire::SpaceCatalog candidate;
    if (reserve) {
        const auto global = impl_->catalog.reserved_blob_sequence();
        if (count > ChunkId::max_sequence - global) throw Error(wire::CONFLICT, "Blob sequences exhausted");
        const auto last = global + std::min<std::uint64_t>(4096, ChunkId::max_sequence - global);
        first = global;
        candidate = impl_->catalog;
        candidate.set_reserved_blob_sequence(last);
        for (auto& item : *candidate.mutable_spaces()) {
            if (item.space().id() != space_id) continue;
            if (!item.blob_sequences().empty() && item.blob_sequences().rbegin()->end() == global) {
                item.mutable_blob_sequences(item.blob_sequences_size() - 1)->set_end(last);
            } else {
                auto* range = item.add_blob_sequences();
                range->set_begin(global + 1);
                range->set_end(last);
            }
            break;
        }
    }
    wire::AllocateBlobsResponse result;
    result.mutable_status();
    result.set_revision(impl_->catalog.revision());
    for (std::uint64_t offset = 1; offset <= count; ++offset) {
        auto* blob = result.add_blobs();
        blob->set_seq_id(first + offset);
        blob->set_space_id(space_id);
        blob->set_start_cursor(placement::initial_cursor(state, first + offset));
    }
    if (reserve) {
        impl_->persist_catalog(candidate);
        impl_->catalog.Swap(&candidate);
    }
    cursor = first + count;
    response.Swap(&result);
}

void Store::allocate_chunks(const wire::AllocateChunksRequest& request, wire::AllocateChunksResponse& response) {
    std::lock_guard lock(impl_->mutex);
    const auto& state = impl_->find_space(request.name());
    const auto count = request.count() ? request.count() : 1;
    if (count > 1024) throw Error(wire::INVALID_ARGUMENT, "chunk count must be in 1..1024");
    if (!state.space().bound_pgs()) throw Error(wire::CONFLICT, "space has no bound PGs");
    if (!placement::owns_blob(state, request.seq_id())) throw Error(wire::NOT_FOUND, "Blob sequence does not belong to this Space");
    std::unordered_map<std::string, bool> eligible;
    for (const auto& server : impl_->topology.servers()) eligible.emplace(server.id(), impl_->eligible(server));
    wire::AllocateChunksResponse result;
    result.mutable_status();
    result.set_revision(impl_->catalog.revision());
    const auto start = request.has_start_cursor() ? request.start_cursor() :
        placement::initial_cursor(state, request.seq_id());
    auto cursor = placement::next_cursor(state, start);
    // A batch may visit a PG more than once when few PGs are bound. Select and
    // validate its replicas once per request, retaining a single health view.
    std::unordered_map<std::uint32_t, wire::PlacementGroup> routes;
    routes.reserve(std::min(count, state.space().bound_pgs()));
    for (std::uint32_t offset = 0; offset < count; ++offset) {
        if (!cursor) throw Error(wire::CONFLICT, "64-bit Chunk allocation cursor exhausted");
        const auto pg = static_cast<std::uint32_t>(*cursor % placement::pg_count);
        const auto index = unhash_chunk_index(*cursor);
        auto* chunk = result.add_chunks();
        chunk->set_chunk_id(placement::chunk_id(state.space().id(), request.seq_id(), index));
        *chunk->mutable_space() = state.space();
        auto [route, inserted] = routes.try_emplace(pg);
        if (inserted) {
            route->second = placement::group(state, pg);
            if (route->second.recovering()) throw Error(wire::CONFLICT, "PG recovery in progress");
            for (const auto& replica : route->second.replicas()) {
                if (!eligible.at(replica.server_id())) {
                    throw Error(wire::CONFLICT, "PG replica is unavailable or administratively disabled: " + replica.server_id());
                }
            }
        }
        *chunk->mutable_pg() = route->second;
        cursor = *cursor == UINT64_MAX ? std::nullopt : placement::next_cursor(state, *cursor + 1);
    }
    if (cursor) result.set_next_cursor(*cursor);
    response.Swap(&result);
}

void Store::locate_chunk(const wire::LocateChunkRequest& request, wire::LocateChunkResponse& response) const {
    const auto id = placement::decode_id(request.chunk_id());
    std::lock_guard lock(impl_->mutex);
    const auto& state = impl_->find_space(request.name());
    if (id.space != state.space().id() || !placement::owns_blob(state, id.sequence) || !placement::owns_pg(state, id.pg)) {
        throw Error(wire::NOT_FOUND, "chunk ID does not belong to this space's allocated namespace");
    }
    response.Clear();
    response.mutable_status();
    response.set_revision(impl_->catalog.revision());
    auto* chunk = response.mutable_chunk();
    chunk->set_chunk_id(request.chunk_id());
    *chunk->mutable_space() = state.space();
    *chunk->mutable_pg() = placement::group(state, id.pg);
}

namespace {
void set_override(wire::SpaceState& state, const wire::PlacementGroup& pg) {
    auto* values = state.mutable_overrides();
    for (auto& value : *values) if (value.id() == pg.id()) { value = pg; return; }
    *values->Add() = pg;
    std::sort(values->begin(), values->end(), [](const auto& a, const auto& b) { return a.id() < b.id(); });
}
bool contains_replica(const wire::PlacementGroup& pg, const std::string& id) {
    return std::any_of(pg.replicas().begin(), pg.replicas().end(), [&](const auto& node) { return node.server_id() == id; });
}
}

void Store::list_recoveries(const wire::ListRecoveriesRequest& request, wire::ListRecoveriesResponse& response) const {
    std::lock_guard lock(impl_->mutex); impl_->ensure_available();
    const auto limit = request.limit() ? request.limit() : 100;
    if (limit > 1000) throw Error(wire::INVALID_ARGUMENT, "recovery page exceeds 1000");
    response.Clear(); response.mutable_status(); response.set_revision(impl_->catalog.revision());
    *response.mutable_failures() = impl_->catalog.failures();
    for (const auto& job : impl_->catalog.recoveries()) {
        if (job.id() <= request.after_id() || (!request.space_name().empty() && request.space_name() != job.space_name()) ||
            (request.active_only() && job.phase() == wire::RECOVERED)) continue;
        if (response.recoveries_size() == static_cast<int>(limit)) { response.set_next_id(response.recoveries(response.recoveries_size() - 1).id()); break; }
        *response.add_recoveries() = job;
    }
}

void Store::retirement_directives(const std::string& server, wire::ServerHeartbeatRequest& request) const {
    std::lock_guard lock(impl_->mutex); impl_->ensure_available();
    std::map<std::uint32_t, std::uint64_t> retired;
    for (const auto& job : impl_->catalog.recoveries()) {
        if (job.phase() == wire::RECOVERED && contains_replica(job.old_pg(), server) && !contains_replica(job.new_pg(), server)) retired[job.pg_id()] = job.id();
    }
    for (const auto& [pg, epoch] : retired) { auto* item = request.add_retired_pgs(); item->set_pg_id(pg); item->set_epoch(epoch); }
}

void Store::plan_recoveries(std::uint64_t now, std::uint64_t grace, std::uint32_t scan_limit) {
    if (!grace || !scan_limit || scan_limit > 4096) throw Error(wire::INVALID_ARGUMENT, "invalid recovery scheduler limits");
    std::lock_guard lock(impl_->mutex); impl_->ensure_available();
    if (impl_->catalog.spaces().empty() || impl_->catalog.failures().empty()) return;
    std::set<std::string> failed;
    for (const auto& failure : impl_->catalog.failures()) {
        const auto configured = find_server(impl_->topology, failure.server_id());
        const auto live = impl_->live_servers.find(failure.server_id());
        if (configured != impl_->topology.mutable_servers()->end() && !configured->disabled() &&
            live != impl_->live_servers.end() && !live->second.status.error().empty() &&
            now >= failure.since_unix_ms() && now - failure.since_unix_ms() >= grace) failed.insert(failure.server_id());
    }
    if (failed.empty()) return;
    unsigned active = 0;
    for (const auto& job : impl_->catalog.recoveries()) if (job.phase() != wire::RECOVERED) ++active;
    unsigned empty_spaces = 0;
    for (unsigned inspected = 0; inspected < scan_limit && active < 16;) {
        impl_->recovery_scan_space %= impl_->catalog.spaces_size();
        const auto space_index = impl_->recovery_scan_space;
        const auto& state = impl_->catalog.spaces(static_cast<int>(space_index));
        const auto next = placement::next_cursor(state, impl_->recovery_scan_pg);
        if (!next || *next >= placement::pg_count) {
            ++impl_->recovery_scan_space; impl_->recovery_scan_pg = 0;
            if (++empty_spaces >= static_cast<unsigned>(impl_->catalog.spaces_size())) break;
            continue;
        }
        empty_spaces = 0; ++inspected;
        const auto pg_id = static_cast<std::uint32_t>(*next); impl_->recovery_scan_pg = pg_id + 1;
        const auto old = placement::group(state, pg_id);
        if (old.recovering()) continue;
        std::vector<unsigned> missing;
        bool survivors_healthy = true;
        for (int i = 0; i < old.replicas_size(); ++i) {
            const auto& node = old.replicas(i);
            if (failed.contains(node.server_id())) { missing.push_back(i); continue; }
            const auto configured = find_server(impl_->topology, node.server_id());
            const auto live = impl_->live_servers.find(node.server_id());
            if (configured == impl_->topology.mutable_servers()->end() || !impl_->eligible(*configured) ||
                live == impl_->live_servers.end() || !live->second.status.pg_recovery_capable()) survivors_healthy = false;
        }
        if (missing.empty() || missing.size() == static_cast<std::size_t>(old.replicas_size()) || !survivors_healthy) continue;
        wire::PlacementGroup replacement = old;
        bool found_all = true;
        for (const auto slot : missing) {
            const wire::FServer* best = nullptr;
            std::uint64_t best_load = UINT64_MAX;
            for (const auto& target : impl_->topology.servers()) {
                if (contains_replica(old, target.id()) || contains_replica(replacement, target.id()) || !impl_->eligible(target)) continue;
                const auto& live = impl_->live_servers.at(target.id()).status;
                if (!live.pg_recovery_capable()) continue;
                const auto& allowed = state.space().spec().placement_policy().datacenters();
                if (!allowed.empty() && std::find(allowed.begin(), allowed.end(), target.datacenter()) == allowed.end()) continue;
                bool capacity = false;
                for (const auto& disk : target.disks()) if (!disk.disabled()) for (const auto& report : live.disks()) {
                    if (report.id() == disk.id() && report.healthy() && report.available_bytes() >= state.space().spec().chunk_size()) capacity = true;
                }
                if (!capacity) continue;
                auto proposed = replacement;
                auto* node = proposed.mutable_replicas(slot); node->set_server_id(target.id()); node->set_endpoint(target.endpoint()); node->set_datacenter(target.datacenter());
                wire::SpaceState check; *check.mutable_space() = state.space();
                auto* range = check.add_ranges(); range->set_begin(pg_id); range->set_end(pg_id + 1); range->set_algorithm_version(1);
                *range->mutable_candidates() = proposed.replicas();
                try { placement::group(check, pg_id); } catch (const Error&) { continue; }
                std::uint64_t load = 0;
                for (const auto& job : impl_->catalog.recoveries()) if (contains_replica(job.new_pg(), target.id()) && !contains_replica(job.old_pg(), target.id())) ++load;
                if (!best || load < best_load || (load == best_load && target.id() < best->id())) { best = &target; best_load = load; }
            }
            if (!best) { found_all = false; break; }
            auto* node = replacement.mutable_replicas(slot); node->set_server_id(best->id()); node->set_endpoint(best->endpoint()); node->set_datacenter(best->datacenter());
        }
        if (!found_all) continue;
        if (impl_->catalog.recovery_sequence() == UINT64_MAX) throw Error(wire::CONFLICT, "recovery epochs exhausted");
        auto candidate = impl_->catalog;
        const auto id = candidate.recovery_sequence() + 1; candidate.set_recovery_sequence(id);
        auto* job = candidate.add_recoveries(); job->set_id(id); job->set_space_name(state.space().name()); job->set_pg_id(pg_id);
        *job->mutable_old_pg() = old; replacement.set_epoch(id); replacement.set_recovering(false); *job->mutable_new_pg() = replacement;
        for (const auto slot : missing) job->add_failed_servers(old.replicas(slot).server_id());
        job->set_updated_unix_ms(now);
        auto blocked = old; blocked.set_recovering(true);
        set_override(*candidate.mutable_spaces(static_cast<int>(space_index)), blocked);
        impl_->publish_catalog(candidate); ++active;
    }
}

void Store::advance_recovery(std::uint64_t id, wire::RecoveryPhase expected, wire::RecoveryPhase next,
                             const std::string& cursor, std::uint64_t chunks, std::uint64_t bytes) {
    std::lock_guard lock(impl_->mutex); impl_->ensure_available();
    auto candidate = impl_->catalog;
    auto found = std::find_if(candidate.mutable_recoveries()->begin(), candidate.mutable_recoveries()->end(), [&](const auto& job) { return job.id() == id; });
    if (found == candidate.mutable_recoveries()->end()) throw Error(wire::NOT_FOUND, "recovery not found");
    if (found->phase() != expected) throw Error(wire::CONFLICT, "recovery phase changed");
    if (next < expected || next > expected + 1 || expected == wire::RECOVERED ||
        (next == expected && expected != wire::SEALING && expected != wire::COPYING && expected != wire::VERIFYING)) throw Error(wire::INVALID_ARGUMENT, "invalid recovery transition");
    if ((next == expected && cursor.empty()) || (next != expected && !cursor.empty())) throw Error(wire::INVALID_ARGUMENT, "cursor must advance within a phase and reset between phases");
    if (!cursor.empty() && (cursor.size() != 30 || !valid_storage_chunk_id(cursor) || ChunkId::parse(cursor).pg() != found->pg_id() ||
                           (next == expected && cursor <= found->cursor()))) throw Error(wire::INVALID_ARGUMENT, "invalid recovery progress cursor");
    if (chunks > UINT64_MAX - found->copied_chunks() || bytes > UINT64_MAX - found->copied_bytes()) throw Error(wire::INVALID_ARGUMENT, "recovery counters overflow");
    found->set_phase(next); found->set_cursor(cursor); found->clear_error(); found->set_updated_unix_ms(wall_milliseconds());
    found->set_copied_chunks(found->copied_chunks() + chunks); found->set_copied_bytes(found->copied_bytes() + bytes);
    if (next == wire::RECOVERED) {
        for (auto& state : *candidate.mutable_spaces()) if (state.space().name() == found->space_name()) set_override(state, found->new_pg());
        impl_->publish_catalog(candidate);
    } else { impl_->persist_catalog(candidate); impl_->catalog.Swap(&candidate); }
}

void Store::recovery_error(std::uint64_t id, const std::string& error) {
    std::lock_guard lock(impl_->mutex); impl_->ensure_available();
    auto candidate = impl_->catalog;
    for (auto& job : *candidate.mutable_recoveries()) {
        if (job.id() != id || job.phase() == wire::RECOVERED) continue;
        const auto message = error.substr(0, 4096);
        if (job.error() == message) return;
        job.set_error(message); job.set_updated_unix_ms(wall_milliseconds());
        impl_->persist_catalog(candidate); impl_->catalog.Swap(&candidate); return;
    }
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
    if (std::any_of(impl_->catalog.failures().begin(), impl_->catalog.failures().end(), [&](const auto& f) { return f.server_id() == server.id(); })) {
        auto candidate = impl_->catalog;
        auto* failures = candidate.mutable_failures();
        for (auto it = failures->begin(); it != failures->end();) {
            if (it->server_id() == server.id()) it = failures->erase(it); else ++it;
        }
        impl_->persist_catalog(candidate); impl_->catalog.Swap(&candidate);
    }
    next.status.set_pg_recovery_capable(request.pg_recovery_capable());
    next.status.set_chunk_count(request.chunk_count());
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
    const auto now = wall_milliseconds();
    auto failure = std::find_if(impl_->catalog.failures().begin(), impl_->catalog.failures().end(), [&](const auto& f) { return f.server_id() == server_id; });
    std::uint64_t since = now;
    if (failure == impl_->catalog.failures().end()) {
        auto candidate = impl_->catalog; auto* item = candidate.add_failures(); item->set_server_id(server_id); item->set_since_unix_ms(now);
        impl_->persist_catalog(candidate); impl_->catalog.Swap(&candidate);
    } else since = failure->since_unix_ms();
    next.status.set_failure_since_unix_ms(since);
    next.status.set_last_probe_unix_ms(now);
    next.status.set_error(error.empty() ? "FServer probe failed" : error.substr(0, 4096));
    impl_->live_servers.insert_or_assign(server_id, std::move(next));
}

} // namespace chunkstore::master
