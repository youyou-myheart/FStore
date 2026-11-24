#include "chunk_store.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <climits>
#include <fcntl.h>
#include <limits>
#include <mutex>
#include <shared_mutex>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/xattr.h>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

namespace chunkstore {
namespace {

namespace fs = std::filesystem;

[[noreturn]] void system_failure(const char* operation, int error = errno) {
    throw std::system_error(error, std::generic_category(), operation);
}

class File {
public:
    File() = default;
    explicit File(int fd) : fd_(fd) {}
    ~File() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }
    File(const File&) = delete;
    File& operator=(const File&) = delete;

    int get() const { return fd_; }
    void swap(File& other) noexcept { std::swap(fd_, other.fd_); }

    void open(const fs::path& path, int flags) {
        do {
            fd_ = ::open(path.c_str(), flags | O_CLOEXEC | O_NOFOLLOW, 0600);
        } while (fd_ < 0 && errno == EINTR);
        if (fd_ < 0) {
            system_failure("open");
        }
    }

private:
    int fd_ = -1;
};

void sync_file(int fd, bool data_only) {
    int result;
    do {
        result = data_only ? ::fdatasync(fd) : ::fsync(fd);
    } while (result < 0 && errno == EINTR);
    if (result < 0) {
        system_failure(data_only ? "fdatasync" : "fsync");
    }
}

void sync_directory(const fs::path& path) {
    File directory;
    directory.open(path, O_RDONLY | O_DIRECTORY);
    sync_file(directory.get(), false);
}

constexpr std::string_view hex = "0123456789abcdef";
constexpr auto max_offset = static_cast<std::uint64_t>(std::numeric_limits<off_t>::max());
constexpr char capacity_attribute[] = "user.chunkstore.capacity";

using namespace std::chrono_literals;

std::uint64_t unix_nanoseconds() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

constexpr auto retention_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(24h).count();

bool expired(std::uint64_t timestamp, std::uint64_t now) {
    return now >= timestamp && now - timestamp >= retention_ns;
}

bool expired_entry(std::string_view name, std::string_view shard, std::uint64_t now) {
    const auto suffix = name.rfind('.');
    if (suffix == name.npos || (name.substr(suffix) != ".open" && name.substr(suffix) != ".frozen") ||
        suffix <= 33 || !name.starts_with(shard) || name[32] != '.' ||
        name.substr(0, 32).find_first_not_of(hex) != name.npos) {
        return false;
    }
    std::uint64_t timestamp = 0;
    const char* end = name.data() + suffix;
    const auto parsed = std::from_chars(name.data() + 33, end, timestamp);
    return parsed.ec == std::errc{} && parsed.ptr == end && timestamp != 0 && expired(timestamp, now);
}

void purge_shard(const fs::path& directory, std::string_view shard, std::uint64_t now,
                 std::stop_token stop, TrafficLimiter& traffic) {
    const auto trash = directory / ".trash";
    if (!fs::is_directory(fs::symlink_status(trash))) {
        return;
    }
    bool removed = false;
    for (const auto& entry : fs::directory_iterator(trash)) {
        if (stop.stop_requested()) {
            break;
        }
        if (expired_entry(entry.path().filename().native(), shard, now) &&
            fs::is_regular_file(entry.symlink_status())) {
            struct stat info{};
            if (::lstat(entry.path().c_str(), &info) < 0) {
                if (errno != ENOENT) std::perror("stat expired chunk");
                continue;
            }
            if (!S_ISREG(info.st_mode)) continue;
            if (!traffic.acquire(TrafficLane::Trash, info.st_size, stop)) break;
            if (::unlink(entry.path().c_str()) == 0) {
                removed = true;
            } else if (errno != ENOENT) {
                std::perror("unlink expired chunk");
            }
        }
    }
    if (removed) {
        sync_directory(trash);
    }
}

std::uint64_t load_capacity(int fd) {
    std::array<char, 20> value;
    const auto count = ::fgetxattr(fd, capacity_attribute, value.data(), value.size());
    if (count < 0) {
        if (errno == ENODATA) {
            return 0; // Legacy chunk awaiting an explicit capacity.
        }
        system_failure("read chunk capacity");
    }
    std::uint64_t capacity = 0;
    const auto end = value.data() + count;
    const auto result = std::from_chars(value.data(), end, capacity);
    if (result.ec != std::errc{} || result.ptr != end || capacity == 0 || capacity > max_offset) {
        system_failure("invalid chunk capacity", EIO);
    }
    return capacity;
}

void save_capacity(int fd, std::uint64_t capacity) {
    const auto value = std::to_string(capacity);
    if (::fsetxattr(fd, capacity_attribute, value.data(), value.size(), XATTR_CREATE) != 0) {
        system_failure("write chunk capacity");
    }
}

struct Chunk {
    Chunk(const fs::path& root, std::string_view id)
        : directory(root / id.substr(0, 2)),
          open_path(directory / (std::string(id) + ".open")),
          frozen_path(directory / (std::string(id) + ".frozen")) {}

    bool load() {
        struct stat open_info{}, frozen_info{};
        const bool open_exists = inspect(open_path, open_info);
        const bool frozen_exists = inspect(frozen_path, frozen_info);
        if (open_exists && frozen_exists) {
            system_failure("conflicting chunk files", EIO);
        }
        if (!open_exists && !frozen_exists) {
            return false;
        }

        state = frozen_exists ? State::Frozen : State::Open;
        file.open(frozen_exists ? frozen_path : open_path, frozen_exists ? O_RDONLY : O_RDWR);
        capacity = load_capacity(file.get());
        const auto size = static_cast<std::uint64_t>(frozen_exists ? frozen_info.st_size : open_info.st_size);
        if (capacity != 0 && size > capacity) {
            system_failure("chunk length exceeds stored capacity", EIO);
        }
        if (open_exists || capacity != 0) {
            // A process restart can leave an earlier failed append in page cache.
            // Sync the capacity too if a legacy registration was interrupted.
            sync_file(file.get(), false);
        }
        length.store(size, std::memory_order_relaxed);
        ready.store(true, std::memory_order_release);
        return true;
    }

    void check_writable() const {
        if (write_failed) {
            throw WriteUnavailable("reopen the store after a chunk write or sync failure");
        }
    }

    void check_repair_target() const {
        // A new index entry also starts in Open state, before it owns a file.
        if (state == State::Open && (ready.load() || write_failed)) {
            throw RepairConflict("cannot repair an open destination chunk");
        }
    }

    void check_appendable() const {
        check_writable();
        if (!ready.load(std::memory_order_relaxed)) {
            system_failure("chunk not found", ENOENT);
        }
        if (state == State::Frozen) {
            throw ChunkFrozen("chunk is frozen");
        }
        if (capacity == 0) {
            throw WriteUnavailable("register a capacity with create_chunk before appending");
        }
    }

    fs::path directory;
    fs::path open_path;
    fs::path frozen_path;
    File file;
    std::mutex writer;
    std::shared_mutex file_view;
    State state = State::Open;
    bool write_failed = false;
    std::uint64_t capacity = 0;
    std::atomic<bool> ready{false};
    std::atomic<std::uint64_t> length{0};

private:
    static bool inspect(const fs::path& path, struct stat& info) {
        if (::lstat(path.c_str(), &info) == 0) {
            if (!S_ISREG(info.st_mode)) {
                system_failure("chunk path is not a regular file", EIO);
            }
            return true;
        }
        if (errno != ENOENT) {
            system_failure("lstat");
        }
        return false;
    }
};

std::size_t checked_read(const Chunk& chunk, std::uint64_t offset, std::size_t size) {
    if (!chunk.ready.load(std::memory_order_acquire)) {
        system_failure("chunk not found", ENOENT);
    }
    const auto length = chunk.length.load(std::memory_order_acquire);
    if (offset > length || size > length - offset) {
        throw std::out_of_range("read range exceeds published chunk length");
    }
    return size;
}

void read_exact(int fd, std::uint64_t offset, std::span<char> output) {
    while (!output.empty()) {
        const auto count = ::pread(fd, output.data(),
                                   std::min(output.size(), static_cast<std::size_t>(SSIZE_MAX)),
                                   static_cast<off_t>(offset));
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count < 0) {
            system_failure("pread");
        }
        if (count == 0) {
            system_failure("unexpected end of chunk", EIO);
        }
        output = output.subspan(count);
        offset += count;
    }
}

} // namespace

struct ChunkStore::Impl {
    Impl(const fs::path& directory, std::uint64_t rate) : root(fs::canonical(directory)), traffic(rate) {
        root_file.open(root, O_RDONLY | O_DIRECTORY);
        if (::flock(root_file.get(), LOCK_EX | LOCK_NB) < 0) {
            system_failure("lock storage directory");
        }
        // Persist a rename or directory creation left visible after a sync error.
        for (const auto& entry : fs::directory_iterator(root)) {
            const auto name = entry.path().filename().native();
            if (name.size() == 2 && name.find_first_not_of(hex) == std::string::npos) {
                const auto trash = entry.path() / ".trash";
                if (fs::is_directory(fs::symlink_status(trash))) {
                    sync_directory(trash);
                }
                sync_directory(entry.path());
            }
        }
        sync_file(root_file.get(), false);
        cleaner = std::jthread([this](std::stop_token stop) {
            std::mutex mutex;
            std::condition_variable_any timer;
            std::unique_lock lock(mutex);
            while (!stop.stop_requested()) {
                try {
                    const auto now = unix_nanoseconds();
                    for (const auto& entry : fs::directory_iterator(root)) {
                        if (stop.stop_requested()) {
                            break;
                        }
                        const auto shard = entry.path().filename().native();
                        if (shard.size() == 2 && shard.find_first_not_of(hex) == shard.npos &&
                            fs::is_directory(entry.symlink_status())) {
                            try {
                                purge_shard(entry.path(), shard, now, stop, traffic);
                            } catch (const std::system_error& e) {
                                std::fprintf(stderr, "trash cleanup: %s\n", e.what());
                            }
                        }
                    }
                } catch (const std::system_error& e) {
                    std::fprintf(stderr, "trash scan: %s\n", e.what());
                }
                timer.wait_for(lock, stop, 1min, [] { return false; });
            }
        });
    }

    std::shared_ptr<Chunk> find(std::string_view id, bool create = false, bool repair = false) {
        if (id.size() != 32 || id.find_first_not_of(hex) != std::string_view::npos) {
            throw std::invalid_argument("chunk ID must be 32 lowercase hex digits");
        }
        {
            std::shared_lock lock(index_mutex);
            const auto found = chunks.find(id);
            if (found != chunks.end()) {
                return found->second;
            }
        }

        // Only cold lookups in the same shard can wait on another file's fsync.
        const auto shard = hex.find(id[0]) * 16 + hex.find(id[1]);
        std::lock_guard loading(load_mutexes[shard]);
        {
            std::shared_lock lock(index_mutex);
            const auto found = chunks.find(id);
            if (found != chunks.end()) {
                return found->second;
            }
        }
        auto chunk = std::make_shared<Chunk>(root, id);
        try {
            if (!chunk->load() && !create) {
                system_failure("chunk not found", ENOENT);
            }
        } catch (const std::system_error&) {
            if (!repair || fs::exists(chunk->open_path) ||
                !fs::is_regular_file(fs::symlink_status(chunk->frozen_path))) {
                throw;
            }
            // Keep a damaged frozen file inaccessible until replacement is complete.
            chunk->state = State::Frozen;
            chunk->write_failed = true;
        }
        std::unique_lock lock(index_mutex);
        return chunks.emplace(std::string(id), std::move(chunk)).first->second;
    }

    void ensure_shard(std::string_view id, const fs::path& directory) {
        auto& initialized = shards[hex.find(id[0]) * 16 + hex.find(id[1])];
        std::call_once(initialized, [&] {
            fs::create_directory(directory);
            sync_file(root_file.get(), false);
        });
    }

    struct Hash {
        using is_transparent = void;
        std::size_t operator()(std::string_view id) const {
            return std::hash<std::string_view>{}(id);
        }
    };

    fs::path root;
    File root_file;
    std::array<std::once_flag, 256> shards;
    std::array<std::mutex, 256> load_mutexes;
    std::shared_mutex index_mutex;
    std::unordered_map<std::string, std::shared_ptr<Chunk>, Hash, std::equal_to<>> chunks;
    TrafficLimiter traffic;
    std::jthread cleaner;
};

ChunkStore::ChunkStore(const fs::path& directory, std::uint64_t rate)
    : impl_(std::make_unique<Impl>(directory, rate)) {}

TrafficLimiter& ChunkStore::traffic() { return impl_->traffic; }
ChunkStore::~ChunkStore() = default;

int ChunkStore::open_snapshot(std::string_view id, ReplicaInfo& info) {
    const auto handle = impl_->find(id);
    auto& chunk = *handle;
    std::lock_guard lock(chunk.writer);
    chunk.check_writable();
    if (!chunk.ready.load()) {
        system_failure("chunk not found", ENOENT);
    }
    if (chunk.state != State::Frozen) {
        throw RepairConflict("source chunk must be frozen before repair");
    }
    if (chunk.capacity == 0) {
        throw RepairConflict("register source capacity before repair");
    }
    info = {chunk.length.load(), chunk.capacity, chunk.state};
    const int fd = ::fcntl(chunk.file.get(), F_DUPFD_CLOEXEC, 0);
    if (fd < 0) {
        system_failure("duplicate snapshot file");
    }
    return fd;
}

int ChunkStore::open_repair_file(std::string_view id) {
    const auto handle = impl_->find(id, true, true);
    auto& chunk = *handle;
    {
        std::lock_guard lock(chunk.writer);
        chunk.check_repair_target();
    }
    const auto shard = impl_->root / id.substr(0, 2);
    impl_->ensure_shard(id, shard);
    const int fd = ::open(shard.c_str(), O_TMPFILE | O_RDWR | O_CLOEXEC, 0600);
    if (fd < 0) {
        system_failure("open unnamed repair file");
    }
    return fd;
}

void ChunkStore::install_replica(std::string_view id, int fd, const ReplicaInfo& info) {
    if (info.state != State::Frozen) {
        throw RepairConflict("only frozen replicas can be installed");
    }
    struct stat statbuf{};
    if (::fstat(fd, &statbuf) < 0) {
        system_failure("stat repair file");
    }
    if (info.capacity == 0 || info.capacity > max_offset || info.length > info.capacity ||
        info.length != static_cast<std::uint64_t>(statbuf.st_size)) {
        throw std::invalid_argument("invalid repair file size or capacity");
    }
    save_capacity(fd, info.capacity);
    sync_file(fd, false);
    const auto handle = impl_->find(id, true, true);
    auto& chunk = *handle;
    std::lock_guard lock(chunk.writer);
    // CreateChunk may have published an OPEN file while blocks were in flight.
    chunk.check_repair_target();

    File replacement(::fcntl(fd, F_DUPFD_CLOEXEC, 0));
    if (replacement.get() < 0) {
        system_failure("duplicate repair file");
    }
    auto staging = chunk.open_path;
    staging.replace_extension(".installing");
    const auto source = "/proc/self/fd/" + std::to_string(fd);
    if (::unlink(staging.c_str()) < 0 && errno != ENOENT) {
        system_failure("remove interrupted replica installation");
    }
    if (::linkat(AT_FDCWD, source.c_str(), AT_FDCWD, staging.c_str(), AT_SYMLINK_FOLLOW) < 0) {
        system_failure("link staged replica");
    }
    std::unique_lock view(chunk.file_view);
    if (::rename(staging.c_str(), chunk.frozen_path.c_str()) < 0) {
        const int error = errno;
        ::unlink(staging.c_str());
        system_failure("publish replica", error);
    }
    chunk.file.swap(replacement);
    chunk.capacity = info.capacity;
    chunk.length.store(info.length);
    chunk.state = State::Frozen;
    chunk.write_failed = true;
    sync_directory(chunk.directory);
    chunk.write_failed = false;
    chunk.ready.store(true);
}

State ChunkStore::create_chunk(std::string_view id, std::uint64_t capacity) {
    if (capacity == 0 || capacity > max_offset) {
        throw std::invalid_argument("capacity must be between 1 and the maximum file offset");
    }
    const auto handle = impl_->find(id, true);
    auto& chunk = *handle;
    std::lock_guard lock(chunk.writer);
    chunk.check_writable();
    if (chunk.ready.load(std::memory_order_relaxed)) {
        if (chunk.capacity != 0) {
            if (chunk.capacity != capacity) {
                throw CapacityMismatch("chunk already has a different capacity");
            }
            return chunk.state;
        }
        if (capacity < chunk.length.load(std::memory_order_relaxed)) {
            throw CapacityMismatch("capacity is smaller than the existing chunk length");
        }
        chunk.write_failed = true;
        save_capacity(chunk.file.get(), capacity);
        sync_file(chunk.file.get(), false);
        chunk.capacity = capacity;
        chunk.write_failed = false;
        return chunk.state;
    }

    impl_->ensure_shard(id, chunk.directory);
    auto staging = chunk.open_path;
    staging.replace_extension(".creating");
    if (::unlink(staging.c_str()) != 0 && errno != ENOENT) {
        system_failure("remove interrupted chunk creation");
    }
    File file;
    file.open(staging, O_CREAT | O_EXCL | O_RDWR);
    try {
        save_capacity(file.get(), capacity);
        sync_file(file.get(), false);
        if (::rename(staging.c_str(), chunk.open_path.c_str()) != 0) {
            system_failure("publish chunk");
        }
    } catch (...) {
        ::unlink(staging.c_str());
        throw;
    }
    chunk.file.swap(file);
    chunk.capacity = capacity;
    chunk.write_failed = true;
    sync_directory(chunk.directory);
    chunk.write_failed = false;
    chunk.ready.store(true, std::memory_order_release);
    return State::Open;
}

AppendResult ChunkStore::append(std::string_view id, std::string_view data) {
    return append_batch(id, std::span(&data, 1));
}

AppendResult ChunkStore::append_batch(std::string_view id,
                                     std::span<const std::string_view> buffers) {
    AppendGroupItem request{buffers, std::nullopt};
    append_group(id, std::span(&request, 1));
    if (!request.result) {
        throw ChunkFull("append exceeds chunk capacity");
    }
    return *request.result;
}

AppendWindow ChunkStore::append_window(std::string_view id) {
    const auto handle = impl_->find(id);
    auto& chunk = *handle;
    std::lock_guard lock(chunk.writer);
    chunk.check_appendable();
    return {chunk.length.load(std::memory_order_relaxed), chunk.capacity};
}

void ChunkStore::append_group(std::string_view id, std::span<AppendGroupItem> requests) {
    const auto handle = impl_->find(id);
    auto& chunk = *handle;
    std::lock_guard lock(chunk.writer);
    chunk.check_appendable();
    const auto offset = chunk.length.load(std::memory_order_relaxed);
    auto end = offset;
    for (auto& request : requests) {
        auto next = end;
        request.result.reset();
        // Validate a whole request before advancing the shared offset.
        std::size_t checked = 0;
        for (const auto buffer : request.buffers) {
            if (buffer.size() > chunk.capacity - next) {
                break;
            }
            next += buffer.size();
            ++checked;
        }
        if (checked == request.buffers.size()) {
            request.result = AppendResult{end, next - end};
            end = next;
        }
    }
    if (end == offset) {
        return;
    }

    // Build bounded vectors on the stack, preserving progress across short writes.
    std::array<iovec, 64> vectors;
    std::span<const std::string_view> buffers;
    auto next_request = requests.begin();
    auto position = offset;
    chunk.write_failed = true;
    while (position < end) {
        std::size_t count = 0;
        while (count < vectors.size()) {
            if (buffers.empty()) {
                if (next_request == requests.end()) {
                    break;
                }
                const auto& request = *next_request++;
                if (request.result) {
                    buffers = request.buffers;
                }
                continue;
            }
            const auto buffer = buffers.front();
            buffers = buffers.subspan(1);
            if (!buffer.empty()) {
                vectors[count++] = {const_cast<char*>(buffer.data()), buffer.size()};
            }
        }

        std::size_t first = 0;
        while (first < count) {
            const auto written = ::pwritev(chunk.file.get(), vectors.data() + first,
                                          static_cast<int>(count - first),
                                          static_cast<off_t>(position));
            if (written < 0 && errno == EINTR) {
                continue;
            }
            if (written < 0) {
                system_failure("pwritev");
            }
            if (written == 0) {
                system_failure("pwritev made no progress", EIO);
            }
            position += written;
            auto remaining = static_cast<std::size_t>(written);
            while (first < count && remaining >= vectors[first].iov_len) {
                remaining -= vectors[first++].iov_len;
            }
            if (remaining != 0) {
                auto& vector = vectors[first];
                vector.iov_base = static_cast<char*>(vector.iov_base) + remaining;
                vector.iov_len -= remaining;
            }
        }
    }
    sync_file(chunk.file.get(), true);
    chunk.length.store(end, std::memory_order_release);
    chunk.write_failed = false;
}

void ChunkStore::read_chunk(std::string_view id, std::uint64_t offset,
                            std::span<char> output) {
    const auto handle = impl_->find(id);
    auto& chunk = *handle;
    std::shared_lock view(chunk.file_view);
    checked_read(chunk, offset, output.size());
    read_exact(chunk.file.get(), offset, output);
}

std::string ChunkStore::read_chunk(std::string_view id, std::uint64_t offset,
                                   std::size_t length) {
    const auto handle = impl_->find(id);
    auto& chunk = *handle;
    std::shared_lock view(chunk.file_view);
    std::string output(checked_read(chunk, offset, length), '\0');
    read_exact(chunk.file.get(), offset, output);
    return output;
}

std::uint64_t ChunkStore::delete_chunk(std::string_view id) {
    const auto handle = impl_->find(id);
    auto& chunk = *handle;
    std::lock_guard lock(chunk.writer);
    chunk.check_writable();
    if (!chunk.ready.load()) {
        system_failure("chunk not found", ENOENT);
    }
    const auto trash = chunk.directory / ".trash";
    fs::create_directory(trash);
    if (!fs::is_directory(fs::symlink_status(trash))) {
        system_failure("invalid trash directory", ENOTDIR);
    }
    const auto& source = chunk.state == State::Frozen ? chunk.frozen_path : chunk.open_path;
    const auto suffix = source.extension().native();
    const auto other_suffix = chunk.state == State::Frozen ? ".open" : ".frozen";
    std::unique_lock view(chunk.file_view);
    auto timestamp = unix_nanoseconds();
    for (;;) {
        const auto base = std::string(id) + "." + std::to_string(timestamp);
        if (fs::exists(fs::symlink_status(trash / (base + other_suffix)))) {
            ++timestamp;
            continue;
        }
        const auto destination = trash / (base + suffix);
        if (::renameat2(AT_FDCWD, source.c_str(), AT_FDCWD, destination.c_str(), RENAME_NOREPLACE) == 0) {
            break;
        }
        if (errno != EEXIST) {
            system_failure("move chunk to trash");
        }
        ++timestamp;
    }
    File retired;
    chunk.file.swap(retired);
    chunk.ready.store(false);
    chunk.length.store(0);
    chunk.capacity = 0;
    chunk.state = State::Open;
    chunk.write_failed = true;
    sync_directory(trash);
    sync_directory(chunk.directory);
    chunk.write_failed = false;

    std::unique_lock index(impl_->index_mutex);
    if (handle.use_count() == 2) {
        impl_->chunks.erase(std::string(id));
    }
    return timestamp;
}

State ChunkStore::restore_chunk(std::string_view id, std::uint64_t timestamp) {
    if (id.size() != 32 || id.find_first_not_of(hex) != id.npos || timestamp == 0) {
        throw std::invalid_argument("invalid chunk ID or deletion timestamp");
    }
    if (expired(timestamp, unix_nanoseconds())) {
        system_failure("recycled chunk has expired", ENOENT);
    }
    const auto shard = impl_->root / id.substr(0, 2);
    const auto trash = shard / ".trash";
    if (!fs::is_directory(fs::symlink_status(trash))) {
        system_failure("trash directory not found", ENOENT);
    }
    const auto base = std::string(id) + "." + std::to_string(timestamp);
    auto source = trash / (base + ".open");
    State state = State::Open;
    if (!fs::is_regular_file(fs::symlink_status(source))) {
        source = trash / (base + ".frozen");
        state = State::Frozen;
        if (!fs::is_regular_file(fs::symlink_status(source))) {
            system_failure("recycled chunk not found", ENOENT);
        }
    }
    File file;
    file.open(source, state == State::Open ? O_RDWR : O_RDONLY);
    const auto capacity = load_capacity(file.get());
    struct stat info{};
    if (::fstat(file.get(), &info) < 0) {
        system_failure("stat recycled chunk");
    }
    const auto length = static_cast<std::uint64_t>(info.st_size);
    if (!S_ISREG(info.st_mode) || (capacity && length > capacity)) {
        system_failure("invalid recycled chunk", EIO);
    }
    const auto handle = impl_->find(id, true);
    auto& chunk = *handle;
    std::lock_guard lock(chunk.writer);
    chunk.check_writable();
    if (chunk.ready.load()) {
        throw RestoreConflict("chunk ID is already active");
    }
    std::unique_lock view(chunk.file_view);
    if (expired(timestamp, unix_nanoseconds())) {
        system_failure("recycled chunk has expired", ENOENT);
    }
    const auto& target = state == State::Open ? chunk.open_path : chunk.frozen_path;
    if (::renameat2(AT_FDCWD, source.c_str(), AT_FDCWD, target.c_str(), RENAME_NOREPLACE) < 0) {
        system_failure("restore chunk from trash");
    }
    chunk.file.swap(file);
    chunk.capacity = capacity;
    chunk.state = state;
    chunk.length.store(length);
    chunk.write_failed = true;
    sync_directory(shard);
    sync_directory(trash);
    chunk.write_failed = false;
    chunk.ready.store(true, std::memory_order_release);
    return state;
}

std::uint64_t ChunkStore::freeze_chunk(std::string_view id) {
    return freeze_chunk(id, std::nullopt);
}

std::uint64_t ChunkStore::freeze_chunk(std::string_view id, std::optional<std::uint64_t> expected_length) {
    const auto handle = impl_->find(id);
    auto& chunk = *handle;
    std::lock_guard lock(chunk.writer);
    chunk.check_writable();
    if (!chunk.ready.load(std::memory_order_relaxed)) {
        system_failure("chunk not found", ENOENT);
    }
    const auto length = chunk.length.load(std::memory_order_relaxed);
    if (expected_length && length != *expected_length) {
        throw LengthMismatch("expected length " + std::to_string(*expected_length) +
                             ", stored length " + std::to_string(length));
    }
    if (chunk.state == State::Open) {
        chunk.write_failed = true;
        if (::rename(chunk.open_path.c_str(), chunk.frozen_path.c_str()) < 0) {
            system_failure("rename");
        }
        chunk.state = State::Frozen;
        sync_directory(chunk.directory);
        chunk.write_failed = false;
    }
    return length;
}

} // namespace chunkstore
