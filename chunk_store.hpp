#pragma once

#include "traffic_limiter.hpp"
#include "pg_inventory.hpp"
#include "chunk_state.hpp"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>

namespace chunkstore {

// None acknowledges appended bytes in the kernel page cache, not on stable storage.
// Creation, Freeze/Seal, repair installation and namespace metadata still sync.
enum class AppendSync { Always, None };

struct ReplicaInfo {
    std::uint64_t length;
    std::uint64_t capacity;
    State state;
};

class RepairConflict : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class RestoreConflict : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

struct AppendResult {
    std::uint64_t offset;
    std::uint64_t length;
};

struct AppendWindow {
    std::uint64_t offset;
    std::uint64_t capacity;
};

struct AppendGroupItem {
    std::span<const std::string_view> buffers;
    // Empty for a request that did not fit. Results are valid on return.
    std::optional<AppendResult> result;
};

class ChunkFrozen : public std::logic_error {
public:
    using std::logic_error::logic_error;
};
class ChunkSealed : public std::logic_error {
public:
    using std::logic_error::logic_error;
};

class WriteUnavailable : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class ChunkFull : public std::length_error {
public:
    using std::length_error::length_error;
};

class CapacityMismatch : public std::invalid_argument {
public:
    using std::invalid_argument::invalid_argument;
};

class LengthMismatch : public std::logic_error {
public:
    using std::logic_error::logic_error;
};

// Owns an existing Linux directory. IDs are 30 lowercase hex digits (120 bits).
// Existing 24/32-digit IDs remain supported for compatibility.
// Operations are thread-safe. Finish them before destroying the store.
class ChunkStore {
public:
    explicit ChunkStore(const std::filesystem::path& directory, std::uint64_t rate = TrafficLimiter::default_rate,
                        AppendSync append_sync = AppendSync::Always);
    TrafficLimiter& traffic();
    PGInventory& inventory();
    ~ChunkStore();

    ChunkStore(const ChunkStore&) = delete;
    ChunkStore& operator=(const ChunkStore&) = delete;

    // Returned descriptors are owned by the caller.
    int open_snapshot(std::string_view id, ReplicaInfo& info, std::uint64_t epoch = 0, std::uint64_t recovery_token = 0,
                      bool require_closed = true);
    int open_repair_file(std::string_view id, std::uint64_t epoch = 0, std::uint64_t recovery_token = 0);
    void install_replica(std::string_view id, int fd, const ReplicaInfo& info, std::uint64_t epoch = 0, std::uint64_t recovery_token = 0);

    State create_chunk(std::string_view id, std::uint64_t capacity, std::uint64_t epoch = 0);
    AppendResult append(std::string_view id, std::string_view data, std::uint64_t epoch = 0);
    // Appends contiguous buffers with the configured sync policy. Returns the whole range.
    AppendResult append_batch(std::string_view id, std::span<const std::string_view> buffers, std::uint64_t epoch = 0);
    AppendWindow append_window(std::string_view id, std::uint64_t epoch = 0);
    // Processes requests in order; Always syncs accepted bytes once before publication.
    void append_group(std::string_view id, std::span<AppendGroupItem> requests, std::uint64_t epoch = 0);
    // Sync the current file version without closing it, regardless of AppendSync.
    // A supplied expected_length must match exactly. Returns the durable length.
    std::uint64_t sync_chunk(std::string_view id, std::optional<std::uint64_t> expected_length = std::nullopt,
                             std::uint64_t epoch = 0);
    // Reads exactly output.size() bytes into the caller's buffer.
    State read_chunk(std::string_view id, std::uint64_t offset, std::span<char> output, std::uint64_t epoch = 0);
    std::string read_chunk(std::string_view id, std::uint64_t offset, std::size_t length, std::uint64_t epoch = 0);
    std::uint64_t delete_chunk(std::string_view id, std::uint64_t epoch = 0);
    State restore_chunk(std::string_view id, std::uint64_t deleted_at_ns, std::uint64_t epoch = 0);
    std::uint64_t freeze_chunk(std::string_view id);
    std::uint64_t freeze_chunk(std::string_view id, std::optional<std::uint64_t> expected_length, std::uint64_t epoch = 0);
    std::uint64_t seal_chunk(std::string_view id, std::uint64_t recovery_token,
                            std::optional<std::uint64_t> expected_length = std::nullopt);

private:
    std::uint64_t close_chunk(std::string_view id, std::optional<std::uint64_t> expected_length,
                              std::uint64_t epoch, std::uint64_t recovery_token, State state);
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace chunkstore
