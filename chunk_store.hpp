#pragma once

#include "traffic_limiter.hpp"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>

namespace chunkstore {

enum class State { Open, Frozen };

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

// Owns an existing Linux directory. IDs are 32 lowercase hex digits.
// Operations are thread-safe. Finish them before destroying the store.
class ChunkStore {
public:
    explicit ChunkStore(const std::filesystem::path& directory, std::uint64_t rate = TrafficLimiter::default_rate);
    TrafficLimiter& traffic();
    ~ChunkStore();

    ChunkStore(const ChunkStore&) = delete;
    ChunkStore& operator=(const ChunkStore&) = delete;

    // Returned descriptors are owned by the caller.
    int open_snapshot(std::string_view id, ReplicaInfo& info);
    int open_repair_file(std::string_view id);
    void install_replica(std::string_view id, int fd, const ReplicaInfo& info);

    State create_chunk(std::string_view id, std::uint64_t capacity);
    AppendResult append(std::string_view id, std::string_view data);
    // Appends contiguous buffers with one sync. Returns the whole range.
    AppendResult append_batch(std::string_view id, std::span<const std::string_view> buffers);
    AppendWindow append_window(std::string_view id);
    // Processes requests in order and syncs accepted bytes once.
    void append_group(std::string_view id, std::span<AppendGroupItem> requests);
    // Reads exactly output.size() bytes into the caller's buffer.
    void read_chunk(std::string_view id, std::uint64_t offset, std::span<char> output);
    std::string read_chunk(std::string_view id, std::uint64_t offset, std::size_t length);
    std::uint64_t delete_chunk(std::string_view id);
    State restore_chunk(std::string_view id, std::uint64_t deleted_at_ns);
    std::uint64_t freeze_chunk(std::string_view id);
    std::uint64_t freeze_chunk(std::string_view id, std::optional<std::uint64_t> expected_length);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace chunkstore
