#pragma once

#include "chunk.pb.h"
#include "chunk_store.hpp"

#include <butil/iobuf.h>
#include <exception>
#include <optional>
#include <vector>

namespace chunkstore {

class RpcError : public std::runtime_error {
public:
    RpcError(int code, const std::string& message) : std::runtime_error(message), code_(code) {}
    int code() const noexcept { return code_; }
private:
    int code_;
};

class RemoteChunkError : public std::runtime_error {
public:
    RemoteChunkError(rpc::v1::Code code, const std::string& message)
        : std::runtime_error(message), code_(code) {}
    rpc::v1::Code code() const noexcept { return code_; }
private:
    rpc::v1::Code code_;
};

struct ClientOptions {
    int connect_timeout_ms = 1000;
    int timeout_ms = 5000;
    std::uint64_t pg_epoch = 0;
};

// Reuse a client across requests and threads. Finish requests before destruction.
class ChunkClient {
public:
    explicit ChunkClient(const std::string& endpoint, ClientOptions options = {});
    ~ChunkClient();
    ChunkClient(const ChunkClient&) = delete;
    ChunkClient& operator=(const ChunkClient&) = delete;

    std::uint64_t repair_chunk(std::string_view id, const std::string& destination,
                               int timeout_ms = 300000) const;
    std::uint64_t repair_chunk(std::string_view id, const std::string& destination,
                               rpc::v1::CopyPriority priority, int timeout_ms = 300000) const;
    rpc::v1::TrafficStatsResponse traffic_stats() const;
    rpc::v1::ListLocalPGsResponse list_pgs(std::uint32_t start = 0, std::uint32_t limit = 100) const;
    rpc::v1::PGInventoryResponse pg_chunks(std::uint32_t pg, std::string_view after = {}, std::uint32_t limit = 100) const;
    State create_chunk(std::string_view id, std::uint64_t capacity) const;
    AppendResult append(std::string_view id, std::uint64_t offset,
                        std::string_view data, std::uint32_t wait_timeout_ms) const;
    AppendResult append(std::string_view id, std::uint64_t offset,
                        const butil::IOBuf& data, std::uint32_t wait_timeout_ms) const;
    butil::IOBuf read_chunk(std::string_view id, std::uint64_t offset, std::size_t length) const;
    std::uint64_t delete_chunk(std::string_view id) const;
    State restore_chunk(std::string_view id, std::uint64_t deleted_at_ns) const;
    std::uint64_t freeze_chunk(std::string_view id) const;
    std::uint64_t freeze_chunk(std::string_view id, std::uint64_t expected_length) const;
    // Repair control operation; the server requires a matching SOURCE PG fence.
    std::uint64_t seal_chunk(std::string_view id, std::uint64_t recovery_token,
                             std::optional<std::uint64_t> expected_length = std::nullopt) const;
    struct ReadResult {
        butil::IOBuf data;
        std::optional<State> state;
        bool closed() const { return state && is_closed(*state); }
    };
    ReadResult read_with_state(std::string_view id, std::uint64_t offset, std::size_t length) const;

private:
    friend class ReplicatedChunkClient;
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

struct ReplicaReply {
    std::string endpoint;
    std::string acknowledgement;
    std::exception_ptr error; // RpcError, RemoteChunkError or local exception, if any.
};

struct ReplicaReadResult {
    std::string endpoint;
    butil::IOBuf data;
    // Legacy immutable flag: true for both user Freeze and repair Seal.
    std::optional<bool> frozen;
    std::exception_ptr error; // Per-replica failure; data is empty and state unset.
    std::optional<State> state;
};

class ReplicaCommitError : public std::runtime_error {
public:
    explicit ReplicaCommitError(std::vector<ReplicaReply> replies,
                                std::string message = "replica commit incomplete");
    const std::vector<ReplicaReply>& replies() const noexcept { return replies_; }
private:
    std::vector<ReplicaReply> replies_;
};

// Synchronous all-replica writes. Share across callers and assign offsets externally.
class ReplicatedChunkClient {
public:
    explicit ReplicatedChunkClient(const std::vector<std::string>& endpoints, ClientOptions options = {});
    ReplicatedChunkClient(const ReplicatedChunkClient&) = delete;
    ReplicatedChunkClient& operator=(const ReplicatedChunkClient&) = delete;

    State create_chunk(std::string_view id, std::uint64_t capacity) const;
    AppendResult append(std::string_view id, std::uint64_t offset, std::string_view data,
                        std::uint32_t wait_timeout_ms) const;
    AppendResult append(std::string_view id, std::uint64_t offset, const butil::IOBuf& data,
                        std::uint32_t wait_timeout_ms) const;
    std::uint64_t freeze_chunk(std::string_view id, std::uint64_t expected_length) const;
    // One immutable (frozen/sealed) reply or a strict majority of identical reads.
    butil::IOBuf read_chunk(std::string_view id, std::uint64_t offset, std::size_t length) const;
    // Read every replica concurrently, retaining every result in endpoint order.
    std::vector<ReplicaReadResult> read_replicas(std::string_view id, std::uint64_t offset,
                                              std::size_t length) const;

private:
    struct Node {
        std::string endpoint;
        std::unique_ptr<ChunkClient> client;
    };
    std::vector<Node> nodes_;
    template<class Operation> auto commit(const Operation& operation) const;
};

} // namespace chunkstore
