#pragma once

#include "chunk.pb.h"
#include "chunk_store.hpp"

#include <butil/iobuf.h>
#include <exception>
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

class ReplicaCommitError : public std::runtime_error {
public:
    explicit ReplicaCommitError(std::vector<ReplicaReply> replies);
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

private:
    struct Node {
        std::string endpoint;
        std::unique_ptr<ChunkClient> client;
    };
    std::vector<Node> nodes_;
    template<class Operation> auto commit(const Operation& operation) const;
};

} // namespace chunkstore
