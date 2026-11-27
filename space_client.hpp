#pragma once

#include "chunk_client.hpp"
#include "chunk_id.hpp"
#include "fmaster_client.hpp"

namespace chunkstore {

// Retain the handle for repeated I/O. Reads and writes use its reusable bRPC
// replica client directly and make no FMaster calls.
class SpaceChunk {
public:
    const std::string& id() const noexcept { return route_.chunk_id(); }
    ChunkId decoded_id() const { return ChunkId::parse(id()); }
    const master::wire::ChunkRoute& route() const noexcept { return route_; }
    State create() const { return replicas_->create_chunk(id(), route_.space().spec().chunk_size()); }
    AppendResult append(std::uint64_t offset, const butil::IOBuf& data, std::uint32_t wait_ms) const {
        return replicas_->append(id(), offset, data, wait_ms);
    }
    AppendResult append(std::uint64_t offset, std::string_view data, std::uint32_t wait_ms) const {
        return replicas_->append(id(), offset, data, wait_ms);
    }
    butil::IOBuf read(std::uint64_t offset, std::size_t length) const {
        return replicas_->read_chunk(id(), offset, length);
    }
    std::vector<ReplicaReadResult> read_replicas(std::uint64_t offset, std::size_t length) const {
        return replicas_->read_replicas(id(), offset, length);
    }
    std::uint64_t freeze(std::uint64_t expected_length) const {
        return replicas_->freeze_chunk(id(), expected_length);
    }
    std::uint64_t sync(std::optional<std::uint64_t> expected_length = std::nullopt) const {
        return replicas_->sync_chunk(id(), expected_length);
    }
private:
    friend class SpaceClient;
    SpaceChunk(master::wire::ChunkRoute route, std::shared_ptr<ReplicatedChunkClient> replicas);
    master::wire::ChunkRoute route_;
    std::shared_ptr<ReplicatedChunkClient> replicas_;
};

class SpaceClient {
public:
    SpaceClient(const std::string& master_endpoint, std::string space, ClientOptions options = {});
    // Allocation and creation are separate: persist/retain id() before create(),
    // so a partial replica failure can be retried with the same ID.
    SpaceChunk allocate_chunk() const;
    // Convenience: allocate a new Blob, then its first batch of Chunk indices.
    std::vector<SpaceChunk> allocate_chunks(std::uint32_t count) const;
    master::wire::Blob allocate_blob() const;
    std::vector<master::wire::Blob> allocate_blobs(std::uint32_t count) const;
    struct ChunkBatch {
        std::vector<SpaceChunk> chunks;
        std::optional<std::uint64_t> next_cursor;
    };
    struct ReplicaPoolStats {
        std::uint64_t hits = 0;
        std::uint64_t misses = 0;
        std::size_t cached_clients = 0;
    };
    // The cache is consulted only when building handles, never on handle I/O.
    ReplicaPoolStats replica_pool_stats() const;
    // First batch: spread the starting PG across the Space's current bindings.
    ChunkBatch allocate_chunks(std::uint64_t seq_id, std::uint32_t count) const;
    // Reuse seq_id for all chunks in a Blob. Retain returned IDs and next_cursor.
    ChunkBatch allocate_chunks(std::uint64_t seq_id, std::uint64_t start_cursor, std::uint32_t count) const;
    SpaceChunk open_chunk(std::string_view id) const;
private:
    struct ReplicaPool;
    SpaceChunk make_chunk(master::wire::ChunkRoute route, const std::vector<std::string>& endpoints) const;
    ChunkBatch allocate_chunks_impl(std::uint64_t seq_id, std::optional<std::uint64_t> start_cursor,
                                   std::uint32_t count) const;
    master::Client master_;
    std::string space_;
    ClientOptions options_;
    std::shared_ptr<ReplicaPool> pool_;
};
} // namespace chunkstore
