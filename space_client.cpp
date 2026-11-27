#include "space_client.hpp"
#include "replica_client_pool.hpp"
#include "space_client_validation.hpp"

namespace chunkstore {
struct SpaceClient::ReplicaPool : detail::ReplicaClientPool<ReplicatedChunkClient> {};

SpaceChunk::SpaceChunk(master::wire::ChunkRoute route, std::shared_ptr<ReplicatedChunkClient> replicas)
    : route_(std::move(route)), replicas_(std::move(replicas)) {}

SpaceClient::SpaceClient(const std::string& endpoint, std::string space, ClientOptions options)
    : master_(endpoint, options.connect_timeout_ms, options.timeout_ms), space_(std::move(space)), options_(options),
      pool_(std::make_shared<ReplicaPool>()) {
    if (space_.empty()) throw std::invalid_argument("space name is required");
}

SpaceChunk SpaceClient::make_chunk(master::wire::ChunkRoute route, const std::vector<std::string>& endpoints) const {
    auto replicas = pool_->get(endpoints, [&](const auto& targets) {
        return std::make_shared<ReplicatedChunkClient>(targets, options_);
    });
    return SpaceChunk(std::move(route), std::move(replicas));
}

SpaceClient::ReplicaPoolStats SpaceClient::replica_pool_stats() const {
    const auto stats = pool_->stats();
    return {stats.hits, stats.misses, stats.cached_clients};
}

std::vector<SpaceChunk> SpaceClient::allocate_chunks(std::uint32_t count) const {
    if (!count || count > 1024) throw std::invalid_argument("allocation count must be in 1..1024");
    const auto blob = allocate_blob();
    return allocate_chunks(blob.seq_id(), blob.start_cursor(), count).chunks;
}

std::vector<master::wire::Blob> SpaceClient::allocate_blobs(std::uint32_t count) const {
    if (!count || count > 1024) throw std::invalid_argument("Blob count must be in 1..1024");
    master::wire::AllocateBlobsRequest request;
    request.set_name(space_);
    request.set_count(count);
    const auto response = master_.allocate_blobs(request);
    detail::validate_blobs(response, count);
    return {response.blobs().begin(), response.blobs().end()};
}

master::wire::Blob SpaceClient::allocate_blob() const { return allocate_blobs(1).front(); }

SpaceClient::ChunkBatch SpaceClient::allocate_chunks(std::uint64_t seq_id, std::uint64_t start_cursor, std::uint32_t count) const {
    return allocate_chunks_impl(seq_id, start_cursor, count);
}

SpaceClient::ChunkBatch SpaceClient::allocate_chunks(std::uint64_t seq_id, std::uint32_t count) const {
    return allocate_chunks_impl(seq_id, std::nullopt, count);
}

SpaceClient::ChunkBatch SpaceClient::allocate_chunks_impl(std::uint64_t seq_id, std::optional<std::uint64_t> start_cursor,
                                                        std::uint32_t count) const {
    if (!count || count > 1024) throw std::invalid_argument("allocation count must be in 1..1024");
    if (!seq_id || seq_id > ChunkId::max_sequence) throw std::invalid_argument("invalid Blob sequence");
    master::wire::AllocateChunksRequest request;
    request.set_name(space_); request.set_count(count); request.set_seq_id(seq_id);
    if (start_cursor) request.set_start_cursor(*start_cursor);
    const auto response = master_.allocate_chunks(request);
    // Validate the entire reply before populating the client cache or building
    // handles. A malformed later route cannot leave partially accepted state.
    const auto validated = detail::validate_chunk_batch(response, space_, seq_id, start_cursor, count);
    ChunkBatch result;
    result.chunks.reserve(count);
    if (response.has_next_cursor()) result.next_cursor = response.next_cursor();
    for (std::uint32_t i = 0; i < count; ++i) {
        result.chunks.push_back(make_chunk(response.chunks(i), validated[i].endpoints));
    }
    return result;
}

SpaceChunk SpaceClient::allocate_chunk() const {
    auto result = allocate_chunks(1);
    return std::move(result.front());
}

SpaceChunk SpaceClient::open_chunk(std::string_view id) const {
    master::wire::LocateChunkRequest request;
    request.set_name(space_); request.set_chunk_id(std::string(id));
    const auto response = master_.locate_chunk(request);
    const auto validated = detail::validate_located_chunk(response, space_, id);
    return make_chunk(response.chunk(), validated.endpoints);
}
} // namespace chunkstore
