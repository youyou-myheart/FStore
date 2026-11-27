#pragma once

#include "chunk_id.hpp"
#include "fmaster_client.hpp"
#include <optional>
#include <vector>

namespace chunkstore::detail {
struct ValidatedRoute {
    ChunkId id;
    std::vector<std::string> endpoints;
};
ValidatedRoute validate_route(const master::wire::ChunkRoute& route);
void validate_blobs(const master::wire::AllocateBlobsResponse& response, std::uint32_t count);
std::vector<ValidatedRoute> validate_chunk_batch(const master::wire::AllocateChunksResponse& response,
    std::string_view space, std::uint64_t sequence, std::optional<std::uint64_t> start, std::uint32_t count);
ValidatedRoute validate_located_chunk(const master::wire::LocateChunkResponse& response,
                                    std::string_view space, std::string_view id);
} // namespace chunkstore::detail
