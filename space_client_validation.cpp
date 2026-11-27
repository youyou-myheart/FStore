#include "space_client_validation.hpp"

#include <algorithm>
#include <cerrno>
#include <set>

namespace chunkstore::detail {
namespace {
bool same_space(const master::wire::Space& a, const master::wire::Space& b) {
    const auto& x = a.spec().placement_policy();
    const auto& y = b.spec().placement_policy();
    return a.id() == b.id() && a.bound_pgs() == b.bound_pgs() &&
        a.spec().replicas() == b.spec().replicas() && a.spec().chunk_size() == b.spec().chunk_size() &&
        x.mode() == y.mode() && x.min_datacenters() == y.min_datacenters() &&
        std::equal(x.datacenters().begin(), x.datacenters().end(), y.datacenters().begin(), y.datacenters().end());
}
}
ValidatedRoute validate_route(const master::wire::ChunkRoute& route) {
    if (!route.has_space() || !route.has_pg() || !route.pg().active() ||
        route.pg().replicas_size() != static_cast<int>(route.space().spec().replicas()) ||
        route.space().spec().replicas() == 0 || route.space().spec().replicas() > 16 ||
        route.space().total_pgs() != global_pg_count || route.space().spec().chunk_size() == 0 ||
        route.space().spec().chunk_size() > static_cast<std::uint64_t>(INT64_MAX) || route.pg().id() >= global_pg_count) {
        throw master::RpcError(EPROTO, "invalid space route response");
    }
    ChunkId id{};
    try { id = ChunkId::parse(route.chunk_id()); }
    catch (const std::invalid_argument&) { throw master::RpcError(EPROTO, "invalid routed chunk ID"); }
    if (!id.seq_id || id.space_id != route.space().id() || id.pg() != route.pg().id()) {
        throw master::RpcError(EPROTO, "chunk ID and Space/PG do not match");
    }
    std::vector<std::string> endpoints;
    std::set<std::string> servers, unique_endpoints;
    for (const auto& target : route.pg().replicas()) {
        if (target.server_id().empty() || target.endpoint().empty() ||
            !servers.insert(target.server_id()).second || !unique_endpoints.insert(target.endpoint()).second) {
            throw master::RpcError(EPROTO, "invalid duplicate replica route");
        }
        endpoints.push_back(target.endpoint());
    }
    return {id, std::move(endpoints)};
}

void validate_blobs(const master::wire::AllocateBlobsResponse& response, std::uint32_t count) {
    if (response.blobs_size() != static_cast<int>(count)) throw master::RpcError(EPROTO, "incomplete Blob allocation");
    std::set<std::uint64_t> seen;
    for (const auto& blob : response.blobs()) {
        if (!blob.seq_id() || blob.seq_id() > ChunkId::max_sequence || blob.space_id() > 255 ||
            blob.start_cursor() >= global_pg_count || blob.space_id() != response.blobs(0).space_id() ||
            !seen.insert(blob.seq_id()).second) {
            throw master::RpcError(EPROTO, "invalid Blob allocation");
        }
    }
}

std::vector<ValidatedRoute> validate_chunk_batch(const master::wire::AllocateChunksResponse& response,
    std::string_view space, std::uint64_t sequence, std::optional<std::uint64_t> start, std::uint32_t count) {
    if (response.chunks_size() != static_cast<int>(count)) throw master::RpcError(EPROTO, "incomplete chunk allocation");
    std::vector<ValidatedRoute> result;
    result.reserve(count);
    std::uint64_t previous = start.value_or(0);
    bool exhausted = false;
    for (const auto& route : response.chunks()) {
        if (route.space().name() != space) throw master::RpcError(EPROTO, "unexpected space in allocation");
        auto validated = validate_route(route);
        if (!same_space(route.space(), response.chunks(0).space())) {
            throw master::RpcError(EPROTO, "inconsistent Space specification in allocation");
        }
        const auto cursor = hash_chunk_index(validated.id.index);
        if (exhausted || validated.id.seq_id != sequence || cursor < previous) {
            throw master::RpcError(EPROTO, "unexpected Blob sequence or index");
        }
        exhausted = cursor == UINT64_MAX;
        previous = exhausted ? cursor : cursor + 1;
        result.push_back(std::move(validated));
    }
    if (response.has_next_cursor() && (exhausted || response.next_cursor() < previous)) {
        throw master::RpcError(EPROTO, "invalid next allocation cursor");
    }
    return result;
}

ValidatedRoute validate_located_chunk(const master::wire::LocateChunkResponse& response,
                                    std::string_view space, std::string_view id) {
    if (!response.has_chunk() || response.chunk().chunk_id() != id || response.chunk().space().name() != space) {
        throw master::RpcError(EPROTO, "unexpected chunk route response");
    }
    return validate_route(response.chunk());
}
} // namespace chunkstore::detail
