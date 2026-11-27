#include "space_client_validation.hpp"
#include "unit.hpp"

#include <cerrno>
#include <functional>

namespace wire = fstore::master::v1;
namespace detail = chunkstore::detail;
using chunkstore::ChunkId;
using unit::check;
namespace {
template<class F> void rejected(F&& operation) {
    check(unit::expect<chunkstore::master::RpcError>(operation).code() == EPROTO, "malformed response returned wrong error code");
}
wire::ChunkRoute route(std::uint64_t cursor = 10, std::uint64_t sequence = 7, unsigned space = 3) {
    const ChunkId id{sequence, static_cast<std::uint8_t>(space), chunkstore::unhash_chunk_index(cursor)};
    wire::ChunkRoute result;
    result.set_chunk_id(id.str());
    auto* spec = result.mutable_space();
    spec->set_name("logs"); spec->set_id(space); spec->set_total_pgs(chunkstore::global_pg_count); spec->set_bound_pgs(200);
    spec->mutable_spec()->set_replicas(2); spec->mutable_spec()->set_chunk_size(65536);
    auto* pg = result.mutable_pg(); pg->set_active(true); pg->set_id(id.pg());
    for (unsigned i = 0; i < 2; ++i) {
        auto* replica = pg->add_replicas(); replica->set_server_id("s" + std::to_string(i));
        replica->set_endpoint("127.0.0.1:" + std::to_string(8100 + i));
    }
    return result;
}

void route_validation() {
    const auto good = route();
    const auto parsed = detail::validate_route(good);
    check(parsed.id.str() == good.chunk_id() && parsed.endpoints == std::vector<std::string>{"127.0.0.1:8100", "127.0.0.1:8101"},
          "valid route lost ID or preferred replica order");
    using Mutation = std::function<void(wire::ChunkRoute&)>;
    const std::vector<Mutation> invalid{
        [](auto& r) { r.clear_space(); }, [](auto& r) { r.clear_pg(); },
        [](auto& r) { r.mutable_pg()->set_active(false); },
        [](auto& r) { r.mutable_space()->mutable_spec()->set_replicas(0); },
        [](auto& r) { r.mutable_space()->mutable_spec()->set_replicas(1); },
        [](auto& r) { r.mutable_space()->mutable_spec()->set_replicas(17); },
        [](auto& r) { r.mutable_space()->mutable_spec()->set_chunk_size(0); },
        [](auto& r) { r.mutable_space()->mutable_spec()->set_chunk_size(static_cast<std::uint64_t>(INT64_MAX) + 1); },
        [](auto& r) { r.mutable_space()->set_total_pgs(1000000); },
        [](auto& r) { r.mutable_space()->set_id(4); },
        [](auto& r) { r.mutable_pg()->set_id(11); },
        [](auto& r) { r.mutable_pg()->set_id(chunkstore::global_pg_count); },
        [](auto& r) { r.set_chunk_id(std::string(24, '0')); },
        [](auto& r) { r.set_chunk_id(std::string(30, 'A')); },
        [](auto& r) { r.set_chunk_id(ChunkId{0, 3, chunkstore::unhash_chunk_index(10)}.str()); },
        [](auto& r) { r.mutable_pg()->mutable_replicas(0)->clear_server_id(); },
        [](auto& r) { r.mutable_pg()->mutable_replicas(0)->clear_endpoint(); },
        [](auto& r) { r.mutable_pg()->mutable_replicas(1)->set_server_id("s0"); },
        [](auto& r) { r.mutable_pg()->mutable_replicas(1)->set_endpoint("127.0.0.1:8100"); }
    };
    for (const auto& mutate : invalid) { auto broken = good; mutate(broken); rejected([&] { detail::validate_route(broken); }); }
    auto largest = good; largest.mutable_space()->mutable_spec()->set_chunk_size(INT64_MAX);
    detail::validate_route(largest);
}

wire::AllocateBlobsResponse blobs() {
    wire::AllocateBlobsResponse response;
    for (unsigned seq = 1; seq <= 2; ++seq) {
        auto* blob = response.add_blobs(); blob->set_seq_id(seq); blob->set_space_id(255); blob->set_start_cursor(9999999);
    }
    return response;
}
void blob_boundaries() {
    auto good = blobs(); detail::validate_blobs(good, 2);
    good.mutable_blobs(1)->set_seq_id(ChunkId::max_sequence); detail::validate_blobs(good, 2);
    rejected([&] { detail::validate_blobs(good, 1); });
    using Mutation = std::function<void(wire::Blob&)>;
    for (const auto& mutate : std::vector<Mutation>{
             [](auto& b) { b.set_seq_id(0); }, [](auto& b) { b.set_seq_id(ChunkId::max_sequence + 1); },
             [](auto& b) { b.set_space_id(256); }, [](auto& b) { b.set_start_cursor(10000000); }}) {
        auto broken = good; mutate(*broken.mutable_blobs(1)); rejected([&] { detail::validate_blobs(broken, 2); });
    }
    auto duplicate = good; duplicate.mutable_blobs(1)->set_seq_id(duplicate.blobs(0).seq_id());
    rejected([&] { detail::validate_blobs(duplicate, 2); });
}
void mixed_blob_spaces() {
    auto mixed = blobs(); mixed.mutable_blobs(1)->set_space_id(1);
    rejected([&] { detail::validate_blobs(mixed, 2); });
}
wire::AllocateChunksResponse batch() {
    wire::AllocateChunksResponse result;
    *result.add_chunks() = route(10); *result.add_chunks() = route(10000010);
    result.set_next_cursor(10000011);
    return result;
}
void batch_progression() {
    const auto good = batch();
    check(detail::validate_chunk_batch(good, "logs", 7, 10, 2).size() == 2, "valid sparse batch rejected");
    detail::validate_chunk_batch(good, "logs", 7, std::nullopt, 2);
    rejected([&] { detail::validate_chunk_batch(good, "logs", 7, 11, 2); });
    rejected([&] { detail::validate_chunk_batch(good, "other", 7, 0, 2); });
    rejected([&] { detail::validate_chunk_batch(good, "logs", 8, 0, 2); });
    rejected([&] { detail::validate_chunk_batch(good, "logs", 7, 0, 3); });
    auto reversed = good; reversed.mutable_chunks()->SwapElements(0, 1);
    rejected([&] { detail::validate_chunk_batch(reversed, "logs", 7, 0, 2); });
    auto duplicate = good; *duplicate.mutable_chunks(1) = duplicate.chunks(0);
    rejected([&] { detail::validate_chunk_batch(duplicate, "logs", 7, 0, 2); });
    auto backwards = good; backwards.set_next_cursor(10000010);
    rejected([&] { detail::validate_chunk_batch(backwards, "logs", 7, 0, 2); });
    auto broken = good; broken.mutable_chunks(1)->mutable_pg()->set_active(false);
    rejected([&] { detail::validate_chunk_batch(broken, "logs", 7, 0, 2); });
}
void cursor_exhaustion() {
    wire::AllocateChunksResponse response;
    *response.add_chunks() = route(UINT64_MAX);
    check(detail::validate_chunk_batch(response, "logs", 7, UINT64_MAX, 1).size() == 1, "last valid cursor rejected");
    response.set_next_cursor(0);
    rejected([&] { detail::validate_chunk_batch(response, "logs", 7, 0, 1); });
    response.clear_next_cursor(); *response.add_chunks() = route(0);
    rejected([&] { detail::validate_chunk_batch(response, "logs", 7, 0, 2); });
    auto sparse = batch(); sparse.clear_next_cursor();
    detail::validate_chunk_batch(sparse, "logs", 7, 0, 2); // Exhaustion can also follow the last bound PG before UINT64_MAX.
}
void mixed_chunk_spaces() {
    auto mixed = batch(); *mixed.mutable_chunks(1) = route(10000010, 7, 4);
    rejected([&] { detail::validate_chunk_batch(mixed, "logs", 7, 0, 2); });
}
void mixed_chunk_specs() {
    using Mutation = std::function<void(wire::Space&)>;
    for (const auto& mutate : std::vector<Mutation>{
             [](auto& s) { s.mutable_spec()->set_chunk_size(32768); },
             [](auto& s) { s.set_bound_pgs(400); },
             [](auto& s) { s.mutable_spec()->mutable_placement_policy()->set_mode(wire::PlacementPolicy::BALANCED_DATACENTERS); },
             [](auto& s) { s.mutable_spec()->mutable_placement_policy()->set_min_datacenters(2); },
             [](auto& s) { s.mutable_spec()->mutable_placement_policy()->add_datacenters("a"); }}) {
        auto mixed = batch(); mutate(*mixed.mutable_chunks(1)->mutable_space());
        rejected([&] { detail::validate_chunk_batch(mixed, "logs", 7, 0, 2); });
    }
}
void locate_validation() {
    wire::LocateChunkResponse response;
    const auto good = route();
    rejected([&] { detail::validate_located_chunk(response, "logs", good.chunk_id()); });
    *response.mutable_chunk() = good;
    check(detail::validate_located_chunk(response, "logs", good.chunk_id()).id.str() == good.chunk_id(), "valid lookup failed");
    rejected([&] { detail::validate_located_chunk(response, "other", good.chunk_id()); });
    rejected([&] { detail::validate_located_chunk(response, "logs", route(11).chunk_id()); });
    response.mutable_chunk()->mutable_pg()->set_active(false);
    rejected([&] { detail::validate_located_chunk(response, "logs", good.chunk_id()); });
}
}
int main() {
    return unit::run({{"route_validation", route_validation}, {"blob_boundaries", blob_boundaries},
        {"mixed_blob_spaces", mixed_blob_spaces}, {"batch_progression", batch_progression}, {"cursor_exhaustion", cursor_exhaustion},
        {"mixed_chunk_spaces", mixed_chunk_spaces}, {"mixed_chunk_specs", mixed_chunk_specs}, {"locate_validation", locate_validation}});
}
