#include "fmaster_store.hpp"
#include "fmaster_fixture.hpp"
#include "fmaster_placement.hpp"
#include "chunk_id.hpp"
#include <butil/crc32c.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <thread>
#include <mutex>
#include <unistd.h>

namespace master = chunkstore::master;
namespace wire = fstore::master::v1;
namespace fs = std::filesystem;
using namespace fmaster_test;

void check(bool value, const char* reason) { if (!value) throw std::runtime_error(reason); }
template<class F> void rejected(F operation) {
    try { operation(); } catch (const master::Error&) { return; }
    throw std::runtime_error("operation unexpectedly succeeded");
}

void verify(const fs::path& root) {
    std::string original_pg, original_id;
    std::uint64_t highest_blob = 0;
    std::set<std::string> issued;
    {
        master::Store store(root, {}, std::chrono::minutes(5));
        node(store, 1, "a"); node(store, 2, "a"); node(store, 3, "b");
        wire::SpaceResponse response;
        auto request = creation("logs", wire::PlacementPolicy::BALANCED_DATACENTERS);
        store.create_space(request, response);
        check(response.revision() == 2 && response.space().bound_pgs() == 200 && response.space().total_pgs() == 10000000,
              "incorrect global PG defaults");
        store.create_space(request, response);
        check(response.revision() == 2, "idempotent creation changed revision");
        auto conflict = request; conflict.mutable_spec()->set_chunk_size(1);
        rejected([&] { store.create_space(conflict, response); });
        conflict = creation("strict", wire::PlacementPolicy::DISTINCT_DATACENTERS);
        rejected([&] { store.create_space(conflict, response); });
        conflict = creation("single", wire::PlacementPolicy::SINGLE_DATACENTER);
        rejected([&] { store.create_space(conflict, response); });
        const auto before = groups(store, "logs", 0, 201);
        original_pg = before.pgs(0).SerializeAsString();
        check(!before.pgs(200).active(), "unbound PG returned as bound");
        for (unsigned pg = 0; pg < 200; ++pg) {
            std::set<std::string> servers, dcs;
            for (const auto& replica : before.pgs(pg).replicas()) { servers.insert(replica.server_id()); dcs.insert(replica.datacenter()); }
            check(servers.size() == 3 && dcs.size() == 2, "placement policy violated");
        }
        store.create_space(creation("other", wire::PlacementPolicy::BALANCED_DATACENTERS), response);
        check(groups(store, "other", 200, 1).pgs(0).active(), "creation did not use globally free PGs");
        check(!groups(store, "logs", 200, 1).pgs(0).active(), "PG ownership is not exclusive");
        wire::BindPGsRequest bind;
        bind.set_name("logs"); bind.set_start(199); bind.set_count(2);
        rejected([&] { store.bind_pgs(bind, response); });
        auto batch = allocate(store, "logs", 401);
        const auto seq = chunkstore::ChunkId::parse(batch.chunks(0).chunk_id()).seq_id;
        for (int i = 0; i < batch.chunks_size(); ++i) {
            const auto& chunk = batch.chunks(i);
            const auto id = chunkstore::ChunkId::parse(chunk.chunk_id());
            check(id.seq_id == seq && id.pg() == static_cast<unsigned>(i % 200), "index hash routing is incorrect");
            check(chunk.pg().id() == id.pg() && issued.insert(chunk.chunk_id()).second, "duplicate or misrouted Chunk");
        }
        original_id = batch.chunks(0).chunk_id();
        check(batch.has_next_cursor(), "missing allocation cursor");
        check(chunks(store, "logs", seq, 0, 401).SerializeAsString() == batch.SerializeAsString(), "planning is not deterministic");
        const auto continuation = chunks(store, "logs", seq, batch.next_cursor(), 100);
        for (const auto& chunk : continuation.chunks()) check(issued.insert(chunk.chunk_id()).second, "cursor reused index");
        rejected([&] { chunks(store, "other", seq, 0, 1); });
        node(store, 4, "c");
        bind.set_start(7000000); bind.set_count(200); bind.set_expected_revision(response.revision());
        store.bind_pgs(bind, response);
        check(response.space().bound_pgs() == 400, "noncontiguous binding count incorrect");
        check(groups(store, "logs", 0, 1).pgs(0).SerializeAsString() == original_pg, "binding moved old PG");
        check(groups(store, "logs", 7000000, 1).pgs(0).active(), "arbitrary global PG not bound");
        rejected([&] { store.bind_pgs(bind, response); }); // Stale CAS.
        bind.clear_expected_revision();
        const auto revision = response.revision(); store.bind_pgs(bind, response);
        check(response.revision() == revision, "idempotent bind changed revision");
        rejected([&] { store.unbind_pgs(bind, response); });
        const auto distant = chunks(store, "logs", seq, 7000000, 200);
        check(distant.chunks(0).pg().id() == 7000000, "cursor did not select distant binding");
        wire::ListPGsRequest page; page.set_name("logs"); page.set_start(199); page.set_limit(2);
        wire::ListPGsResponse listed; store.list_pgs(page, listed);
        check(listed.pgs(0).id() == 199 && listed.pgs(1).id() == 7000000 && listed.has_next_start(), "bound PG pagination failed");
        // Reclaim unused ownership, then automatic creation fills global holes.
        bind.set_name("other"); bind.set_start(200); bind.set_count(50);
        store.unbind_pgs(bind, response);
        check(response.space().bound_pgs() == 150, "partial unbind failed");
        store.create_space(creation("strict", wire::PlacementPolicy::DISTINCT_DATACENTERS), response);
        check(groups(store, "strict", 200, 1).pgs(0).active(), "free PG hole was not reused");
        check(groups(store, "strict", 400, 1).pgs(0).active(), "fragmented default binding failed");
        auto strict = groups(store, "strict", 200, 1);
        std::set<std::string> dcs;
        for (const auto& replica : strict.pgs(0).replicas()) dcs.insert(replica.datacenter());
        check(dcs.size() == 3, "distinct datacenter policy failed");
        const auto other_blob = blob(store, "strict");
        check(other_blob != seq, "Blob sequence collided between Spaces");
        wire::ListPGPoolRequest pool_request; pool_request.set_start(198); pool_request.set_limit(4);
        wire::ListPGPoolResponse pool; store.list_pg_pool(pool_request, pool);
        check(pool.total_pgs() == 10000000 && pool.bound_pgs() == 750, "global pool counters incorrect");
        check(pool.pgs(0).space_name() == "logs" && pool.pgs(2).space_name() == "strict", "global ownership view incorrect");
        wire::RemoveServerRequest remove; remove.set_id("s1");
        rejected([&] { store.remove_server(remove); });
        std::mutex output;
        std::exception_ptr error;
        std::vector<std::thread> workers;
        for (int i = 0; i < 8; ++i) workers.emplace_back([&] {
            try {
                const auto allocated = allocate(store, "logs", 128);
                std::lock_guard lock(output);
                for (const auto& chunk : allocated.chunks()) {
                    check(issued.insert(chunk.chunk_id()).second, "concurrent duplicate Chunk ID");
                    highest_blob = std::max(highest_blob, chunkstore::ChunkId::parse(chunk.chunk_id()).seq_id);
                }
            } catch (...) { std::lock_guard lock(output); error = std::current_exception(); }
        });
        for (auto& worker : workers) worker.join();
        if (error) std::rethrow_exception(error);
    }
    {
        master::Store store(root, {}, std::chrono::minutes(5));
        check(groups(store, "logs", 0, 1).pgs(0).SerializeAsString() == original_pg, "restart moved PG");
        wire::LocateChunkRequest request; request.set_name("logs"); request.set_chunk_id(original_id);
        wire::LocateChunkResponse located; store.locate_chunk(request, located);
        check(located.chunk().pg().SerializeAsString() == original_pg, "restart lost routing");
        node(store, 1, "a"); node(store, 2, "a"); node(store, 3, "b"); node(store, 4, "c");
        const auto next = allocate(store, "logs", 1).chunks(0);
        check(chunkstore::ChunkId::parse(next.chunk_id()).seq_id > highest_blob && !issued.contains(next.chunk_id()), "recovery reused Blob sequence");
    }
    std::fstream snapshot(root / "spaces.pb", std::ios::binary | std::ios::in | std::ios::out);
    snapshot.seekp(16); snapshot.put('!'); snapshot.close();
    rejected([&] { master::Store damaged(root); });
}

void write_catalog(const fs::path& root, const wire::SpaceCatalog& catalog, const std::string& magic) {
    fs::create_directories(root);
    const auto payload = catalog.SerializeAsString();
    std::string bytes = magic;
    for (const auto value : {static_cast<std::uint32_t>(payload.size()), butil::crc32c::Value(payload.data(), payload.size())}) {
        for (int shift = 24; shift >= 0; shift -= 8) bytes.push_back(static_cast<char>(value >> shift));
    }
    bytes += payload;
    std::ofstream output(root / "spaces.pb", std::ios::binary);
    output.write(bytes.data(), bytes.size());
    check(bool(output), "catalog fixture write failed");
}

void verify_limits(const fs::path& root) {
    {
        master::Store store(root / "space-limit", {}, std::chrono::minutes(5));
        node(store, 1, "a"); node(store, 2, "a"); node(store, 3, "a");
        for (unsigned id = 0; id <= 255; ++id) {
            wire::SpaceResponse response;
            store.create_space(creation("s" + std::to_string(id), wire::PlacementPolicy::SINGLE_DATACENTER), response);
            check(response.space().id() == id && response.space().bound_pgs() == 200, "8-bit Space/default PG binding incorrect");
        }
        wire::SpaceResponse response;
        rejected([&] { store.create_space(creation("overflow", wire::PlacementPolicy::SINGLE_DATACENTER), response); });
    }
    {
        master::Store store(root / "pool-limit", {}, std::chrono::minutes(5));
        node(store, 1, "a"); node(store, 2, "a"); node(store, 3, "a");
        wire::SpaceResponse response;
        store.create_space(creation("all", wire::PlacementPolicy::SINGLE_DATACENTER), response);
        wire::BindPGsRequest bind; bind.set_name("all"); bind.set_start(200); bind.set_count(10000000 - 200);
        store.bind_pgs(bind, response);
        check(response.space().bound_pgs() == 10000000 && fs::file_size(root / "pool-limit" / "spaces.pb") < 16384,
              "global pool limit or compact metadata failed");
        rejected([&] { store.create_space(creation("no-room", wire::PlacementPolicy::SINGLE_DATACENTER), response); });
        const auto seq = blob(store, "all");
        auto last = chunks(store, "all", seq, UINT64_MAX, 1);
        check(!last.has_next_cursor() && chunkstore::hash_chunk_index(chunkstore::ChunkId::parse(last.chunks(0).chunk_id()).index) == UINT64_MAX,
              "64-bit cursor boundary failed");
        rejected([&] { chunks(store, "all", seq, UINT64_MAX, 2); });
        bind.set_start(10000000); bind.set_count(1);
        rejected([&] { store.bind_pgs(bind, response); });
    }
    // Exercise the final two 48-bit Blob sequences without wrapping.
    const auto file = root / "pool-limit" / "spaces.pb";
    std::ifstream saved(file, std::ios::binary);
    const std::string bytes((std::istreambuf_iterator<char>(saved)), {});
    wire::SpaceCatalog near_limit;
    check(near_limit.ParseFromString(bytes.substr(16)), "fixture decode failed");
    near_limit.set_reserved_blob_sequence(chunkstore::ChunkId::max_sequence - 2);
    near_limit.mutable_spaces(0)->mutable_blob_sequences(0)->set_end(chunkstore::ChunkId::max_sequence - 2);
    write_catalog(root / "pool-limit", near_limit, "FSPACE03");
    {
        master::Store store(root / "pool-limit");
        wire::AllocateBlobsRequest request; request.set_name("all"); request.set_count(2);
        wire::AllocateBlobsResponse response; store.allocate_blobs(request, response);
        check(response.blobs(1).seq_id() == chunkstore::ChunkId::max_sequence, "48-bit sequence boundary truncated");
        rejected([&] { store.allocate_blobs(request, response); });
    }
    wire::SpaceCatalog legacy; legacy.set_revision(1); legacy.set_next_space_id(1);
    write_catalog(root / "empty-legacy", legacy, "FSPACE02");
    { master::Store upgraded(root / "empty-legacy"); }
    std::ifstream input(root / "empty-legacy" / "spaces.pb", std::ios::binary);
    char magic[8]; input.read(magic, 8);
    check(std::string(magic, 8) == "FSPACE04", "empty legacy catalog not upgraded");
    legacy.add_spaces()->mutable_space()->set_name("old-space");
    write_catalog(root / "allocated-legacy", legacy, "FSPACE02");
    rejected([&] { master::Store unsupported(root / "allocated-legacy"); });
}

void verify_distribution(const fs::path& root) {
    master::Store store(root, {}, std::chrono::minutes(5));
    node(store, 1, "a"); node(store, 2, "a"); node(store, 3, "a");
    wire::SpaceResponse space;
    store.create_space(creation("spread", wire::PlacementPolicy::SINGLE_DATACENTER), space);
    wire::BindPGsRequest bind; bind.set_name("spread"); bind.set_start(7000000); bind.set_count(200);
    store.bind_pgs(bind, space);
    wire::AllocateBlobsRequest blobs; blobs.set_name("spread"); blobs.set_count(1000);
    std::map<std::uint64_t, unsigned> histogram;
    wire::Blob retained;
    for (unsigned batch = 0; batch < 20; ++batch) {
        wire::AllocateBlobsResponse response;
        store.allocate_blobs(blobs, response);
        for (const auto& item : response.blobs()) {
            const auto start = item.start_cursor();
            check(start < 200 || (start >= 7000000 && start < 7000200), "default cursor is outside owned PGs");
            ++histogram[start];
        }
        retained = response.blobs(0);
        wire::AllocateChunksRequest request; request.set_name("spread"); request.set_seq_id(retained.seq_id());
        wire::AllocateChunksResponse automatic; store.allocate_chunks(request, automatic);
        check(automatic.chunks(0).pg().id() == retained.start_cursor(), "omitted cursor did not use Blob start");
        check(chunks(store, "spread", retained.seq_id(), retained.start_cursor(), 1).SerializeAsString() == automatic.SerializeAsString(),
              "explicit Blob cursor and automatic allocation disagree");
        check(chunks(store, "spread", retained.seq_id(), 0, 1).chunks(0).pg().id() == 0,
              "explicit zero cursor no longer starts at PG zero");
    }
    check(histogram.size() == 400, "single-Chunk Blobs do not reach every owned PG");
    for (const auto& [pg, count] : histogram) {
        (void)pg;
        check(count >= 20 && count <= 85, "small-Blob placement is badly skewed across sparse PG bindings");
    }
    const auto before = chunks(store, "spread", retained.seq_id(), retained.start_cursor(), 1).chunks(0);
    bind.set_start(8000000); bind.set_count(200); store.bind_pgs(bind, space);
    const auto after = chunks(store, "spread", retained.seq_id(), retained.start_cursor(), 1).chunks(0);
    check(before.chunk_id() == after.chunk_id() && before.pg().SerializeAsString() == after.pg().SerializeAsString(),
          "retained initial cursor changed the first Chunk after new bindings");
}

int main() {
    char path[] = "/tmp/fstore-spaces-XXXXXX";
    if (!::mkdtemp(path)) return 1;
    try { verify(path); verify_limits(fs::path(path) / "limits"); verify_distribution(fs::path(path) / "distribution"); }
    catch (const std::exception& error) { std::cerr << error.what() << '\n'; fs::remove_all(path); return 1; }
    fs::remove_all(path);
}
