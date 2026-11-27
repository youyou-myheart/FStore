#include "fmaster_fixture.hpp"
#include "chunk_id.hpp"
#include "unit.hpp"

#include <filesystem>
#include <set>
#include <unistd.h>

namespace wire = fstore::master::v1;
namespace master = chunkstore::master;
using namespace fmaster_test;
using unit::check;
namespace {
struct TemporaryRoot {
    std::filesystem::path path;
    TemporaryRoot() {
        char pattern[] = "/tmp/fstore-batch-ut-XXXXXX";
        if (!::mkdtemp(pattern)) throw std::runtime_error("mkdtemp failed");
        path = pattern;
    }
    ~TemporaryRoot() { std::error_code ignored; std::filesystem::remove_all(path, ignored); }
};
struct Fixture {
    TemporaryRoot root;
    master::Store store{root.path, {}, std::chrono::minutes(5)};
    Fixture() {
        node(store, 1, "a"); node(store, 2, "a"); node(store, 3, "a");
        wire::SpaceResponse response;
        store.create_space(creation("logs", wire::PlacementPolicy::SINGLE_DATACENTER), response);
    }
    void only_pg_zero() {
        wire::BindPGsRequest remove; remove.set_name("logs"); remove.set_start(1); remove.set_count(199);
        wire::SpaceResponse response; store.unbind_pgs(remove, response);
    }
};

template<class F> void rejected_unchanged(wire::AllocateChunksResponse& response, wire::Code code, F&& operation) {
    response.set_revision(123456);
    response.clear_chunks(); response.add_chunks()->set_chunk_id("sentinel");
    const auto before = response.SerializeAsString();
    check(unit::expect<master::Error>(operation).code() == code, "wrong batch failure code");
    check(response.SerializeAsString() == before, "failed batch exposed a partial response");
}

void repeated_pg_and_continuation() {
    Fixture f; f.only_pg_zero();
    const auto sequence = blob(f.store, "logs");
    const auto first = chunks(f.store, "logs", sequence, 0, 1024);
    const auto expected_pg = groups(f.store, "logs", 0, 1).pgs(0).SerializeAsString();
    std::set<std::string> ids;
    for (const auto& chunk : first.chunks()) {
        const auto decoded = chunkstore::ChunkId::parse(chunk.chunk_id());
        check(decoded.seq_id == sequence && decoded.pg() == 0, "repeated PG received the wrong Blob or index");
        check(chunk.pg().SerializeAsString() == expected_pg, "reused route differs from authoritative placement");
        check(ids.insert(chunk.chunk_id()).second, "cached route reused a Chunk ID");
    }
    check(first.has_next_cursor(), "missing continuation on repeated-PG batch");
    const auto next = chunks(f.store, "logs", sequence, first.next_cursor(), 1024);
    for (const auto& chunk : next.chunks()) check(ids.insert(chunk.chunk_id()).second, "continuation reused an index");
    check(ids.size() == 2048, "batch count was truncated");
    check(first.SerializeAsString() == chunks(f.store, "logs", sequence, 0, 1024).SerializeAsString(), "retry changed a batch");
}

void health_is_rechecked_between_requests() {
    Fixture f; f.only_pg_zero();
    wire::AllocateChunksRequest request; request.set_name("logs"); request.set_seq_id(blob(f.store, "logs")); request.set_count(1024);
    wire::AllocateChunksResponse response; f.store.allocate_chunks(request, response);
    const auto before = response.SerializeAsString();
    wire::SetServerStateRequest disable; disable.set_id("s1"); disable.set_disabled(true); f.store.set_server_state(disable);
    rejected_unchanged(response, wire::CONFLICT, [&] { f.store.allocate_chunks(request, response); });
    node(f.store, 1, "a"); // Re-enable and provide a fresh heartbeat without TCP.
    f.store.allocate_chunks(request, response);
    check(response.SerializeAsString() == before, "healthy retry did not recover original placement");
}

void later_unavailable_pg_rejects_whole_batch() {
    Fixture f;
    node(f.store, 4, "a");
    wire::BindPGsRequest bind; bind.set_name("logs"); bind.set_start(7000000); bind.set_count(100);
    wire::SpaceResponse space; f.store.bind_pgs(bind, space);
    const auto added = groups(f.store, "logs", 7000000, 100);
    int blocked_offset = -1;
    for (int i = 0; i < added.pgs_size() && blocked_offset < 0; ++i) {
        for (const auto& replica : added.pgs(i).replicas()) if (replica.server_id() == "s4") { blocked_offset = i; break; }
    }
    check(blocked_offset >= 0, "fixture never placed a replica on the added node");
    wire::AllocateChunksRequest request; request.set_name("logs"); request.set_seq_id(blob(f.store, "logs"));
    request.set_start_cursor(0); request.set_count(201 + blocked_offset);
    wire::AllocateChunksResponse response; f.store.allocate_chunks(request, response);
    check(response.chunks(0).pg().id() == 0 && response.chunks(response.chunks_size() - 1).pg().id() == static_cast<unsigned>(7000000 + blocked_offset),
          "batch did not cross the intended placement pools");
    wire::SetServerStateRequest disable; disable.set_id("s4"); disable.set_disabled(true); f.store.set_server_state(disable);
    rejected_unchanged(response, wire::CONFLICT, [&] { f.store.allocate_chunks(request, response); });
    // The earlier PGs are still usable; the failure belongs to the later range.
    request.set_count(200); f.store.allocate_chunks(request, response);
    check(response.chunks_size() == 200, "unrelated unavailable node invalidated healthy PGs");
}

void exhaustion_and_invalid_count_are_atomic() {
    Fixture f; f.only_pg_zero();
    wire::AllocateChunksRequest request; request.set_name("logs"); request.set_seq_id(blob(f.store, "logs"));
    constexpr auto last = UINT64_MAX - UINT64_MAX % chunkstore::global_pg_count;
    request.set_start_cursor(last); request.set_count(2);
    wire::AllocateChunksResponse response;
    rejected_unchanged(response, wire::CONFLICT, [&] { f.store.allocate_chunks(request, response); });
    request.set_count(1); f.store.allocate_chunks(request, response);
    check(!response.has_next_cursor() && chunkstore::hash_chunk_index(chunkstore::ChunkId::parse(response.chunks(0).chunk_id()).index) == last,
          "last valid sparse cursor was lost or wrapped");
    request.set_start_cursor(last + 1);
    rejected_unchanged(response, wire::CONFLICT, [&] { f.store.allocate_chunks(request, response); });
    request.set_start_cursor(0); request.set_count(1025);
    rejected_unchanged(response, wire::INVALID_ARGUMENT, [&] { f.store.allocate_chunks(request, response); });
}
}
int main() {
    return unit::run({{"repeated_pg_and_continuation", repeated_pg_and_continuation},
        {"fresh_health", health_is_rechecked_between_requests}, {"late_failure_atomicity", later_unavailable_pg_rejects_whole_batch},
        {"exhaustion_atomicity", exhaustion_and_invalid_count_are_atomic}});
}
