#include "chunk_store.hpp"
#include "chunk_id.hpp"
#include "unit.hpp"

#include <fstream>
#include <future>
#include <unistd.h>

using namespace chunkstore;
using unit::check;
namespace fs = std::filesystem;
namespace {
struct Directory {
    fs::path path;
    Directory() { char name[] = "/tmp/fstore-pg-inventory-XXXXXX"; if (!::mkdtemp(name)) throw std::runtime_error("mkdtemp"); path = name; }
    ~Directory() { fs::remove_all(path); }
};
std::string id(unsigned sequence, unsigned pg = 37) { return ChunkId{sequence, 1, unhash_chunk_index(pg)}.str(); }

void lifecycle() {
    Directory root;
    const auto a = id(1), b = id(2), c = id(3, 38);
    std::uint64_t deleted;
    {
        ChunkStore store(root.path);
        store.create_chunk(a, 4096); store.create_chunk(b, 4096);
        store.append(a, "data"); store.freeze_chunk(a);
        auto page = store.inventory().page(37, "", 1);
        check(page.count == 2 && page.open_count == 1 && page.chunks[0].id == a && page.chunks[0].state == State::Frozen && page.next == a, "create/freeze/page index");
        page = store.inventory().page(37, *page.next, 1);
        check(page.chunks[0].id == b && !page.next, "second inventory page");
        deleted = store.delete_chunk(a);
        check(store.inventory().chunk_count() == 1 && store.inventory().page(37, "", 10).open_count == 1, "delete index");
        store.restore_chunk(a, deleted);
        check(store.inventory().chunk_count() == 2 && store.inventory().page(37, "", 10).open_count == 1, "restore index");
        const int fd = store.open_repair_file(c);
        check(::write(fd, "copy", 4) == 4, "staging write");
        store.install_replica(c, fd, {4, 4096, State::Frozen}); ::close(fd);
        check(store.inventory().page(38, "", 10).chunks[0].state == State::Frozen, "repair installation index");
        store.create_chunk(std::string(32, 'a'), 4096);
        store.delete_chunk(b);
        std::ofstream(root.path / a.substr(0, 2) / (b + ".creating")) << "ignored";
    }
    {
        ChunkStore store(root.path);
        check(store.inventory().chunk_count() == 2 && store.inventory().legacy_count() == 1, "scan excludes trash/staging/legacy");
        const auto pgs = store.inventory().list(37, 1);
        check(pgs.size() == 1 && pgs[0].id == 37 && pgs[0].count == 1 && pgs[0].open_count == 0, "PG list");
        check(store.read_chunk(a, 0, 4) == "data" && store.read_chunk(c, 0, 4) == "copy", "scan preserved contents");
    }
}
void fences() {
    Directory root;
    const auto a = id(1), b = id(2);
    {
        ChunkStore store(root.path); store.create_chunk(a, 100); store.append(a, "a");
        store.inventory().fence(37, 10, 0, PGMode::Source);
        unit::expect<WriteUnavailable>([&] { store.create_chunk(b, 100); });
        unit::expect<WriteUnavailable>([&] { store.delete_chunk(a); });
        ReplicaInfo info;
        unit::expect<WriteUnavailable>([&] { store.open_snapshot(a, info); });
        unit::expect<WriteUnavailable>([&] { store.append(a, "b"); });
        store.seal_chunk(a, 10, 1);
        const auto snapshot = store.open_snapshot(a, info, 0, 10);
        check(info.length == 1, "fenced source snapshot"); ::close(snapshot);
        unit::expect<WriteUnavailable>([&] { store.inventory().fence(37, 9, 0, PGMode::Source); });
    }
    {
        ChunkStore store(root.path);
        check(store.inventory().page(37, "", 1).mode == PGMode::Source, "source fence survived restart");
        unit::expect<WriteUnavailable>([&] { store.create_chunk(b, 100); });
        store.inventory().fence(37, 10, 0, PGMode::Source); // Idempotent replay.
        store.inventory().fence(37, 10, 0, PGMode::Active);
        unit::expect<WriteUnavailable>([&] { store.read_chunk(a, 0, 1); });
        check(store.read_chunk(a, 0, 1, 10) == "a", "new route epoch read");
        store.create_chunk(b, 100, 10); store.append(b, "b", 10);
        store.inventory().fence(37, 20, 10, PGMode::Source);
        store.inventory().fence(37, 10, 0, PGMode::Retired); // Delayed old heartbeat cannot fence the newer job.
        check(store.inventory().page(37, "", 1).mode == PGMode::Source, "late retirement changed newer fence");
        store.inventory().fence(37, 20, 0, PGMode::Retired);
        unit::expect<WriteUnavailable>([&] { store.append(b, "c", 20); });
        store.inventory().fence(38, 21, 0, PGMode::Target);
        unit::expect<WriteUnavailable>([&] { store.create_chunk(id(3, 38), 100); });
        unit::expect<WriteUnavailable>([&] { store.open_repair_file(id(3, 38), 0, 20); });
        const auto fd = store.open_repair_file(id(3, 38), 0, 21); ::close(fd);
    }
    ChunkStore restarted(root.path);
    check(restarted.inventory().page(37, "", 1).mode == PGMode::Retired, "retirement survived restart");
    check(restarted.inventory().page(38, "", 1).mode == PGMode::Target, "target fence survived restart");
}
void inflight_barrier() {
    Directory root; PGInventory inventory(root.path);
    std::optional<PGInventory::Guard> guard(inventory.guard(id(1), 0, PGAccess::Namespace));
    std::promise<void> entered;
    auto fence = std::async(std::launch::async, [&] { entered.set_value(); inventory.fence(37, 1, 0, PGMode::Source); });
    entered.get_future().wait();
    check(fence.wait_for(std::chrono::milliseconds(30)) == std::future_status::timeout, "fence bypassed in-flight operation");
    guard.reset(); fence.get();
    unit::expect<WriteUnavailable>([&] { inventory.guard(id(1), 0, PGAccess::Namespace); });
}
void malformed_scan() {
    Directory root;
    { ChunkStore store(root.path); store.create_chunk(id(1), 100); store.inventory().fence(37, 1, 0, PGMode::Source); }
    auto control = root.path / ".pg_control" / "37";
    std::ofstream(control, std::ios::app) << "corrupt";
    unit::expect<std::runtime_error>([&] { ChunkStore store(root.path); });
    fs::remove(control);
    const auto shard = root.path / id(1).substr(0, 2);
    fs::copy_file(shard / (id(1) + ".open"), shard / (id(1) + ".frozen"));
    unit::expect<std::runtime_error>([&] { ChunkStore store(root.path); });
}
}
int main() { return unit::run({{"inventory lifecycle and restart", lifecycle}, {"durable PG fences", fences},
                              {"in-flight namespace barrier", inflight_barrier}, {"reject corrupt scan", malformed_scan}}); }
