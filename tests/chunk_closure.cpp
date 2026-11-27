#include "chunk_store.hpp"
#include "chunk_id.hpp"
#include "unit.hpp"
#include <barrier>
#include <future>
#include <sys/xattr.h>
#include <unistd.h>

using namespace chunkstore;
using unit::check;
namespace fs = std::filesystem;
namespace {
struct Directory {
    fs::path path;
    Directory() { char name[] = "/tmp/fstore-closure-XXXXXX"; if (!::mkdtemp(name)) throw std::runtime_error("mkdtemp"); path = name; }
    ~Directory() { fs::remove_all(path); }
};
std::string id(unsigned sequence) { return ChunkId{sequence, 1, unhash_chunk_index(37)}.str(); }
State state(ChunkStore& store, const std::string& key, std::uint64_t epoch = 0) {
    return store.read_chunk(key, 0, std::span<char>{}, epoch);
}
void close_causes_and_restart() {
    Directory root;
    {
        ChunkStore store(root.path);
        for (unsigned i = 1; i <= 2; ++i) { store.create_chunk(id(i), 4096); store.append(id(i), "data"); }
        store.freeze_chunk(id(1));
        unit::expect<WriteUnavailable>([&] { store.seal_chunk(id(2), 0); });
        unit::expect<WriteUnavailable>([&] { store.seal_chunk(id(2), 7); });
        store.inventory().fence(37, 7, 0, PGMode::Source);
        unit::expect<LengthMismatch>([&] { store.seal_chunk(id(2), 7, 3); });
        check(state(store, id(2)) == State::Open, "length mismatch closed a chunk");
        check(store.seal_chunk(id(2), 7, 4) == 4 && store.seal_chunk(id(2), 7) == 4, "seal not idempotent");
        store.seal_chunk(id(1), 7, 4); store.freeze_chunk(id(2));
        check(state(store, id(1)) == State::Frozen && state(store, id(2)) == State::Sealed, "cross-close overwrote first reason");
        auto page = store.inventory().page(37, "", 10);
        check(page.open_count == 0 && page.chunks[0].state == State::Frozen && page.chunks[1].state == State::Sealed, "inventory lost close causes");
        store.inventory().fence(37, 7, 0, PGMode::Active);
        unit::expect<ChunkFrozen>([&] { store.append(id(1), "x", 7); });
        unit::expect<ChunkSealed>([&] { store.append(id(2), "x", 7); });
    }
    {
        ChunkStore store(root.path);
        check(state(store, id(1), 7) == State::Frozen && state(store, id(2), 7) == State::Sealed, "restart lost cause");
        check(store.inventory().page(37, "", 10).chunks[1].state == State::Sealed, "startup scan lost cause");
        const auto token = store.delete_chunk(id(2), 7);
        check(store.restore_chunk(id(2), token, 7) == State::Sealed, "trash restore lost seal");
        check(store.create_chunk(id(2), 4096, 7) == State::Sealed, "create retry lost seal");
        ReplicaInfo info; const int fd = store.open_snapshot(id(2), info, 7); ::close(fd);
        check(info.state == State::Sealed && info.length == 4, "sealed chunk cannot be repaired");
        check(store.read_chunk(id(2), 0, 4, 7) == "data", "closure modified data");
    }
}
void repair_replaces_reason_atomically() {
    Directory root;
    {
        ChunkStore store(root.path); store.create_chunk(id(1), 4096); store.append(id(1), "old"); store.freeze_chunk(id(1));
        check(state(store, id(1)) == State::Frozen, "prime cached read");
        for (const auto desired : {State::Sealed, State::Frozen, State::Sealed}) {
            const int fd = store.open_repair_file(id(1));
            try {
                check(::write(fd, "new", 3) == 3, "write staged data");
                store.install_replica(id(1), fd, {3, 4096, desired});
            } catch (...) { ::close(fd); throw; }
            ::close(fd);
            check(state(store, id(1)) == desired && store.inventory().page(37, "", 1).chunks[0].state == desired,
                  "repair retained old cached close cause");
        }
    }
    ChunkStore restarted(root.path);
    check(state(restarted, id(1)) == State::Sealed && restarted.read_chunk(id(1), 0, 3) == "new", "installed reason not durable");
}
void legacy_and_pre_rename_metadata() {
    Directory root;
    {
        ChunkStore store(root.path); store.create_chunk(id(1), 100); store.freeze_chunk(id(1)); store.create_chunk(id(2), 100);
    }
    const auto closed = root.path / id(1).substr(0, 2) / (id(1) + ".frozen");
    const auto open = root.path / id(2).substr(0, 2) / (id(2) + ".open");
    check(::removexattr(closed.c_str(), close_reason_attribute) == 0, "remove legacy reason");
    check(::setxattr(open.c_str(), close_reason_attribute, "PG_REPAIR", 9, 0) == 0, "stage pre-rename reason");
    {
        ChunkStore store(root.path);
        check(state(store, id(1)) == State::Frozen && state(store, id(2)) == State::Open, "legacy or pre-rename state incorrect");
        store.freeze_chunk(id(2));
        check(state(store, id(2)) == State::Frozen, "stale pre-rename reason reused");
    }
    check(::setxattr(closed.c_str(), close_reason_attribute, "INVALID", 7, 0) == 0, "corrupt reason");
    unit::expect<std::system_error>([&] { ChunkStore store(root.path); });
}
void concurrent_first_close() {
    Directory root; ChunkStore store(root.path);
    for (unsigned n = 1; n <= 20; ++n) store.create_chunk(id(n), 100);
    store.inventory().fence(37, 1, 0, PGMode::Source);
    for (unsigned n = 1; n <= 20; ++n) {
        std::barrier start(2);
        auto user = std::async(std::launch::async, [&] { start.arrive_and_wait(); store.freeze_chunk(id(n)); });
        start.arrive_and_wait(); store.seal_chunk(id(n), 1); user.get();
        const auto observed = state(store, id(n));
        check(is_closed(observed), "concurrent close stayed open");
        store.seal_chunk(id(n), 1); store.freeze_chunk(id(n));
        check(state(store, id(n)) == observed && load_closed_state(root.path / id(n).substr(0, 2) / (id(n) + ".frozen")) == observed,
              "first reason differs in memory and on disk");
    }
    check(store.inventory().page(37, "", 100).open_count == 0, "concurrent close corrupted open count");
}
}
int main() { return unit::run({{"close causes survive restart and restore", close_causes_and_restart},
    {"repair replaces data and cause together", repair_replaces_reason_atomically},
    {"legacy and pre-rename attributes", legacy_and_pre_rename_metadata}, {"concurrent first close wins", concurrent_first_close}}); }
