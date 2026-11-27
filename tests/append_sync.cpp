#include "chunk_store.hpp"
#include "chunk_id.hpp"
#include "unit.hpp"

#include <array>
#include <cerrno>
#include <future>
#include <semaphore>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

using namespace chunkstore;
using unit::check;
namespace fs = std::filesystem;
namespace {
struct Calls {
    unsigned data = 0, file = 0, directory = 0, writes = 0;
    bool fail_data = false, fail_file = false, fail_write = false;
    std::binary_semaphore* entered = nullptr;
    std::binary_semaphore* resume = nullptr;
};
thread_local Calls* observed = nullptr;
struct Observe {
    Calls calls;
    Observe() { observed = &calls; }
    ~Observe() { observed = nullptr; }
};
struct Directory {
    fs::path path;
    Directory() { char name[] = "/tmp/fstore-sync-XXXXXX"; if (!::mkdtemp(name)) throw std::runtime_error("mkdtemp"); path = name; }
    ~Directory() { fs::remove_all(path); }
};
std::string id(unsigned n) { return ChunkId{n, 1, unhash_chunk_index(37)}.str(); }
void default_and_group_commit() {
    Directory root; ChunkStore store(root.path);
    store.create_chunk(id(1), 4);
    const std::array<std::string_view, 3> buffers{"ab", "cd", "e"};
    std::array<AppendGroupItem, 3> requests;
    for (unsigned i = 0; i < requests.size(); ++i) requests[i].buffers = std::span(buffers).subspan(i, 1);
    Observe sample;
    store.append_group(id(1), requests);
    check(sample.calls.data == 1 && sample.calls.writes == 1, "default must sync once per accepted batch");
    check(requests[0].result && requests[1].result && !requests[2].result, "capacity decisions changed");
    check(store.read_chunk(id(1), 0, 4) == "abcd", "batch bytes not readable");
    unit::expect<ChunkFull>([&] { store.append(id(1), "e"); });
    check(sample.calls.data == 1 && sample.calls.writes == 1, "rejected append performed IO");
}
void none_preserves_close_and_metadata_sync() {
    Directory root;
    {
        ChunkStore store(root.path, TrafficLimiter::default_rate, AppendSync::None);
        for (unsigned i = 1; i <= 2; ++i) store.create_chunk(id(i), 8);
        Observe sample;
        sample.calls.fail_data = true; // None must not attempt fdatasync even if it would fail.
        store.append(id(1), "ab");
        const std::array<std::string_view, 2> buffers{"c", "d"};
        store.append_batch(id(1), buffers);
        store.append(id(2), "seal");
        check(sample.calls.data == 0 && sample.calls.file == 0 && sample.calls.writes == 3, "none synced appends");
        check(store.read_chunk(id(1), 0, 4) == "abcd", "none must publish completed writes");
        unit::expect<ChunkFull>([&] { store.append(id(1), "12345"); });
        const auto before = sample.calls.file;
        store.freeze_chunk(id(1), 4);
        check(sample.calls.file > before && sample.calls.directory > 0, "freeze skipped data or directory sync");
        unit::expect<ChunkFrozen>([&] { store.append(id(1), "x"); });
        store.inventory().fence(37, 7, 0, PGMode::Source);
        const auto sealed_before = sample.calls.file;
        store.seal_chunk(id(2), 7, 4);
        check(sample.calls.file > sealed_before, "seal skipped data sync");
        store.inventory().fence(37, 7, 0, PGMode::Active);
        const auto metadata_before = sample.calls.file;
        store.create_chunk(id(3), 8, 7);
        check(sample.calls.file > metadata_before, "none weakened create durability");
        const auto directory_before = sample.calls.directory;
        const auto token = store.delete_chunk(id(1), 7);
        check(store.restore_chunk(id(1), token, 7) == State::Frozen, "restore lost closed state");
        check(sample.calls.directory > directory_before, "none skipped namespace sync");
    }
    ChunkStore restarted(root.path, TrafficLimiter::default_rate, AppendSync::None);
    check(restarted.read_chunk(id(1), 0, 4, 7) == "abcd" && restarted.read_chunk(id(2), 0, 4, 7) == "seal", "closed data lost on reopen");
}
void sync_and_write_errors_are_not_acknowledged() {
    for (const auto policy : {AppendSync::Always, AppendSync::None}) {
        Directory root; ChunkStore store(root.path, TrafficLimiter::default_rate, policy);
        store.create_chunk(id(1), 8);
        Observe sample;
        sample.calls.fail_data = policy == AppendSync::Always;
        sample.calls.fail_write = policy == AppendSync::None;
        unit::expect<std::system_error>([&] { store.append(id(1), "data"); });
        unit::expect<WriteUnavailable>([&] { store.append(id(1), "next"); });
        unit::expect<WriteUnavailable>([&] { store.freeze_chunk(id(1)); });
        unit::expect<std::out_of_range>([&] { store.read_chunk(id(1), 0, 1); });
    }
}
void none_does_not_ignore_close_or_repair_errors() {
    Directory root; ChunkStore store(root.path, TrafficLimiter::default_rate, AppendSync::None);
    store.create_chunk(id(1), 8); store.append(id(1), "data");
    Observe sample; sample.calls.fail_file = true;
    unit::expect<std::system_error>([&] { store.freeze_chunk(id(1)); });
    check(store.read_chunk(id(1), 0, std::span<char>{}) == State::Open, "failed freeze published closed state");
    sample.calls.fail_file = false;
    int fd = store.open_repair_file(id(2));
    try {
        check(::write(fd, "copy", 4) == 4, "stage repair");
        sample.calls.fail_file = true;
        unit::expect<std::system_error>([&] { store.install_replica(id(2), fd, {4, 8, State::Sealed}); });
        check(!fs::exists(root.path / id(2).substr(0, 2) / (id(2) + ".frozen")), "failed repair published its file");
        sample.calls.fail_file = false;
        // Failed transfers are discarded; retries start with a fresh staging inode.
        ::close(fd); fd = -1;
        fd = store.open_repair_file(id(2));
        check(::write(fd, "copy", 4) == 4, "restage repair");
        const auto before = sample.calls.file;
        store.install_replica(id(2), fd, {4, 8, State::Sealed});
        check(sample.calls.file > before && store.read_chunk(id(2), 0, 4) == "copy", "repair did not sync and publish");
    } catch (...) { ::close(fd); throw; }
    ::close(fd);
}
void explicit_sync_and_state() {
    for (const auto policy : {AppendSync::Always, AppendSync::None}) {
        Directory root;
        {
            ChunkStore store(root.path, TrafficLimiter::default_rate, policy);
            for (unsigned i = 1; i <= 2; ++i) store.create_chunk(id(i), 16);
            check(store.sync_chunk(id(1), 0) == 0, "empty sync failed");
            store.append(id(1), "abc");
            Observe sample;
            unit::expect<LengthMismatch>([&] { store.sync_chunk(id(1), 0); });
            check(sample.calls.data == 0, "length mismatch must not sync");
            check(store.sync_chunk(id(1), 3) == 3 && sample.calls.data == 1, "explicit sync skipped fdatasync");
            check(store.sync_chunk(id(1)) == 3 && sample.calls.data == 2, "repeat sync failed");
            check(store.read_chunk(id(1), 0, std::span<char>{}) == State::Open, "sync closed chunk");
            check(store.append(id(1), "def").offset == 3, "sync changed append offset");
            check(store.sync_chunk(id(1), 6) == 6, "sync did not cover later writes");
            store.freeze_chunk(id(1));
            check(store.sync_chunk(id(1), 6) == 6, "frozen sync failed");
            store.append(id(2), "seal");
            store.inventory().fence(37, 7, 0, PGMode::Source);
            check(store.sync_chunk(id(2), 4) == 4, "source fence must permit sync");
            store.seal_chunk(id(2), 7);
            check(store.sync_chunk(id(2), 4) == 4 && store.read_chunk(id(2), 0, std::span<char>{}) == State::Sealed,
                  "sync lost seal reason");
            store.inventory().fence(37, 7, 0, PGMode::Active);
            unit::expect<WriteUnavailable>([&] { store.sync_chunk(id(1)); });
            check(store.sync_chunk(id(1), 6, 7) == 6, "current epoch sync failed");
            unit::expect<std::system_error>([&] { store.sync_chunk(id(3), std::nullopt, 7); });
        }
        ChunkStore reopened(root.path, TrafficLimiter::default_rate, policy);
        check(reopened.sync_chunk(id(1), 6, 7) == 6, "sync reopened read-only descriptor failed");
        reopened.inventory().fence(37, 8, 7, PGMode::Target);
        unit::expect<WriteUnavailable>([&] { reopened.sync_chunk(id(1), 6, 7); });
        reopened.inventory().fence(37, 8, 7, PGMode::Retired);
        unit::expect<WriteUnavailable>([&] { reopened.sync_chunk(id(1), 6, 8); });
    }
}
void explicit_sync_failure_blocks_writes() {
    Directory root; ChunkStore store(root.path, TrafficLimiter::default_rate, AppendSync::None);
    store.create_chunk(id(1), 16); store.append(id(1), "abc");
    Observe sample; sample.calls.fail_data = true;
    unit::expect<std::system_error>([&] { store.sync_chunk(id(1)); });
    sample.calls.fail_data = false;
    unit::expect<WriteUnavailable>([&] { store.sync_chunk(id(1)); });
    unit::expect<WriteUnavailable>([&] { store.append(id(1), "x"); });
    unit::expect<WriteUnavailable>([&] { store.freeze_chunk(id(1)); });
    check(store.read_chunk(id(1), 0, 3) == "abc", "sync failure changed existing data");
}
void explicit_sync_serializes_append() {
    Directory root; ChunkStore store(root.path, TrafficLimiter::default_rate, AppendSync::None);
    store.create_chunk(id(1), 16); store.append(id(1), "abc");
    std::binary_semaphore entered(0), resume(0), appending(0);
    auto synced = std::async(std::launch::async, [&] {
        Observe sample; sample.calls.entered = &entered; sample.calls.resume = &resume;
        return store.sync_chunk(id(1), 3);
    });
    entered.acquire();
    auto appended = std::async(std::launch::async, [&] { appending.release(); return store.append(id(1), "def"); });
    appending.acquire();
    const auto blocked = appended.wait_for(std::chrono::milliseconds(20)) == std::future_status::timeout;
    resume.release();
    check(synced.get() == 3 && appended.get().offset == 3 && blocked, "append crossed sync barrier");
    check(store.sync_chunk(id(1), 6) == 6, "post-barrier sync failed");
}
}

extern "C" int __real_fdatasync(int);
extern "C" int __real_fsync(int);
extern "C" ssize_t __real_pwritev(int, const iovec*, int, off_t);
extern "C" int __wrap_fdatasync(int fd) {
    if (observed) {
        ++observed->data;
        if (observed->entered) { observed->entered->release(); observed->resume->acquire(); }
        if (observed->fail_data) { errno = EIO; return -1; }
    }
    return __real_fdatasync(fd);
}
extern "C" int __wrap_fsync(int fd) {
    if (observed) {
        struct stat info{};
        if (::fstat(fd, &info) == 0 && S_ISREG(info.st_mode)) {
            ++observed->file;
            if (observed->fail_file) { errno = EIO; return -1; }
        } else ++observed->directory;
    }
    return __real_fsync(fd);
}
extern "C" ssize_t __wrap_pwritev(int fd, const iovec* vectors, int count, off_t offset) {
    if (observed) { ++observed->writes; if (observed->fail_write) { errno = EIO; return -1; } }
    return __real_pwritev(fd, vectors, count, offset);
}
int main() { return unit::run({{"default batch sync", default_and_group_commit},
    {"none preserves close and metadata sync", none_preserves_close_and_metadata_sync},
    {"IO errors do not acknowledge writes", sync_and_write_errors_are_not_acknowledged},
    {"none preserves close failures and repair sync", none_does_not_ignore_close_or_repair_errors},
    {"explicit sync preserves state and enforces epochs", explicit_sync_and_state},
    {"explicit sync failures do not reopen writes", explicit_sync_failure_blocks_writes},
    {"explicit sync serializes appends", explicit_sync_serializes_append}}); }
