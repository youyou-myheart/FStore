#include "pg_inventory.hpp"
#include "chunk_id.hpp"
#include "chunk_store.hpp"

#include <atomic>
#include <array>
#include <charconv>
#include <fcntl.h>
#include <fstream>
#include <map>
#include <mutex>
#include <sstream>
#include <system_error>
#include <unistd.h>

namespace chunkstore {
namespace fs = std::filesystem;
namespace {
std::atomic<std::uint64_t> inventory_instances{0};
void sync_dir(const fs::path& path) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) throw std::system_error(errno, std::generic_category(), "open PG directory");
    const int result = ::fsync(fd), error = errno; ::close(fd);
    if (result < 0) throw std::system_error(error, std::generic_category(), "sync PG directory");
}
std::uint64_t checksum(std::string_view text) {
    std::uint64_t h = 14695981039346656037ULL;
    for (const unsigned char c : text) h = (h ^ c) * 1099511628211ULL;
    return h;
}
void valid_pg(std::uint32_t pg) {
    if (pg >= global_pg_count) throw std::invalid_argument("PG ID exceeds global pool");
}
}
struct PGInventory::Group {
    mutable std::shared_mutex gate;
    mutable std::mutex mutex;
    std::map<std::string, State, std::less<>> chunks;
    std::uint64_t open = 0, generation = 0, token = 0, epoch = 0;
    PGMode mode = PGMode::Active;
};
struct PGInventory::Impl {
    const std::uint64_t instance = ++inventory_instances;
    mutable std::shared_mutex mutex;
    std::map<std::uint32_t, std::shared_ptr<Group>> groups;
    std::atomic<bool> failed{false};
    std::atomic<std::uint64_t> chunks{0};
};
std::shared_ptr<PGInventory::Group> PGInventory::group(std::uint32_t pg) const {
    valid_pg(pg);
    if (impl_->failed.load()) throw WriteUnavailable("PG inventory failed; restart required");
    // Hot chunk I/O avoids the directory lock; groups live until store shutdown.
    struct Cached { std::uint64_t owner = 0; std::uint32_t pg = 0; std::weak_ptr<Group> group; };
    thread_local std::array<Cached, 64> cache;
    auto& cached = cache[pg % cache.size()];
    if (cached.owner == impl_->instance && cached.pg == pg) if (auto value = cached.group.lock()) return value;
    auto remember = [&](const std::shared_ptr<Group>& value) {
        cached = {impl_->instance, pg, value}; return value;
    };
    { std::shared_lock lock(impl_->mutex); const auto it = impl_->groups.find(pg); if (it != impl_->groups.end()) return remember(it->second); }
    std::unique_lock lock(impl_->mutex);
    auto [it, inserted] = impl_->groups.try_emplace(pg);
    if (inserted) {
        try { it->second = std::make_shared<Group>(); }
        catch (...) { impl_->groups.erase(it); throw; }
    }
    return remember(it->second);
}
PGInventory::PGInventory(const fs::path& root) : impl_(std::make_shared<Impl>()), root_(root) {
    const auto control = root_ / ".pg_control";
    fs::create_directory(control);
    if (!fs::is_directory(fs::symlink_status(control))) throw std::runtime_error("invalid PG control directory");
    sync_dir(root_);
    for (const auto& entry : fs::directory_iterator(control)) {
        const auto name = entry.path().filename().string();
        if (name.ends_with(".tmp")) continue;
        std::uint32_t pg = 0;
        const auto parsed = std::from_chars(name.data(), name.data() + name.size(), pg);
        if (parsed.ec != std::errc{} || parsed.ptr != name.data() + name.size() || name != std::to_string(pg) || !fs::is_regular_file(entry.symlink_status())) {
            throw std::runtime_error("invalid PG fence file");
        }
        std::ifstream input(entry.path());
        std::string line, hash, extra;
        std::getline(input, line); std::getline(input, hash);
        if (!input || std::getline(input, extra) || hash != std::to_string(checksum(line))) throw std::runtime_error("PG fence checksum mismatch");
        std::istringstream fields(line); std::string magic; unsigned mode; std::uint64_t token, epoch;
        if (!(fields >> magic >> token >> epoch >> mode) || magic != "FPG1" || !token || mode > 3 || (fields >> extra)) {
            throw std::runtime_error("invalid PG fence metadata");
        }
        auto g = group(pg); g->token = token; g->epoch = epoch; g->mode = static_cast<PGMode>(mode);
        if ((g->mode == PGMode::Active || g->mode == PGMode::Retired) ? epoch != token : epoch >= token) {
            throw std::runtime_error("invalid PG fence epoch");
        }
    }
    for (const auto& shard : fs::directory_iterator(root_)) {
        const auto name = shard.path().filename().string();
        if (name.size() != 2 || name.find_first_not_of(chunk_id_hex) != name.npos) continue;
        if (!fs::is_directory(shard.symlink_status())) throw std::runtime_error("invalid Chunk shard");
        for (const auto& entry : fs::directory_iterator(shard)) {
            const auto ext = entry.path().extension().string();
            if (ext != ".open" && ext != ".frozen") continue;
            const auto id = entry.path().stem().string();
            if (!valid_storage_chunk_id(id) || !id.starts_with(name) || !fs::is_regular_file(entry.symlink_status())) {
                throw std::runtime_error("invalid active Chunk path during PG scan");
            }
            if (id.size() != 30) { ++legacy_; continue; }
            auto g = group(ChunkId::parse(id).pg());
            if (g->chunks.contains(id)) throw std::runtime_error("conflicting active Chunk files during PG scan");
            update(id, ext == ".frozen" ? load_closed_state(entry.path()) : State::Open);
        }
    }
    sync_dir(control);
}
PGInventory::Guard PGInventory::guard(std::string_view id, std::uint64_t epoch, PGAccess access, std::uint64_t token) {
    Guard result;
    if (access == PGAccess::Seal && !token) throw WriteUnavailable("SealChunk requires a PG recovery token");
    if (!valid_storage_chunk_id(id)) throw std::invalid_argument("invalid Chunk ID");
    if (id.size() != 30) {
        if (epoch || token) throw WriteUnavailable("legacy Chunk has no PG epoch");
        return result;
    }
    result.group_ = group(ChunkId::parse(id).pg());
    auto& g = *result.group_;
    result.lock_ = std::shared_lock(g.gate);
    if (token) {
        const auto expected = access == PGAccess::Repair ? PGMode::Target : PGMode::Source;
        if ((access != PGAccess::Repair && access != PGAccess::Snapshot && access != PGAccess::Seal) || g.token != token || g.mode != expected) {
            throw WriteUnavailable("recovery token or PG role mismatch");
        }
    } else if (g.epoch != epoch || g.mode == PGMode::Target || g.mode == PGMode::Retired ||
               (g.mode == PGMode::Source && (access == PGAccess::Append || access == PGAccess::Namespace || access == PGAccess::Repair || access == PGAccess::Snapshot))) {
        throw WriteUnavailable("stale PG epoch or PG recovery fence; refresh the route");
    }
    return result;
}
void PGInventory::update(std::string_view id, State state) {
    if (id.size() != 30) return;
    try {
        auto g = group(ChunkId::parse(id).pg()); std::lock_guard lock(g->mutex);
        auto [it, added] = g->chunks.try_emplace(std::string(id), state);
        if (added) { if (!is_closed(state)) ++g->open; ++g->generation; impl_->chunks.fetch_add(1, std::memory_order_relaxed); }
        else if (it->second != state) {
            if (!is_closed(it->second)) --g->open;
            if (!is_closed(state)) ++g->open;
            it->second = state; ++g->generation;
        }
    } catch (...) { impl_->failed.store(true); throw; }
}
void PGInventory::remove(std::string_view id) {
    if (id.size() != 30) return;
    auto g = group(ChunkId::parse(id).pg()); std::lock_guard lock(g->mutex);
    const auto it = g->chunks.find(id);
    if (it != g->chunks.end()) { if (!is_closed(it->second)) --g->open; g->chunks.erase(it); ++g->generation; impl_->chunks.fetch_sub(1, std::memory_order_relaxed); }
}
PGPage PGInventory::page(std::uint32_t pg, std::string_view after, std::uint32_t limit) const {
    if (!limit || limit > 1024) throw std::invalid_argument("PG inventory page must have 1..1024 entries");
    auto g = group(pg); std::shared_lock gate(g->gate); std::lock_guard lock(g->mutex);
    PGPage page; page.count = g->chunks.size(); page.open_count = g->open; page.generation = g->generation;
    page.token = g->token; page.epoch = g->epoch; page.mode = g->mode;
    auto it = g->chunks.upper_bound(after);
    while (it != g->chunks.end() && page.chunks.size() < limit) { page.chunks.push_back({it->first, it->second}); ++it; }
    if (it != g->chunks.end()) page.next = page.chunks.back().id;
    return page;
}
std::vector<PGSummary> PGInventory::list(std::uint32_t start, std::uint32_t limit) const {
    if (start > global_pg_count || !limit || limit > 1024) throw std::invalid_argument("invalid inventory page");
    if (impl_->failed.load()) throw WriteUnavailable("PG inventory failed");
    std::shared_lock lock(impl_->mutex); std::vector<PGSummary> result;
    for (auto it = impl_->groups.lower_bound(start); it != impl_->groups.end() && result.size() < limit; ++it) {
        std::lock_guard members(it->second->mutex);
        if (!it->second->chunks.empty()) result.push_back({it->first, it->second->chunks.size(), it->second->open});
    }
    return result;
}
std::uint64_t PGInventory::chunk_count() const {
    if (impl_->failed.load()) throw WriteUnavailable("PG inventory failed");
    return impl_->chunks.load(std::memory_order_relaxed);
}
void PGInventory::persist(std::uint32_t pg, std::uint64_t token, std::uint64_t epoch, PGMode mode) {
    const auto directory = root_ / ".pg_control";
    const auto target = directory / std::to_string(pg), temporary = directory / (std::to_string(pg) + ".tmp");
    const auto payload = "FPG1 " + std::to_string(token) + " " + std::to_string(epoch) + " " + std::to_string(static_cast<unsigned>(mode));
    const auto contents = payload + "\n" + std::to_string(checksum(payload)) + "\n";
    const int fd = ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (fd < 0) throw std::system_error(errno, std::generic_category(), "open PG fence");
    try {
        std::size_t done = 0;
        while (done < contents.size()) {
            const auto n = ::write(fd, contents.data() + done, contents.size() - done);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) throw std::system_error(n < 0 ? errno : EIO, std::generic_category(), "write PG fence");
            done += n;
        }
        if (::fsync(fd) < 0) throw std::system_error(errno, std::generic_category(), "sync PG fence");
        if (::rename(temporary.c_str(), target.c_str()) < 0) throw std::system_error(errno, std::generic_category(), "publish PG fence");
        sync_dir(directory);
    } catch (...) { ::close(fd); impl_->failed.store(true); throw; }
    ::close(fd);
}
void PGInventory::fence(std::uint32_t pg, std::uint64_t token, std::uint64_t expected_epoch, PGMode mode) {
    if (!token || static_cast<unsigned>(mode) > 3) throw std::invalid_argument("invalid PG recovery fence");
    auto g = group(pg); std::unique_lock gate(g->gate);
    if (token < g->token) {
        if (mode == PGMode::Retired) return;
        throw WriteUnavailable("stale recovery token");
    }
    if (token == g->token && mode == g->mode) return;
    if (mode == PGMode::Active) {
        if (token != g->token || (g->mode != PGMode::Source && g->mode != PGMode::Target)) throw WriteUnavailable("PG not prepared for activation");
        expected_epoch = token;
    } else if (mode == PGMode::Retired) {
        expected_epoch = token;
    } else if (token <= g->token || expected_epoch != g->epoch || token <= expected_epoch) {
        throw WriteUnavailable("PG epoch changed or recovery already completed");
    }
    persist(pg, token, expected_epoch, mode);
    g->token = token; g->epoch = expected_epoch; g->mode = mode;
}
}
