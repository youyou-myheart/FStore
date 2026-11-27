#pragma once
#include "chunk_state.hpp"
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

namespace chunkstore {
enum class PGMode : unsigned { Active, Source, Target, Retired };
enum class PGAccess { Read, Append, Freeze, Seal, Namespace, Snapshot, Repair };
struct PGChunk { std::string id; State state; };
struct PGPage {
    std::vector<PGChunk> chunks;
    std::optional<std::string> next;
    std::uint64_t count = 0, open_count = 0, generation = 0, token = 0, epoch = 0;
    PGMode mode = PGMode::Active;
};
struct PGSummary { std::uint32_t id; std::uint64_t count, open_count; };
class PGInventory {
    struct Group;
public:
    class Guard {
        friend class PGInventory;
        std::shared_ptr<Group> group_;
        std::shared_lock<std::shared_mutex> lock_;
    };
    explicit PGInventory(const std::filesystem::path& root);
    Guard guard(std::string_view id, std::uint64_t epoch, PGAccess access, std::uint64_t token = 0);
    void update(std::string_view id, State state);
    void remove(std::string_view id);
    PGPage page(std::uint32_t pg, std::string_view after, std::uint32_t limit) const;
    std::vector<PGSummary> list(std::uint32_t start, std::uint32_t limit) const;
    void fence(std::uint32_t pg, std::uint64_t token, std::uint64_t expected_epoch, PGMode mode);
    std::uint64_t chunk_count() const;
    std::uint64_t legacy_count() const { return legacy_; }
private:
    std::shared_ptr<Group> group(std::uint32_t pg) const;
    void persist(std::uint32_t pg, std::uint64_t token, std::uint64_t epoch, PGMode mode);
    struct Impl;
    std::shared_ptr<Impl> impl_;
    std::filesystem::path root_;
    std::uint64_t legacy_ = 0;
};
}
