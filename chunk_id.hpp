#pragma once

#include <charconv>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

namespace chunkstore {
inline constexpr std::string_view chunk_id_hex = "0123456789abcdef";
inline constexpr std::uint32_t global_pg_count = 10000000;

// Stable version-1 permutation of the entire 64-bit index. It is independent
// of the Blob sequence and Space ID. These constants are part of the format.
inline std::uint64_t hash_chunk_index(std::uint64_t index) {
    index = (index ^ (index >> 30)) * 0xbf58476d1ce4e5b9ULL;
    index = (index ^ (index >> 27)) * 0x94d049bb133111ebULL;
    return index ^ (index >> 31);
}
inline std::uint64_t unhash_chunk_index(std::uint64_t hash) {
    hash ^= hash >> 31; hash ^= hash >> 62;
    hash *= 0x319642b2d24d8ec3ULL;
    hash ^= hash >> 27; hash ^= hash >> 54;
    hash *= 0x96de1b173f119089ULL;
    hash ^= hash >> 30; hash ^= hash >> 60;
    return hash;
}

// Logical 120-bit ID. Explicit text serialization excludes native struct padding.
// High to low: seq_id (48 bits), space_id (8), index (64).
struct ChunkId {
    static constexpr std::uint64_t max_sequence = (1ULL << 48) - 1;
    static constexpr std::uint64_t max_index = UINT64_MAX;
    std::uint64_t seq_id;
    std::uint8_t space_id;
    std::uint64_t index;

    std::string str() const {
        if (seq_id > max_sequence) throw std::invalid_argument("Blob sequence exceeds 48 bits");
        std::string result(30, '0');
        auto sequence = seq_id;
        for (int i = 11; i >= 0; --i) { result[i] = chunk_id_hex[sequence & 15]; sequence >>= 4; }
        result[12] = chunk_id_hex[space_id >> 4];
        result[13] = chunk_id_hex[space_id & 15];
        auto position = index;
        for (int i = 29; i >= 14; --i) { result[i] = chunk_id_hex[position & 15]; position >>= 4; }
        return result;
    }

    static ChunkId parse(std::string_view text) {
        if (text.size() != 30 || text.find_first_not_of(chunk_id_hex) != text.npos) {
            throw std::invalid_argument("chunk ID must be 30 lowercase hex digits (120 bits)");
        }
        std::uint64_t sequence = 0, position = 0;
        unsigned space = 0;
        std::from_chars(text.data(), text.data() + 12, sequence, 16);
        std::from_chars(text.data() + 12, text.data() + 14, space, 16);
        std::from_chars(text.data() + 14, text.data() + 30, position, 16);
        return {sequence, static_cast<std::uint8_t>(space), position};
    }

    std::uint32_t pg() const {
        return static_cast<std::uint32_t>(hash_chunk_index(index) % global_pg_count);
    }
};

inline bool valid_storage_chunk_id(std::string_view id) {
    return (id.size() == 30 || id.size() == 24 || id.size() == 32) &&
           id.find_first_not_of(chunk_id_hex) == id.npos;
}
} // namespace chunkstore
