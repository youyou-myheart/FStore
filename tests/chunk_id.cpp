#include "chunk_id.hpp"

#include <array>
#include <iostream>

using chunkstore::ChunkId;
void check(bool condition) { if (!condition) throw std::runtime_error("120-bit ID check failed"); }

int main() {
    try {
        check(ChunkId{0x010203040506ULL, 0xab, 0x0708090a0b0c0d0eULL}.str() == "010203040506ab0708090a0b0c0d0e");
        check(chunkstore::hash_chunk_index(0) == 0);
        check(chunkstore::hash_chunk_index(1) == 0x5692161d100b05e5ULL);
        for (const auto sequence : std::array<std::uint64_t, 4>{0, 1, 0x100000000ULL, ChunkId::max_sequence}) {
            for (const auto space : {0, 1, 127, 255}) {
                for (const auto index : std::array<std::uint64_t, 5>{0, 1, 10000000, 0x100000000ULL, UINT64_MAX}) {
                    const ChunkId id{sequence, static_cast<std::uint8_t>(space), index};
                    const auto decoded = ChunkId::parse(id.str());
                    check(decoded.seq_id == sequence && decoded.space_id == space && decoded.index == index);
                    check(id.pg() == chunkstore::hash_chunk_index(index) % 10000000);
                    check(id.pg() == (ChunkId{1, 0, index}).pg());
                    check(chunkstore::unhash_chunk_index(chunkstore::hash_chunk_index(index)) == index);
                    check(chunkstore::hash_chunk_index(chunkstore::unhash_chunk_index(index)) == index);
                }
            }
        }
        std::uint64_t value = 1234567;
        for (int i = 0; i < 10000; ++i) {
            value = value * 6364136223846793005ULL + 1442695040888963407ULL;
            check(chunkstore::hash_chunk_index(chunkstore::unhash_chunk_index(value)) == value);
        }
        for (const auto bad : {"", "000000000000000000000000", "00000000000000000000000000000",
                               "0000000000000000000000000000000", "00000000000000000000000000000A",
                               "00000000000000000000000000000/", "00000000000000000000000000000000"}) {
            bool rejected = false;
            try { ChunkId::parse(bad); } catch (const std::invalid_argument&) { rejected = true; }
            check(rejected);
        }
        bool rejected = false;
        try { ChunkId{1ULL << 48, 0, 0}.str(); } catch (const std::invalid_argument&) { rejected = true; }
        check(rejected);
        for (const auto length : {24, 30, 32}) check(chunkstore::valid_storage_chunk_id(std::string(length, 'a')));
    } catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
