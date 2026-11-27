#include "space_client.hpp"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <iostream>
#include <map>
#include <vector>

namespace {
unsigned number(std::string_view value, unsigned maximum) {
    unsigned result = 0;
    const auto parsed = std::from_chars(value.data(), value.data() + value.size(), result);
    if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size() || !result || result > maximum) {
        throw std::invalid_argument("invalid positive request or batch count");
    }
    return result;
}
}

int main(int argc, char** argv) {
    if (argc < 3 || argc > 5) {
        std::cerr << "usage: space_bench MASTER SPACE [BLOBS=1000 [CHUNKS_PER_BLOB=1]]\n"
                  << "Allocates real Blob IDs and plans routes; does not create or write Chunk files.\n";
        return 2;
    }
    try {
        const auto count = argc >= 4 ? number(argv[3], 1000000) : 1000;
        const auto batch = argc == 5 ? number(argv[4], 1024) : 1;
        chunkstore::SpaceClient client(argv[1], argv[2]);
        std::vector<double> latency;
        latency.reserve(count);
        std::map<unsigned, unsigned> pgs;
        const auto begin = std::chrono::steady_clock::now();
        for (unsigned i = 0; i < count; ++i) {
            const auto start = std::chrono::steady_clock::now();
            auto chunks = client.allocate_chunks(batch);
            latency.push_back(std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - start).count());
            for (const auto& chunk : chunks) ++pgs[chunk.route().pg().id()];
        }
        const auto seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count();
        std::sort(latency.begin(), latency.end());
        unsigned maximum = 0;
        for (const auto& [id, load] : pgs) { (void)id; maximum = std::max(maximum, load); }
        const auto pool = client.replica_pool_stats();
        std::cout << "{\"blobs\":" << count << ",\"chunks_per_blob\":" << batch
                  << ",\"distinct_pgs\":" << pgs.size() << ",\"max_pg_chunks\":" << maximum
                  << ",\"blobs_per_second\":" << count / seconds
                  << ",\"chunk_routes_per_second\":" << static_cast<double>(count) * batch / seconds
                  << ",\"p50_us\":" << latency[count / 2] << ",\"p99_us\":" << latency[count * 99 / 100]
                  << ",\"pool_hits\":" << pool.hits << ",\"pool_misses\":" << pool.misses
                  << ",\"cached_clients\":" << pool.cached_clients << "}\n";
    } catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
