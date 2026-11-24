// Run against disposable server data directories. Each run creates fresh chunks.
#include "chunk_client.hpp"
#include "chunk_rpc_limits.hpp"

#include <algorithm>
#include <barrier>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <cstddef>
#include <iostream>
#include <random>
#include <thread>

using Clock = std::chrono::steady_clock;

int positive(std::string_view arg) {
    int n;
    auto [end, error] = std::from_chars(arg.data(), arg.data() + arg.size(), n);
    if (error != std::errc{} || end != arg.data() + arg.size() || n <= 0)
        throw std::invalid_argument("expected a positive integer");
    return n;
}

int main(int argc, char** argv) try {
    if (argc < 6) throw std::invalid_argument("usage: chunk_bench read|append THREADS OPS_PER_THREAD BYTES ENDPOINT...");
    const bool read = std::string_view(argv[1]) == "read";
    if (!read && std::string_view(argv[1]) != "append") throw std::invalid_argument("unknown operation");
    const int threads = positive(argv[2]), ops = positive(argv[3]), size = positive(argv[4]);
    if (size > chunkstore::max_rpc_payload || (read && argc != 6))
        throw std::invalid_argument("read requires one endpoint; maximum request size is 8 MiB");
    std::vector<std::string> endpoints(argv + 5, argv + argc);
    chunkstore::ChunkClient node(endpoints.front());
    chunkstore::ReplicatedChunkClient replicas(endpoints);
    const auto run = std::mt19937_64(std::random_device{}())();
    butil::IOBuf payload;
    if (payload.append(std::string(size, 'x')) != 0) throw std::bad_alloc();
    std::vector<std::string> ids;
    for (int i = 0; i < (read ? 1 : threads); ++i) {
        char id[33];
        std::snprintf(id, sizeof(id), "%016llx%016x", static_cast<unsigned long long>(run), i);
        ids.emplace_back(id);
        replicas.create_chunk(id, (std::uint64_t(ops) + 1) * size);
        replicas.append(id, 0, payload, 0);
    }

    std::vector<double> us(std::size_t(threads) * ops);
    std::vector<std::exception_ptr> errors(threads);
    std::barrier start(static_cast<std::ptrdiff_t>(threads) + 1);
    bool aborted = false;
    std::vector<std::jthread> workers;
    auto worker = [&](int i) {
        start.arrive_and_wait();
        if (aborted) return;
        try {
            for (int j = 0; j < ops; ++j) {
                const auto begin = Clock::now();
                const auto offset = (std::uint64_t(j) + 1) * size;
                if (read) node.read_chunk(ids.front(), 0, size);
                else if (endpoints.size() == 1) node.append(ids[i], offset, payload, 0);
                else replicas.append(ids[i], offset, payload, 0);
                us[std::size_t(i) * ops + j] = std::chrono::duration<double, std::micro>(Clock::now() - begin).count();
            }
        } catch (...) { errors[i] = std::current_exception(); }
    };
    try {
        for (int i = 0; i < threads; ++i) workers.emplace_back(worker, i);
    } catch (...) {
        aborted = true;
        // Release participants already waiting if an OS thread cannot be created.
        (void)start.arrive(static_cast<std::ptrdiff_t>(threads) + 1 - workers.size());
        throw;
    }
    const auto begin = Clock::now();
    start.arrive_and_wait();
    workers.clear();
    const auto seconds = std::chrono::duration<double>(Clock::now() - begin).count();
    for (const auto& error : errors) if (error) std::rethrow_exception(error);
    std::sort(us.begin(), us.end());
    std::cout << "ops/s=" << us.size() / seconds << " p50_us=" << us[us.size()/2]
              << " p99_us=" << us[(us.size()-1)*99/100] << '\n';
} catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
    return 1;
}
