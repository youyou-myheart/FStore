#include "space_client.hpp"

#include <iostream>
#include <future>
#include <set>

int main(int argc, char** argv) {
    if (argc != 3) return 2;
    try {
        chunkstore::SpaceClient client(argv[1], argv[2]);
        const auto blob = client.allocate_blob();
        auto first = client.allocate_chunks(blob.seq_id(), blob.start_cursor(), 2);
        if (first.chunks.front().route().pg().id() != blob.start_cursor()) throw std::runtime_error("SDK lost initial cursor");
        if (!first.next_cursor || first.chunks.size() != 2) throw std::runtime_error("missing sparse batch");
        auto second = client.allocate_chunks(blob.seq_id(), *first.next_cursor, 1);
        if (second.chunks.front().decoded_id().seq_id != blob.seq_id() ||
            chunkstore::hash_chunk_index(second.chunks.front().decoded_id().index) <= chunkstore::hash_chunk_index(first.chunks.back().decoded_id().index)) {
            throw std::runtime_error("Blob sequence binding or continuation failed");
        }
        auto chunk = std::move(first.chunks.front());
        // The caller retains the ID before attempting a multi-replica create.
        std::cout << chunk.id() << std::endl;
        chunk.create();
        const std::string payload(4096, 'x');
        chunk.append(0, payload, 1000);
        if (chunk.read(0, payload.size()).to_string() != payload) throw std::runtime_error("quorum read mismatch");
        const auto replicas = chunk.read_replicas(0, payload.size());
        if (replicas.size() != chunk.route().space().spec().replicas()) throw std::runtime_error("missing replica");
        for (const auto& replica : replicas) {
            if (replica.error || replica.data.to_string() != payload) throw std::runtime_error("replica data mismatch");
        }
        bool overflow = false;
        try { chunk.append(payload.size(), std::string(65536, 'y'), 1000); }
        catch (const chunkstore::ReplicaCommitError&) { overflow = true; }
        if (!overflow) throw std::runtime_error("space chunk size not enforced");
        chunk.freeze(payload.size());
        auto reopened = client.open_chunk(chunk.id());
        if (reopened.read(0, payload.size()).to_string() != payload) throw std::runtime_error("located read mismatch");
        for (const auto& replica : reopened.read_replicas(0, payload.size())) {
            if (replica.error || !replica.frozen.value_or(false)) throw std::runtime_error("replica not frozen");
        }
        // Many handles on the same three servers need at most six ordered
        // replica clients. Reuse survives releasing each allocation batch.
        for (int i = 0; i < 2; ++i) {
            auto warm = client.allocate_chunks(blob.seq_id(), blob.start_cursor(), 401);
            if (warm.chunks.size() != 401) throw std::runtime_error("incomplete pooled batch");
        }
        const auto stats = client.replica_pool_stats();
        if (stats.cached_clients > 6 || stats.misses > 6 || stats.hits < 790) throw std::runtime_error("replica clients are not reused");
        std::vector<std::future<void>> readers;
        for (int i = 0; i < 8; ++i) readers.push_back(std::async(std::launch::async, [&] {
            for (int j = 0; j < 8; ++j) {
                auto handle = client.open_chunk(chunk.id());
                if (handle.read(0, payload.size()).to_string() != payload) throw std::runtime_error("concurrent pooled read mismatch");
            }
        }));
        for (auto& reader : readers) reader.get();
        auto retained = [&] {
            chunkstore::SpaceClient temporary(argv[1], argv[2]);
            return temporary.open_chunk(chunk.id());
        }();
        if (retained.read(0, payload.size()).to_string() != payload) throw std::runtime_error("handle lost client with pool owner");
        std::set<unsigned> starts;
        for (const auto& next_blob : client.allocate_blobs(100)) {
            auto batch = client.allocate_chunks(next_blob.seq_id(), 1);
            starts.insert(batch.chunks.front().route().pg().id());
        }
        if (starts.size() < 60) throw std::runtime_error("SDK first batch concentrates small Blobs");
    } catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
