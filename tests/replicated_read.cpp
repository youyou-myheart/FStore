#include "chunk_client.hpp"

#include <future>
#include <iostream>

// Called by replicated_client.py against real servers, including a paused node.
int main(int argc, char** argv) {
    if (argc < 4) return 1;
    try {
        const std::string id = argv[1], expected = argv[2];
        const std::vector<std::string> endpoints(argv + 3, argv + argc);
        auto read = [&](const chunkstore::ReplicatedChunkClient& client) {
            if (!client.read_chunk(id, 0, expected.size()).equals(expected)) {
                throw std::runtime_error("unexpected replica read result");
            }
        };
        // Cancellation callbacks may still be running when these clients die.
        for (unsigned i = 0; i < 20; ++i) {
            chunkstore::ReplicatedChunkClient client(endpoints);
            read(client);
        }
        chunkstore::ReplicatedChunkClient client(endpoints);
        std::vector<std::future<void>> readers;
        for (unsigned i = 0; i < 8; ++i) {
            readers.push_back(std::async(std::launch::async, [&] {
                for (unsigned j = 0; j < 10; ++j) read(client);
            }));
        }
        for (auto& reader : readers) reader.get();
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
