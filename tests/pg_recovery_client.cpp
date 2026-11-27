#include "space_client.hpp"
#include <iostream>

int main(int argc, char** argv) {
    try {
        if (argc != 6) throw std::invalid_argument("seed|read MASTER SPACE COUNT|ID LENGTH");
        chunkstore::SpaceClient client(argv[2], argv[3]);
        const auto length = std::stoul(argv[5]);
        const std::string payload(length, 'x');
        if (std::string_view(argv[1]) == "seed") {
            const auto chunks = client.allocate_chunks(std::stoul(argv[4]));
            for (std::size_t i = 0; i < chunks.size(); ++i) {
                chunks[i].create(); chunks[i].append(0, payload, 0);
                if (i % 2 == 0) chunks[i].freeze(length);
                std::cout << chunks[i].id() << '\n';
            }
        } else {
            const auto expected_state = std::string_view(argv[1]) == "read-sealed" ? chunkstore::State::Sealed : chunkstore::State::Frozen;
            const auto chunk = client.open_chunk(argv[4]);
            if (chunk.read(0, length).to_string() != payload) throw std::runtime_error("logical read mismatch");
            for (const auto& replica : chunk.read_replicas(0, length)) {
                if (replica.error) std::rethrow_exception(replica.error);
                if (replica.data.to_string() != payload || !replica.frozen.value_or(false) || replica.state != expected_state) throw std::runtime_error("replica data/state mismatch");
            }
        }
    } catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
