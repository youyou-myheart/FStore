#include "chunk_client.hpp"
#include "chunk_rpc_limits.hpp"

#include <charconv>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <google/protobuf/util/json_util.h>
#include <limits>

namespace {
std::uint64_t number(std::string_view text) {
    std::uint64_t value;
    const auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (ec != std::errc{} || end != text.data() + text.size()) {
        throw std::invalid_argument("expected an unsigned integer");
    }
    return value;
}

template<class Client>
void write_chunk(const Client& client, std::string_view command, char** argv) {
    const auto* id = argv[3];
    if (command == "create") {
        std::cout << chunkstore::state_name(client.create_chunk(id, number(argv[4]))) << '\n';
    } else if (command == "append") {
        const auto expected_offset = number(argv[4]);
        const auto wait = number(argv[5]);
        if (wait > chunkstore::max_gap_wait_ms) {
            throw std::invalid_argument("WAIT_MS must be in 0..60000");
        }
        std::ifstream file;
        std::istream* input = &std::cin;
        if (std::string_view(argv[6]) != "-") {
            file.open(argv[6], std::ios::binary);
            if (!file) {
                throw std::runtime_error("could not open append input");
            }
            input = &file;
        }
        std::string data(chunkstore::max_rpc_payload + 1, '\0');
        input->read(data.data(), data.size());
        if (input->bad() || (input->fail() && !input->eof())) {
            throw std::runtime_error("could not read append input");
        }
        data.resize(input->gcount());
        const auto [offset, length] = client.append(
            id, expected_offset, data, static_cast<std::uint32_t>(wait));
        std::cout << offset << ' ' << length << '\n';
    } else {
        std::cout << client.freeze_chunk(id, number(argv[4])) << '\n';
    }
}

template<class Client>
void read_chunk(const Client& client, char** argv) {
    const auto length = number(argv[5]);
    if (length > chunkstore::max_rpc_payload) {
        throw std::length_error("RPC payload exceeds 8 MiB");
    }
    const auto data = client.read_chunk(argv[3], number(argv[4]), static_cast<std::size_t>(length));
    for (std::size_t i = 0; i < data.backing_block_num(); ++i) {
        const auto block = data.backing_block(i);
        std::cout.write(block.data(), block.size());
    }
}

std::vector<std::string> split_replicas(std::string_view list) {
    std::vector<std::string> endpoints;
    for (;;) {
        const auto comma = list.find(',');
        endpoints.emplace_back(list.substr(0, comma));
        if (comma == list.npos) return endpoints;
        list.remove_prefix(comma + 1);
    }
}

}

int main(int argc, char** argv) {
    chunkstore::ClientOptions options;
    if (argc > 3 && std::string_view(argv[argc - 1]).starts_with("--pg-epoch=")) {
        try { options.pg_epoch = number(std::string_view(argv[--argc]).substr(11)); }
        catch (const std::exception& error) { std::cerr << error.what() << '\n'; return EXIT_FAILURE; }
    }
    const std::string_view command = argc > 2 ? argv[2] : "";
    const bool valid = (command == "stats" && argc == 3) ||
                       (command == "state" && argc == 4) ||
                       (command == "seal" && (argc == 5 || argc == 6)) ||
                       (command == "pgs" && argc >= 3 && argc <= 5) ||
                       (command == "pg-chunks" && argc >= 4 && argc <= 6) ||
                       ((command == "create" || command == "restore") && argc == 5) ||
                       (command == "repair" && (argc == 5 || argc == 6)) ||
                       (command == "freeze" && (argc == 4 || argc == 5)) || (command == "delete" && argc == 4) ||
                       (command == "append" && argc == 7) || (command == "read" && argc == 6);
    if (!valid) {
        std::cerr << "usage: chunk_cli ENDPOINT create ID CAPACITY_BYTES\n"
                     "       chunk_cli ENDPOINT append ID OFFSET WAIT_MS FILE|-\n"
                     "       chunk_cli ENDPOINT read ID OFFSET LENGTH\n"
                     "       chunk_cli SOURCE repair ID DESTINATION [high|normal|low]\n"
                     "       chunk_cli ENDPOINT delete ID\n"
                     "       chunk_cli ENDPOINT restore ID DELETED_AT_NS\n"
                     "       chunk_cli ENDPOINT stats\n"
                     "       chunk_cli ENDPOINT state ID\n"
                     "       chunk_cli ENDPOINT seal ID RECOVERY_TOKEN [EXPECTED_LENGTH]\n"
                     "       chunk_cli ENDPOINT pgs [START [LIMIT]]\n"
                     "       chunk_cli ENDPOINT pg-chunks PG_ID [AFTER_CHUNK [LIMIT]]\n"
                     "       chunk_cli ENDPOINT freeze ID [EXPECTED_LENGTH]\n"
                     "       chunk_cli HOST:PORT,HOST:PORT,... create ID CAPACITY_BYTES\n"
                     "       chunk_cli HOST:PORT,HOST:PORT,... append ID OFFSET WAIT_MS FILE|-\n"
                     "       chunk_cli HOST:PORT,HOST:PORT,... freeze ID EXPECTED_LENGTH\n"
                     "       chunk_cli HOST:PORT,HOST:PORT,... read ID OFFSET LENGTH\n"
                     "       append --pg-epoch=N to data commands after a PG route change\n";
        return EXIT_FAILURE;
    }
    try {
        const std::string_view endpoints = argv[1];
        if (endpoints.find(',') != endpoints.npos) {
            if (command != "append" && command != "create" && command != "read" && !(command == "freeze" && argc == 5)) {
                throw std::invalid_argument("replica list supports create, append, read and freeze ID EXPECTED_LENGTH");
            }
            chunkstore::ReplicatedChunkClient client(split_replicas(endpoints), options);
            if (command == "read") read_chunk(client, argv);
            else write_chunk(client, command, argv);
            std::cout.flush();
            return std::cout ? EXIT_SUCCESS : EXIT_FAILURE;
        }
        chunkstore::ChunkClient client(argv[1], options);
        if (command == "pgs" || command == "pg-chunks") {
            auto uint32 = [](const char* text) {
                const auto value = number(text);
                if (value > UINT32_MAX) throw std::invalid_argument("integer exceeds uint32");
                return static_cast<std::uint32_t>(value);
            };
            auto print = [](const google::protobuf::Message& message) {
                std::string json;
                google::protobuf::util::JsonPrintOptions format;
                format.preserve_proto_field_names = true; format.always_print_primitive_fields = true;
                const auto status = google::protobuf::util::MessageToJsonString(message, &json, format);
                if (!status.ok()) throw std::runtime_error(status.ToString());
                std::cout << json << '\n';
            };
            if (command == "pgs") print(client.list_pgs(argc > 3 ? uint32(argv[3]) : 0, argc > 4 ? uint32(argv[4]) : 100));
            else print(client.pg_chunks(uint32(argv[3]), argc > 4 ? argv[4] : "", argc > 5 ? uint32(argv[5]) : 100));
            return std::cout ? EXIT_SUCCESS : EXIT_FAILURE;
        }
        if (command == "stats") {
            std::cout << client.traffic_stats().DebugString();
            return std::cout ? EXIT_SUCCESS : EXIT_FAILURE;
        }
        const auto* id = argv[3];
        if (command == "state") {
            const auto read = client.read_with_state(id, 0, 0);
            std::cout << (read.state ? chunkstore::state_name(*read.state) : "unknown") << '\n';
        } else if (command == "seal") {
            std::cout << client.seal_chunk(id, number(argv[4]), argc == 6 ? std::optional<std::uint64_t>(number(argv[5])) : std::nullopt) << '\n';
        } else if (command == "delete") {
            std::cout << client.delete_chunk(id) << '\n';
        } else if (command == "restore") {
            std::cout << chunkstore::state_name(client.restore_chunk(id, number(argv[4]))) << '\n';
        } else if (command == "repair") {
            chunkstore::rpc::v1::CopyPriority priority = chunkstore::rpc::v1::NORMAL;
            const std::string_view label = argc == 6 ? argv[5] : "normal";
            if (label == "high") priority = chunkstore::rpc::v1::HIGH;
            else if (label == "low") priority = chunkstore::rpc::v1::LOW;
            else if (label != "normal") throw std::invalid_argument("invalid replication priority");
            std::cout << client.repair_chunk(id, argv[4], priority) << '\n';
        } else if (command == "create" || command == "append" || (command == "freeze" && argc == 5)) {
            write_chunk(client, command, argv);
        } else if (command == "read") {
            read_chunk(client, argv);
        } else {
            std::cout << client.freeze_chunk(id) << '\n';
        }
        std::cout.flush();
        if (!std::cout) {
            throw std::runtime_error("could not write command output");
        }
    } catch (const chunkstore::RemoteChunkError& e) {
        std::cerr << "chunk error (" << e.code() << "): " << e.what() << '\n';
        return EXIT_FAILURE;
    } catch (const chunkstore::RpcError& e) {
        std::cerr << "RPC error (" << e.code() << "): " << e.what() << '\n';
        return EXIT_FAILURE;
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return EXIT_FAILURE;
    }
}
