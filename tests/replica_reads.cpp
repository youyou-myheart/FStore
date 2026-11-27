#include "chunk_client.hpp"
#include "chunk_rpc_limits.hpp"

#include <chrono>
#include <future>
#include <iostream>

using namespace chunkstore;

namespace {
const std::string id(32, 'f');
constexpr std::size_t payload_size = 1024 * 1024 + 17;

void check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

std::string contents(std::size_t node, std::size_t length) {
    std::string value(length, '\0');
    for (std::size_t i = 0; i < length; ++i) value[i] = static_cast<char>((i * 31 + node) % 256);
    return value;
}

void check_success(const ReplicaReadResult& reply, const std::string& endpoint,
                   std::size_t node, std::size_t length) {
    check(reply.endpoint == endpoint, "replica results reordered");
    check(!reply.error, "unexpected replica error");
    check(reply.frozen.has_value() && *reply.frozen == (node == 0), "wrong frozen state");
    check(reply.state == (node == 0 ? State::Frozen : State::Open), "wrong detailed state");
    check(reply.data.equals(contents(node, length)), "wrong replica bytes");
}

template<class Error>
void check_error(const ReplicaReadResult& reply) {
    check(bool(reply.error), "expected a per-replica error");
    check(reply.data.empty() && !reply.frozen.has_value() && !reply.state, "failed read exposed data or state");
    try {
        std::rethrow_exception(reply.error);
    } catch (const Error&) {
        return;
    }
    throw std::runtime_error("wrong replica error type");
}

void setup_and_verify(const std::vector<std::string>& endpoints) {
    for (std::size_t i = 0; i < endpoints.size(); ++i) {
        ChunkClient node(endpoints[i]);
        node.create_chunk(id, payload_size);
        node.append(id, 0, contents(i, i == 2 ? 17 : payload_size), 0);
        if (i == 0) node.freeze_chunk(id);
    }
    ReplicatedChunkClient client(endpoints);
    auto verify = [&] {
        const auto replies = client.read_replicas(id, 0, 17);
        check(replies.size() == endpoints.size(), "read omitted replicas after frozen reply");
        for (std::size_t i = 0; i < replies.size(); ++i) check_success(replies[i], endpoints[i], i, 17);
    };
    verify();
    const auto replies = client.read_replicas(id, 0, payload_size);
    check(replies.size() == endpoints.size(), "partial failure omitted replicas");
    check_success(replies[0], endpoints[0], 0, payload_size);
    check_success(replies[1], endpoints[1], 1, payload_size);
    check_error<RemoteChunkError>(replies[2]);
    try {
        std::rethrow_exception(replies[2].error);
    } catch (const RemoteChunkError& error) {
        check(error.code() == rpc::v1::OUT_OF_RANGE, "range error code lost");
    }
    const auto empty = client.read_replicas(id, 17, 0);
    check(empty.size() == endpoints.size(), "zero-length read omitted replicas");
    for (std::size_t i = 0; i < empty.size(); ++i) check_success(empty[i], endpoints[i], i, 0);
    const auto missing = client.read_replicas(std::string(32, '9'), 0, 0);
    check(missing.size() == endpoints.size(), "all-failed read omitted replicas");
    for (const auto& reply : missing) check_error<RemoteChunkError>(reply);
    bool rejected = false;
    try {
        client.read_replicas(id, 0, max_rpc_payload + 1);
    } catch (const std::length_error&) {
        rejected = true;
    }
    check(rejected, "oversized replica read was accepted");

    std::vector<std::future<void>> readers;
    for (unsigned i = 0; i < 6; ++i) {
        readers.push_back(std::async(std::launch::async, [&] {
            for (unsigned j = 0; j < 5; ++j) verify();
        }));
    }
    for (auto& reader : readers) reader.get();
}

void verify_unavailable(const std::vector<std::string>& endpoints, bool timeout) {
    const auto start = std::chrono::steady_clock::now();
    std::vector<ReplicaReadResult> replies;
    {
        ReplicatedChunkClient client(endpoints, {.connect_timeout_ms = 300, .timeout_ms = 300});
        replies = client.read_replicas(id, 0, 17);
    }
    check(replies.size() == endpoints.size(), "unavailable replica omitted");
    check_success(replies[0], endpoints[0], 0, 17);
    check_success(replies[1], endpoints[1], 1, 17);
    check(replies[2].endpoint == endpoints[2], "error result endpoint lost");
    check_error<RpcError>(replies[2]);
    if (timeout) {
        check(std::chrono::steady_clock::now() - start >= std::chrono::milliseconds(150),
              "per-replica read returned before the paused replica timed out");
    }
    // A failed first replica must preserve the remaining replicas' independent results.
    const std::vector<std::string> reversed{endpoints[2], endpoints[1], endpoints[0]};
    ReplicatedChunkClient client(reversed, {.connect_timeout_ms = 300, .timeout_ms = 300});
    const auto reordered = client.read_replicas(id, 0, 17);
    check(reordered.size() == endpoints.size(), "failed first replica stopped fan-out");
    check_error<RpcError>(reordered[0]);
    check_success(reordered[1], endpoints[1], 1, 17);
    check_success(reordered[2], endpoints[0], 0, 17);
}
}

// Python owns three independent server processes and pauses or stops the third.
int main(int argc, char** argv) {
    if (argc != 5) return 1;
    try {
        const std::vector<std::string> endpoints(argv + 2, argv + argc);
        if (std::string_view(argv[1]) == "setup") setup_and_verify(endpoints);
        else verify_unavailable(endpoints, std::string_view(argv[1]) == "timeout");
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
