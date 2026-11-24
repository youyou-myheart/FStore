#include "chunk_client.hpp"
#include "chunk_rpc_limits.hpp"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <bthread/bthread.h>
#include <butil/endpoint.h>

#include <algorithm>
#include <cerrno>
#include <limits>
#include <set>
#include <type_traits>

namespace chunkstore {
namespace {
namespace wire = rpc::v1;

void check_size(std::size_t size) {
    if (size > max_rpc_payload) {
        throw std::length_error("RPC payload exceeds 8 MiB");
    }
}

butil::IOBuf append_buffer(std::string_view data) {
    check_size(data.size());
    butil::IOBuf buffer;
    if (!data.empty() && buffer.append(data.data(), data.size()) != 0) {
        throw std::bad_alloc();
    }
    return buffer;
}

void check_append(std::size_t size, std::uint32_t wait_timeout_ms) {
    check_size(size);
    if (wait_timeout_ms > max_gap_wait_ms || size == 0) {
        throw std::invalid_argument("append requires non-empty data and a timeout of 0..60000 ms");
    }
}

template<class Response>
void check_response(const brpc::Controller& controller, const Response& response) {
    if (controller.Failed()) {
        throw RpcError(controller.ErrorCode(), controller.ErrorText());
    }
    if (!response.has_status()) {
        throw RpcError(EPROTO, "missing status in RPC response");
    }
    if (response.status().code() != wire::OK) {
        throw RemoteChunkError(response.status().code(), response.status().message());
    }
}
}

struct ChunkClient::Impl {
    brpc::Channel channel;
    wire::ChunkService_Stub stub{&channel};
    int timeout_ms;

    Impl(const std::string& endpoint, ClientOptions options) : timeout_ms(options.timeout_ms) {
        brpc::ChannelOptions settings;
        settings.protocol = brpc::PROTOCOL_BAIDU_STD;
        settings.connection_type = brpc::CONNECTION_TYPE_SINGLE;
        settings.connect_timeout_ms = options.connect_timeout_ms;
        settings.timeout_ms = options.timeout_ms;
        settings.max_retry = 0;
        if (channel.Init(endpoint.c_str(), &settings) != 0) {
            throw std::invalid_argument("could not initialize bRPC channel for " + endpoint);
        }
    }
};

ChunkClient::ChunkClient(const std::string& endpoint, ClientOptions options)
    : impl_(std::make_unique<Impl>(endpoint, options)) {}
ChunkClient::~ChunkClient() = default;

std::uint64_t ChunkClient::repair_chunk(std::string_view id, const std::string& destination,
                                        int timeout_ms) const {
    return repair_chunk(id, destination, wire::NORMAL, timeout_ms);
}

std::uint64_t ChunkClient::repair_chunk(std::string_view id, const std::string& destination,
                                        wire::CopyPriority priority, int timeout_ms) const {
    wire::RepairRequest request;
    wire::RepairResponse response;
    brpc::Controller controller;
    controller.set_timeout_ms(timeout_ms);
    request.set_chunk_id(std::string(id));
    request.set_destination(destination);
    request.set_priority(priority);
    impl_->stub.RepairChunk(&controller, &request, &response, nullptr);
    check_response(controller, response);
    return response.length();
}

rpc::v1::TrafficStatsResponse ChunkClient::traffic_stats() const {
    wire::TrafficStatsRequest request;
    wire::TrafficStatsResponse response;
    brpc::Controller controller;
    impl_->stub.GetTrafficStats(&controller, &request, &response, nullptr);
    if (controller.Failed()) throw RpcError(controller.ErrorCode(), controller.ErrorText());
    return response;
}

State ChunkClient::create_chunk(std::string_view id, std::uint64_t capacity) const {
    wire::CreateRequest request;
    wire::CreateResponse response;
    brpc::Controller controller;
    request.set_chunk_id(std::string(id));
    request.set_capacity(capacity);
    impl_->stub.CreateChunk(&controller, &request, &response, nullptr);
    check_response(controller, response);
    if (response.capacity() != capacity) {
        throw RpcError(EPROTO, "create response capacity mismatch");
    }
    return response.frozen() ? State::Frozen : State::Open;
}

AppendResult ChunkClient::append(std::string_view id, std::uint64_t offset,
                                 std::string_view data, std::uint32_t wait_timeout_ms) const {
    return append(id, offset, append_buffer(data), wait_timeout_ms);
}

AppendResult ChunkClient::append(std::string_view id, std::uint64_t offset,
                                 const butil::IOBuf& data, std::uint32_t wait_timeout_ms) const {
    check_append(data.size(), wait_timeout_ms);
    wire::AppendRequest request;
    wire::AppendResponse response;
    brpc::Controller controller;
    request.set_chunk_id(std::string(id));
    request.set_length(data.size());
    request.set_offset(offset);
    request.set_wait_timeout_ms(wait_timeout_ms);
    if (impl_->timeout_ms >= 0) {
        const auto total = static_cast<std::int64_t>(impl_->timeout_ms) + wait_timeout_ms;
        controller.set_timeout_ms(static_cast<int>(
            std::min<std::int64_t>(total, std::numeric_limits<int>::max())));
    }
    controller.request_attachment().append(data);
    impl_->stub.Append(&controller, &request, &response, nullptr);
    check_response(controller, response);
    if (response.length() != data.size() || response.offset() != offset) {
        throw RpcError(EPROTO, "append response range mismatch");
    }
    return {response.offset(), response.length()};
}

butil::IOBuf ChunkClient::read_chunk(std::string_view id, std::uint64_t offset,
                                      std::size_t length) const {
    check_size(length);
    wire::ReadRequest request;
    wire::ReadResponse response;
    brpc::Controller controller;
    request.set_chunk_id(std::string(id));
    request.set_offset(offset);
    request.set_length(length);
    impl_->stub.ReadChunk(&controller, &request, &response, nullptr);
    check_response(controller, response);
    if (controller.response_attachment().size() != length) {
        throw RpcError(EPROTO, "read attachment length mismatch");
    }
    butil::IOBuf result;
    result.swap(controller.response_attachment());
    return result;
}

std::uint64_t ChunkClient::delete_chunk(std::string_view id) const {
    wire::ChunkRequest request;
    wire::DeleteResponse response;
    brpc::Controller controller;
    request.set_chunk_id(std::string(id));
    impl_->stub.DeleteChunk(&controller, &request, &response, nullptr);
    check_response(controller, response);
    return response.deleted_at_ns();
}

State ChunkClient::restore_chunk(std::string_view id, std::uint64_t deleted_at_ns) const {
    wire::RestoreRequest request;
    wire::RestoreResponse response;
    brpc::Controller controller;
    request.set_chunk_id(std::string(id));
    request.set_deleted_at_ns(deleted_at_ns);
    impl_->stub.RestoreChunk(&controller, &request, &response, nullptr);
    check_response(controller, response);
    return response.frozen() ? State::Frozen : State::Open;
}

std::uint64_t ChunkClient::freeze_chunk(std::string_view id) const {
    wire::ChunkRequest request;
    wire::FreezeResponse response;
    brpc::Controller controller;
    request.set_chunk_id(std::string(id));
    impl_->stub.FreezeChunk(&controller, &request, &response, nullptr);
    check_response(controller, response);
    return response.length();
}

std::uint64_t ChunkClient::freeze_chunk(std::string_view id, std::uint64_t expected_length) const {
    wire::FreezeAtLengthRequest request;
    wire::FreezeResponse response;
    brpc::Controller controller;
    request.set_chunk_id(std::string(id));
    request.set_expected_length(expected_length);
    impl_->stub.FreezeChunkAtLength(&controller, &request, &response, nullptr);
    check_response(controller, response);
    if (response.length() != expected_length) throw RpcError(EPROTO, "freeze response length mismatch");
    return response.length();
}

namespace {
std::string describe(State state) { return state == State::Open ? "open" : "frozen"; }
std::string describe(std::uint64_t length) { return std::to_string(length); }

std::string describe_replies(const std::vector<ReplicaReply>& replies) {
    std::string message = "replica commit incomplete";
    for (const auto& reply : replies) {
        message += "\n  " + reply.endpoint + " ";
        if (!reply.error) {
            message += "ack " + reply.acknowledgement;
            continue;
        }
        try {
            std::rethrow_exception(reply.error);
        } catch (const RemoteChunkError& e) {
            message += "chunk error (" + std::to_string(e.code()) + "): " + e.what();
        } catch (const RpcError& e) {
            message += "RPC error (" + std::to_string(e.code()) + "): " + e.what();
        } catch (const std::exception& e) {
            message += e.what();
        } catch (...) {
            message += "unknown error";
        }
    }
    return message;
}
}

ReplicaCommitError::ReplicaCommitError(std::vector<ReplicaReply> replies)
    : std::runtime_error(describe_replies(replies)), replies_(std::move(replies)) {}

ReplicatedChunkClient::ReplicatedChunkClient(const std::vector<std::string>& endpoints, ClientOptions options) {
    if (endpoints.empty()) throw std::invalid_argument("at least one replica is required");
    std::set<std::string> resolved;
    for (const auto& endpoint : endpoints) {
        butil::EndPoint address;
        if (endpoint.empty() || endpoint.find('\0') != std::string::npos ||
            (butil::str2endpoint(endpoint.c_str(), &address) != 0 &&
             butil::hostname2endpoint(endpoint.c_str(), &address) != 0)) {
            throw std::invalid_argument("invalid replica endpoint: " + endpoint);
        }
        const std::string target = butil::endpoint2str(address).c_str();
        if (!resolved.insert(target).second) throw std::invalid_argument("duplicate replica: " + endpoint);
        nodes_.push_back({endpoint, std::make_unique<ChunkClient>(target, options)});
    }
}

template<class Operation>
auto ReplicatedChunkClient::commit(const Operation& operation) const {
    using Value = std::invoke_result_t<Operation, const ChunkClient&>;
    struct Call {
        const ChunkClient* client;
        const Operation* operation;
        Value value{};
        std::exception_ptr error;
        bthread_t thread{};
        bool started = false;
    };
    std::vector<Call> calls(nodes_.size());
    auto run = [](void* arg) -> void* {
        auto& call = *static_cast<Call*>(arg);
        try {
            call.value = (*call.operation)(*call.client);
        } catch (...) {
            call.error = std::current_exception();
        }
        return nullptr;
    };
    // Allocate before launching. The calling thread handles the last replica.
    for (std::size_t i = 0; i < calls.size(); ++i) {
        auto& call = calls[i];
        call.client = nodes_[i].client.get();
        call.operation = &operation;
        if (i + 1 < calls.size()) {
            call.started = bthread_start_background(&call.thread, nullptr, run, &call) == 0;
        }
    }
    for (auto& call : calls) {
        if (!call.started) run(&call);
    }
    for (const auto& call : calls) {
        if (call.started) bthread_join(call.thread, nullptr);
    }
    const auto value = calls.front().value;
    const bool agreed = std::all_of(calls.begin(), calls.end(), [value](const Call& call) {
        return !call.error && call.value == value;
    });
    if (agreed) return value;

    std::vector<ReplicaReply> replies;
    for (std::size_t i = 0; i < calls.size(); ++i) {
        const auto& call = calls[i];
        replies.push_back({nodes_[i].endpoint, call.error ? "" : describe(call.value), call.error});
    }
    throw ReplicaCommitError(std::move(replies));
}

State ReplicatedChunkClient::create_chunk(std::string_view id, std::uint64_t capacity) const {
    return commit([&](const ChunkClient& client) { return client.create_chunk(id, capacity); });
}

AppendResult ReplicatedChunkClient::append(std::string_view id, std::uint64_t offset,
                                           std::string_view data, std::uint32_t wait_timeout_ms) const {
    return append(id, offset, append_buffer(data), wait_timeout_ms);
}

AppendResult ReplicatedChunkClient::append(std::string_view id, std::uint64_t offset,
                                           const butil::IOBuf& data, std::uint32_t wait_timeout_ms) const {
    check_append(data.size(), wait_timeout_ms);
    const auto length = commit([&](const ChunkClient& client) -> std::uint64_t {
        wire::Code conflict;
        try {
            return client.append(id, offset, data, wait_timeout_ms).length;
        } catch (const RemoteChunkError& e) {
            if (e.code() != wire::OFFSET_CONFLICT && e.code() != wire::FROZEN) throw;
            conflict = e.code();
        }
        // A previous attempt may have committed before its acknowledgement was lost.
        try {
            if (client.read_chunk(id, offset, data.size()).equals(data)) return data.size();
        } catch (const RemoteChunkError& e) {
            if (e.code() != wire::OUT_OF_RANGE) throw;
            throw RemoteChunkError(conflict, "requested range is not fully committed");
        }
        throw RemoteChunkError(conflict, "committed bytes differ at the requested range");
    });
    return {offset, length};
}

std::uint64_t ReplicatedChunkClient::freeze_chunk(std::string_view id, std::uint64_t expected_length) const {
    return commit([&](const ChunkClient& client) { return client.freeze_chunk(id, expected_length); });
}

} // namespace chunkstore
