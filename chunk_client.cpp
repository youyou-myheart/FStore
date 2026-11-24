#include "chunk_client.hpp"
#include "chunk_rpc_limits.hpp"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <bthread/bthread.h>
#include <bthread/butex.h>
#include <butil/endpoint.h>

#include <algorithm>
#include <atomic>
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

void check_read_response(const brpc::Controller& controller, const wire::ReadResponse& response,
                         std::size_t length) {
    check_response(controller, response);
    if (controller.response_attachment().size() != length) {
        throw RpcError(EPROTO, "read attachment length mismatch");
    }
}

wire::AppendRequest append_request(std::string_view id, std::uint64_t offset, std::size_t length,
                                   std::uint32_t wait_timeout_ms) {
    wire::AppendRequest request;
    request.set_chunk_id(std::string(id));
    request.set_length(length);
    request.set_offset(offset);
    request.set_wait_timeout_ms(wait_timeout_ms);
    return request;
}

void prepare_append(brpc::Controller& controller, const butil::IOBuf& data, int timeout_ms,
                    std::uint32_t wait_timeout_ms) {
    if (timeout_ms >= 0) {
        const auto total = static_cast<std::int64_t>(timeout_ms) + wait_timeout_ms;
        controller.set_timeout_ms(static_cast<int>(
            std::min<std::int64_t>(total, std::numeric_limits<int>::max())));
    }
    controller.request_attachment().append(data);
}

AppendResult append_result(const brpc::Controller& controller, const wire::AppendResponse& response,
                           std::uint64_t offset, std::size_t length) {
    check_response(controller, response);
    if (response.length() != length || response.offset() != offset) {
        throw RpcError(EPROTO, "append response range mismatch");
    }
    return {response.offset(), response.length()};
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
    wire::AppendResponse response;
    brpc::Controller controller;
    auto request = append_request(id, offset, data.size(), wait_timeout_ms);
    prepare_append(controller, data, impl_->timeout_ms, wait_timeout_ms);
    impl_->stub.Append(&controller, &request, &response, nullptr);
    return append_result(controller, response, offset, data.size());
}

butil::IOBuf ChunkClient::read_chunk(std::string_view id, std::uint64_t offset,
                                      std::size_t length) const {
    return read_with_state(id, offset, length).data;
}

ChunkClient::ReadResult ChunkClient::read_with_state(std::string_view id, std::uint64_t offset,
                                                      std::size_t length) const {
    check_size(length);
    wire::ReadRequest request;
    wire::ReadResponse response;
    brpc::Controller controller;
    request.set_chunk_id(std::string(id));
    request.set_offset(offset);
    request.set_length(length);
    impl_->stub.ReadChunk(&controller, &request, &response, nullptr);
    check_read_response(controller, response, length);
    butil::IOBuf result;
    result.swap(controller.response_attachment());
    return {std::move(result), response.has_frozen() && response.frozen()};
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

std::string describe_replies(const std::vector<ReplicaReply>& replies, std::string message) {
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

ReplicaCommitError::ReplicaCommitError(std::vector<ReplicaReply> replies, std::string message)
    : std::runtime_error(describe_replies(replies, std::move(message))), replies_(std::move(replies)) {}

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
    using Value = std::invoke_result_t<Operation, const ChunkClient&, std::size_t>;
    struct Call {
        const ChunkClient* client;
        const Operation* operation;
        std::size_t index;
        Value value{};
        std::exception_ptr error;
        bthread_t thread{};
        bool started = false;
    };
    std::vector<Call> calls(nodes_.size());
    auto run = [](void* arg) -> void* {
        auto& call = *static_cast<Call*>(arg);
        try {
            call.value = (*call.operation)(*call.client, call.index);
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
        call.index = i;
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

butil::IOBuf ReplicatedChunkClient::read_chunk(std::string_view id, std::uint64_t offset,
                                              std::size_t length) const {
    check_size(length);
    ChunkClient::ReadResult first;
    std::exception_ptr first_error;
    try {
        first = nodes_.front().client->read_with_state(id, offset, length);
        if (first.frozen || nodes_.size() == 1) return std::move(first.data);
    } catch (...) {
        first_error = std::current_exception();
    }

    // Each callback owns the batch until it finishes. This lets a successful
    // read cancel pending calls and return without joining slow replicas.
    struct Batch {
        struct Call : google::protobuf::Closure {
            brpc::Controller controller;
            wire::ReadResponse response;
            ChunkClient::ReadResult result;
            std::exception_ptr error;
            std::shared_ptr<Batch> owner;
            std::atomic<bool> ready{false};
            brpc::CallId call_id{};
            bool started = false;
            bool consumed = false;

            void Run() override {
                auto batch = std::move(owner);
                try {
                    check_read_response(controller, response, batch->request.length());
                    result.frozen = response.has_frozen() && response.frozen();
                    result.data.swap(controller.response_attachment());
                } catch (...) {
                    error = std::current_exception();
                }
                ready.store(true, std::memory_order_release);
                batch->completed->fetch_add(1, std::memory_order_release);
                bthread::butex_wake(batch->completed);
            }
        };

        explicit Batch(std::size_t size) : calls(size) {
            completed = bthread::butex_create_checked<std::atomic<int>>();
            if (!completed) throw std::bad_alloc();
            completed->store(1, std::memory_order_relaxed);
        }
        ~Batch() { bthread::butex_destroy(completed); }

        wire::ReadRequest request;
        std::vector<Call> calls;
        std::atomic<int>* completed;
    };
    const auto batch = std::make_shared<Batch>(nodes_.size());
    batch->request.set_chunk_id(std::string(id));
    batch->request.set_offset(offset);
    batch->request.set_length(length);
    batch->calls.front().result = std::move(first);
    batch->calls.front().error = first_error;
    batch->calls.front().ready.store(true, std::memory_order_relaxed);

    struct Vote {
        std::size_t representative;
        std::size_t count;
    };
    std::vector<Vote> votes;
    votes.reserve(nodes_.size());
    struct CancelPending {
        const std::shared_ptr<Batch>& batch;
        ~CancelPending() {
            for (auto& call : batch->calls) {
                if (call.started && !call.ready.load(std::memory_order_acquire)) {
                    brpc::StartCancel(call.call_id);
                }
            }
        }
    } cancel{batch};
    for (std::size_t i = 1; i < nodes_.size(); ++i) {
        auto& call = batch->calls[i];
        call.call_id = call.controller.call_id();
        call.owner = batch;
        call.started = true;
        nodes_[i].client->impl_->stub.ReadChunk(&call.controller, &batch->request, &call.response, &call);
    }

    const auto quorum = nodes_.size() / 2 + 1;
    std::size_t remaining = nodes_.size();
    while (remaining != 0) {
        const auto completed = batch->completed->load(std::memory_order_acquire);
        for (std::size_t i = 0; i < batch->calls.size(); ++i) {
            auto& call = batch->calls[i];
            if (call.consumed || !call.ready.load(std::memory_order_acquire)) continue;
            call.consumed = true;
            --remaining;
            if (call.error) continue;
            if (call.result.frozen) return std::move(call.result.data);
            const auto match = std::find_if(votes.begin(), votes.end(), [&](const Vote& vote) {
                return batch->calls[vote.representative].result.data.equals(call.result.data);
            });
            if (match == votes.end()) votes.push_back({i, 1});
            else ++match->count;
        }
        // Prefer any frozen response already received over the open votes.
        for (const auto& vote : votes) {
            if (vote.count >= quorum) return std::move(batch->calls[vote.representative].result.data);
        }
        if (remaining != 0) bthread::butex_wait(batch->completed, completed, nullptr);
    }

    std::vector<ReplicaReply> replies;
    for (std::size_t i = 0; i < batch->calls.size(); ++i) {
        const auto& call = batch->calls[i];
        std::string acknowledgement;
        if (!call.error) {
            const auto match = std::find_if(votes.begin(), votes.end(), [&](const Vote& vote) {
                return batch->calls[vote.representative].result.data.equals(call.result.data);
            });
            acknowledgement = "read " + std::to_string(length) + " bytes, matching replicas " +
                              std::to_string(match->count) + "/" + std::to_string(nodes_.size());
        }
        replies.push_back({nodes_[i].endpoint, std::move(acknowledgement), call.error});
    }
    throw ReplicaCommitError(std::move(replies),
                             "replica read quorum not reached, required " + std::to_string(quorum));
}

std::vector<ReplicaReadResult> ReplicatedChunkClient::read_replicas(
        std::string_view id, std::uint64_t offset, std::size_t length) const {
    check_size(length);
    std::vector<ReplicaReadResult> replies(nodes_.size());
    for (std::size_t i = 0; i < nodes_.size(); ++i) {
        replies[i].endpoint = nodes_[i].endpoint;
    }
    wire::ReadRequest request;
    request.set_chunk_id(std::string(id));
    request.set_offset(offset);
    request.set_length(length);
    struct Call {
        brpc::Controller controller;
        wire::ReadResponse response;
        bool started = false;
        void join() {
            if (started) {
                brpc::Join(controller.call_id());
                started = false;
            }
        }
        ~Call() { join(); }
    };
    // Start every RPC before waiting. Keep the request and all controllers live
    // through completion, including if a later launch throws.
    std::vector<Call> calls(nodes_.size());
    for (std::size_t i = 0; i < nodes_.size(); ++i) {
        auto& call = calls[i];
        nodes_[i].client->impl_->stub.ReadChunk(&call.controller, &request, &call.response, brpc::DoNothing());
        call.started = true;
    }
    for (std::size_t i = 0; i < calls.size(); ++i) {
        auto& call = calls[i];
        auto& reply = replies[i];
        call.join();
        try {
            check_read_response(call.controller, call.response, length);
            if (call.response.has_frozen()) reply.frozen = call.response.frozen();
            reply.data.swap(call.controller.response_attachment());
        } catch (...) {
            reply.error = std::current_exception();
        }
    }
    return replies;
}

State ReplicatedChunkClient::create_chunk(std::string_view id, std::uint64_t capacity) const {
    return commit([&](const ChunkClient& client, std::size_t) { return client.create_chunk(id, capacity); });
}

AppendResult ReplicatedChunkClient::append(std::string_view id, std::uint64_t offset,
                                           std::string_view data, std::uint32_t wait_timeout_ms) const {
    return append(id, offset, append_buffer(data), wait_timeout_ms);
}

AppendResult ReplicatedChunkClient::append(std::string_view id, std::uint64_t offset,
                                           const butil::IOBuf& data, std::uint32_t wait_timeout_ms) const {
    check_append(data.size(), wait_timeout_ms);
    // Prepare every attachment before starting any RPC. The immutable request and
    // each controller stay live until Join, including when a later launch fails.
    struct Call {
        brpc::Controller controller;
        wire::AppendResponse response;
        std::exception_ptr error;
        bool started = false;
        void join() {
            if (started) {
                brpc::Join(controller.call_id());
                started = false;
            }
        }
        ~Call() { join(); }
    };
    auto request = append_request(id, offset, data.size(), wait_timeout_ms);
    std::vector<Call> calls(nodes_.size());
    for (std::size_t i = 0; i < nodes_.size(); ++i) {
        prepare_append(calls[i].controller, data, nodes_[i].client->impl_->timeout_ms, wait_timeout_ms);
    }
    for (std::size_t i = 0; i < nodes_.size(); ++i) {
        auto& call = calls[i];
        nodes_[i].client->impl_->stub.Append(&call.controller, &request, &call.response, brpc::DoNothing());
        call.started = true;
    }
    bool all_confirmed = true;
    for (auto& call : calls) {
        call.join();
        try {
            append_result(call.controller, call.response, offset, data.size());
        } catch (...) {
            call.error = std::current_exception();
            all_confirmed = false;
        }
    }
    if (all_confirmed) return {offset, data.size()};

    // Only failed range checks need a second, possibly blocking RPC. Use the
    // same parallel verifier and per-endpoint error reporting as lifecycle calls.
    const auto length = commit([&](const ChunkClient& client, std::size_t i) -> std::uint64_t {
        if (!calls[i].error) return data.size();
        wire::Code conflict;
        try {
            std::rethrow_exception(calls[i].error);
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
    return commit([&](const ChunkClient& client, std::size_t) { return client.freeze_chunk(id, expected_length); });
}

} // namespace chunkstore
