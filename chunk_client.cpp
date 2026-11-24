#include "chunk_client.hpp"
#include "chunk_rpc_limits.hpp"

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <algorithm>
#include <cerrno>
#include <limits>

namespace chunkstore {
namespace {
namespace wire = rpc::v1;

void check_size(std::size_t size) {
    if (size > max_rpc_payload) {
        throw std::length_error("RPC payload exceeds 8 MiB");
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
    check_size(data.size());
    butil::IOBuf buffer;
    if (!data.empty() && buffer.append(data.data(), data.size()) != 0) {
        throw std::bad_alloc();
    }
    return append(id, offset, buffer, wait_timeout_ms);
}

AppendResult ChunkClient::append(std::string_view id, std::uint64_t offset,
                                 const butil::IOBuf& data, std::uint32_t wait_timeout_ms) const {
    check_size(data.size());
    if (wait_timeout_ms > max_gap_wait_ms || data.empty()) {
        throw std::invalid_argument("append requires non-empty data and a timeout of 0..60000 ms");
    }
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

} // namespace chunkstore
