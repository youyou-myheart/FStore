#include "fmaster_client.hpp"

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <cerrno>

namespace chunkstore::master {
namespace {
template<class Response>
void check_response(const brpc::Controller& controller, const Response& response) {
    if (controller.Failed()) throw RpcError(controller.ErrorCode(), controller.ErrorText());
    if (!response.has_status()) throw RpcError(EPROTO, "missing status in FMaster response");
    if (response.status().code() != wire::OK) {
        throw RemoteError(response.status().code(), response.status().message());
    }
}
}

struct Client::Impl {
    brpc::Channel channel;
    wire::FMasterService_Stub stub{&channel};

    Impl(const std::string& endpoint, int connect_timeout_ms, int timeout_ms) {
        if (endpoint.empty() || endpoint.find('\0') != std::string::npos) {
            throw std::invalid_argument("FMaster endpoint must be non-empty and contain no NUL bytes");
        }
        brpc::ChannelOptions options;
        options.protocol = brpc::PROTOCOL_BAIDU_STD;
        options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
        options.connect_timeout_ms = connect_timeout_ms;
        options.timeout_ms = timeout_ms;
        options.max_retry = 0;
        if (channel.Init(endpoint.c_str(), &options) != 0) {
            throw std::invalid_argument("could not initialize FMaster bRPC channel for " + endpoint);
        }
    }
};

Client::Client(const std::string& endpoint, int connect_timeout_ms, int timeout_ms)
    : impl_(std::make_unique<Impl>(endpoint, connect_timeout_ms, timeout_ms)) {}
Client::~Client() = default;

wire::ListResponse Client::list(const std::string& server_id) const {
    wire::ListRequest request;
    request.set_server_id(server_id);
    wire::ListResponse response;
    brpc::Controller controller;
    impl_->stub.List(&controller, &request, &response, nullptr);
    check_response(controller, response);
    if (!response.has_topology()) throw RpcError(EPROTO, "missing topology in FMaster list response");
    return response;
}

std::uint64_t Client::set_server(const wire::SetServerRequest& request) const {
    wire::MutationResponse response;
    brpc::Controller controller;
    impl_->stub.SetServer(&controller, &request, &response, nullptr);
    check_response(controller, response);
    return response.revision();
}

std::uint64_t Client::set_disk(const wire::SetDiskRequest& request) const {
    wire::MutationResponse response;
    brpc::Controller controller;
    impl_->stub.SetDisk(&controller, &request, &response, nullptr);
    check_response(controller, response);
    return response.revision();
}

std::uint64_t Client::set_server_state(const wire::SetServerStateRequest& request) const {
    wire::MutationResponse response;
    brpc::Controller controller;
    impl_->stub.SetServerState(&controller, &request, &response, nullptr);
    check_response(controller, response);
    return response.revision();
}

std::uint64_t Client::set_disk_state(const wire::SetDiskStateRequest& request) const {
    wire::MutationResponse response;
    brpc::Controller controller;
    impl_->stub.SetDiskState(&controller, &request, &response, nullptr);
    check_response(controller, response);
    return response.revision();
}

std::uint64_t Client::remove_server(const wire::RemoveServerRequest& request) const {
    wire::MutationResponse response;
    brpc::Controller controller;
    impl_->stub.RemoveServer(&controller, &request, &response, nullptr);
    check_response(controller, response);
    return response.revision();
}

std::uint64_t Client::remove_disk(const wire::RemoveDiskRequest& request) const {
    wire::MutationResponse response;
    brpc::Controller controller;
    impl_->stub.RemoveDisk(&controller, &request, &response, nullptr);
    check_response(controller, response);
    return response.revision();
}

std::uint64_t Client::apply_config(const wire::ApplyConfigRequest& request) const {
    wire::MutationResponse response;
    brpc::Controller controller;
    impl_->stub.ApplyConfig(&controller, &request, &response, nullptr);
    check_response(controller, response);
    return response.revision();
}

wire::ValidateConfigResponse Client::validate_config(const wire::ValidateConfigRequest& request) const {
    wire::ValidateConfigResponse response;
    brpc::Controller controller;
    impl_->stub.ValidateConfig(&controller, &request, &response, nullptr);
    check_response(controller, response);
    if (!response.has_topology()) throw RpcError(EPROTO, "missing validated configuration");
    return response;
}

wire::ProbeNowResponse Client::probe_now(const std::string& server_id) const {
    wire::ProbeNowRequest request;
    request.set_server_id(server_id);
    wire::ProbeNowResponse response;
    brpc::Controller controller;
    impl_->stub.ProbeNow(&controller, &request, &response, nullptr);
    check_response(controller, response);
    return response;
}

} // namespace chunkstore::master
