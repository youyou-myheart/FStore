#include "fmaster.pb.h"

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <iostream>
#include <stdexcept>
#include <string>

namespace wire = fstore::master::v1;

namespace {
void check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

void verify(const std::string& endpoint, const std::string& server_id,
            const std::string& directory, const std::string& file, const std::string& missing) {
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_BAIDU_STD;
    options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
    options.timeout_ms = 1000;
    options.max_retry = 0;
    check(channel.Init(endpoint.c_str(), &options) == 0, "could not initialize status channel");
    wire::FServerStatusService_Stub stub(&channel);
    auto call = [&](const wire::ServerHeartbeatRequest& request, wire::Code code) {
        brpc::Controller controller;
        wire::ServerHeartbeatResponse response;
        stub.Heartbeat(&controller, &request, &response, nullptr);
        check(!controller.Failed(), "status RPC transport failed");
        check(response.has_status() && response.status().code() == code, "wrong status RPC result");
        return response;
    };

    wire::ServerHeartbeatRequest request;
    request.set_config_revision(99);
    request.mutable_server()->set_id(server_id);
    request.mutable_server()->set_endpoint(endpoint);
    for (const auto& path : {directory, file, missing}) {
        auto* disk = request.mutable_server()->add_disks();
        disk->set_id("d" + std::to_string(request.server().disks_size()));
        disk->set_path(path);
    }
    const auto response = call(request, wire::OK);
    const auto& report = response.report();
    check(report.server_id() == server_id && report.endpoint() == endpoint && report.config_revision() == 99,
          "status response identity or revision mismatch");
    check(report.disks_size() == 3, "status response omitted configured disks");
    for (int i = 0; i < report.disks_size(); ++i) {
        const auto& disk = report.disks(i);
        check(disk.id() == request.server().disks(i).id() && disk.path() == request.server().disks(i).path(),
              "status response changed disk identity");
        if (i == 0) {
            check(disk.healthy() && disk.health() == wire::DISK_HEALTHY, "existing directory reported unhealthy");
            check(disk.total_bytes() > 0 && disk.total_bytes() >= disk.free_bytes() &&
                      disk.free_bytes() >= disk.available_bytes(), "invalid disk capacity report");
        } else {
            check(!disk.healthy() && disk.health() == wire::DISK_ERROR && !disk.error().empty(),
                  "unusable directory did not report error");
            check(disk.total_bytes() == 0 && disk.free_bytes() == 0 && disk.available_bytes() == 0,
                  "failed disk probe retained capacity");
        }
    }
    auto invalid = request;
    invalid.mutable_server()->set_id("wrong-server");
    call(invalid, wire::CONFLICT);
    invalid = request;
    invalid.mutable_server()->set_endpoint("127.0.0.2:" + endpoint.substr(endpoint.rfind(':') + 1));
    call(invalid, wire::CONFLICT);
    invalid = request;
    invalid.mutable_server()->mutable_disks(1)->set_id("d1");
    call(invalid, wire::INVALID_ARGUMENT);
    invalid = request;
    invalid.mutable_server()->mutable_disks(1)->set_path(directory + "/.");
    call(invalid, wire::INVALID_ARGUMENT);
    invalid = request;
    invalid.mutable_server()->mutable_disks(0)->set_path("relative/path");
    call(invalid, wire::INVALID_ARGUMENT);
    call(wire::ServerHeartbeatRequest{}, wire::INVALID_ARGUMENT);
    request.mutable_server()->clear_disks();
    check(call(request, wire::OK).report().disks_size() == 0, "empty disk configuration failed");
}
}

// fmaster.py owns the real FServer process and all probe paths.
int main(int argc, char** argv) {
    if (argc != 6) return 1;
    try {
        verify(argv[1], argv[2], argv[3], argv[4], argv[5]);
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
