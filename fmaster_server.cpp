#include "fmaster_store.hpp"
#include "fmaster_monitor.hpp"
#include "fmaster_recovery.hpp"

#include <brpc/closure_guard.h>
#include <brpc/server.h>

#include <charconv>
#include <iostream>
#include <set>

namespace {
namespace master = chunkstore::master;
namespace wire = fstore::master::v1;

template<class Response, class Operation>
void complete(google::protobuf::Closure* done, Response* response, Operation operation) {
    brpc::ClosureGuard guard(done);
    try {
        operation();
        response->mutable_status()->set_code(wire::OK);
    } catch (const master::Error& error) {
        response->mutable_status()->set_code(error.code());
        response->mutable_status()->set_message(error.what());
    } catch (const std::invalid_argument& error) {
        response->mutable_status()->set_code(wire::INVALID_ARGUMENT);
        response->mutable_status()->set_message(error.what());
    } catch (const std::exception& error) {
        response->mutable_status()->set_code(wire::STORAGE_ERROR);
        response->mutable_status()->set_message(error.what());
    }
}

class Service final : public wire::FMasterService {
public:
    Service(master::Store& store, master::Monitor& monitor) : store_(store), monitor_(monitor) {}

    void List(google::protobuf::RpcController*, const wire::ListRequest* request,
              wire::ListResponse* response, google::protobuf::Closure* done) override {
        complete(done, response, [&] { store_.list(*response, request->server_id()); });
    }

    void SetServer(google::protobuf::RpcController*, const wire::SetServerRequest* request,
                   wire::MutationResponse* response, google::protobuf::Closure* done) override {
        complete(done, response, [&] {
            response->set_revision(store_.set_server(*request));
        });
    }

    void SetDisk(google::protobuf::RpcController*, const wire::SetDiskRequest* request,
                 wire::MutationResponse* response, google::protobuf::Closure* done) override {
        complete(done, response, [&] {
            response->set_revision(store_.set_disk(*request));
        });
    }

    void SetServerState(google::protobuf::RpcController*, const wire::SetServerStateRequest* request,
                        wire::MutationResponse* response, google::protobuf::Closure* done) override {
        complete(done, response, [&] { response->set_revision(store_.set_server_state(*request)); });
    }

    void SetDiskState(google::protobuf::RpcController*, const wire::SetDiskStateRequest* request,
                      wire::MutationResponse* response, google::protobuf::Closure* done) override {
        complete(done, response, [&] { response->set_revision(store_.set_disk_state(*request)); });
    }

    void RemoveServer(google::protobuf::RpcController*, const wire::RemoveServerRequest* request,
                      wire::MutationResponse* response, google::protobuf::Closure* done) override {
        complete(done, response, [&] {
            response->set_revision(store_.remove_server(*request));
        });
    }

    void RemoveDisk(google::protobuf::RpcController*, const wire::RemoveDiskRequest* request,
                    wire::MutationResponse* response, google::protobuf::Closure* done) override {
        complete(done, response, [&] {
            response->set_revision(store_.remove_disk(*request));
        });
    }

    void ApplyConfig(google::protobuf::RpcController*, const wire::ApplyConfigRequest* request,
                     wire::MutationResponse* response, google::protobuf::Closure* done) override {
        complete(done, response, [&] {
            response->set_revision(store_.apply_config(*request));
        });
    }

    void ValidateConfig(google::protobuf::RpcController*, const wire::ValidateConfigRequest* request,
                        wire::ValidateConfigResponse* response, google::protobuf::Closure* done) override {
        complete(done, response, [&] { store_.validate_config(*request, *response); });
    }

    void ProbeNow(google::protobuf::RpcController*, const wire::ProbeNowRequest* request,
                  wire::ProbeNowResponse* response, google::protobuf::Closure* done) override {
        complete(done, response, [&] {
            response->set_scheduled_servers(monitor_.request_probe(request->server_id()));
        });
    }

    void ListRecoveries(google::protobuf::RpcController*, const wire::ListRecoveriesRequest* request,
                        wire::ListRecoveriesResponse* response, google::protobuf::Closure* done) override {
        complete(done, response, [&] { store_.list_recoveries(*request, *response); });
    }

    void CreateSpace(google::protobuf::RpcController*, const wire::CreateSpaceRequest* request,
                    wire::SpaceResponse* response, google::protobuf::Closure* done) override {
        complete(done, response, [&] { store_.create_space(*request, *response); });
    }

    void BindPGs(google::protobuf::RpcController*, const wire::BindPGsRequest* request,
                 wire::SpaceResponse* response, google::protobuf::Closure* done) override {
        complete(done, response, [&] { store_.bind_pgs(*request, *response); });
    }
    void UnbindPGs(google::protobuf::RpcController*, const wire::BindPGsRequest* request,
                   wire::SpaceResponse* response, google::protobuf::Closure* done) override {
        complete(done, response, [&] { store_.unbind_pgs(*request, *response); });
    }
    void ListPGPool(google::protobuf::RpcController*, const wire::ListPGPoolRequest* request,
                    wire::ListPGPoolResponse* response, google::protobuf::Closure* done) override {
        complete(done, response, [&] { store_.list_pg_pool(*request, *response); });
    }

    void ListSpaces(google::protobuf::RpcController*, const wire::ListSpacesRequest* request,
                    wire::ListSpacesResponse* response, google::protobuf::Closure* done) override {
        complete(done, response, [&] { store_.list_spaces(*request, *response); });
    }

    void ListPGs(google::protobuf::RpcController*, const wire::ListPGsRequest* request,
                    wire::ListPGsResponse* response, google::protobuf::Closure* done) override {
        complete(done, response, [&] { store_.list_pgs(*request, *response); });
    }

    void AllocateBlobs(google::protobuf::RpcController*, const wire::AllocateBlobsRequest* request,
                       wire::AllocateBlobsResponse* response, google::protobuf::Closure* done) override {
        complete(done, response, [&] { store_.allocate_blobs(*request, *response); });
    }

    void AllocateChunks(google::protobuf::RpcController*, const wire::AllocateChunksRequest* request,
                    wire::AllocateChunksResponse* response, google::protobuf::Closure* done) override {
        complete(done, response, [&] { store_.allocate_chunks(*request, *response); });
    }

    void LocateChunk(google::protobuf::RpcController*, const wire::LocateChunkRequest* request,
                    wire::LocateChunkResponse* response, google::protobuf::Closure* done) override {
        complete(done, response, [&] { store_.locate_chunk(*request, *response); });
    }

private:
    master::Store& store_;
    master::Monitor& monitor_;
};

struct Options {
    std::filesystem::path directory;
    std::filesystem::path config;
    std::string endpoint = "127.0.0.1:8090";
    std::chrono::milliseconds timeout{10000};
    std::chrono::milliseconds interval{2000};
    std::chrono::milliseconds probe_timeout{1000};
    std::chrono::milliseconds recovery_grace{600000}, recovery_interval{1000}, recovery_timeout{300000};
};

std::chrono::milliseconds duration(std::string_view value) {
    std::uint32_t number = 0;
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), number);
    if (error != std::errc{} || end != value.data() + value.size() || number == 0) {
        throw std::invalid_argument("heartbeat duration must be a positive uint32 in milliseconds");
    }
    return std::chrono::milliseconds(number);
}

Options parse(int argc, char** argv) {
    Options options;
    std::set<std::string_view> seen;
    for (int i = 1; i < argc; ++i) {
        const std::string_view argument = argv[i];
        const auto separator = argument.find('=');
        if (separator == argument.npos || separator + 1 == argument.size()) {
            throw std::invalid_argument("expected --option=value");
        }
        const auto name = argument.substr(0, separator);
        const auto value = argument.substr(separator + 1);
        if (!seen.insert(name).second) throw std::invalid_argument("duplicate option " + std::string(name));
        if (name == "--data-dir") options.directory = value;
        else if (name == "--config") options.config = value;
        else if (name == "--listen") options.endpoint = value;
        else if (name == "--heartbeat-timeout-ms") options.timeout = duration(value);
        else if (name == "--heartbeat-interval-ms") options.interval = duration(value);
        else if (name == "--recovery-grace-ms") options.recovery_grace = duration(value);
        else if (name == "--recovery-interval-ms") options.recovery_interval = duration(value);
        else if (name == "--recovery-rpc-timeout-ms") options.recovery_timeout = duration(value);
        else if (name == "--probe-timeout-ms") options.probe_timeout = duration(value);
        else throw std::invalid_argument("unknown option " + std::string(name));
    }
    if (options.directory.empty()) throw std::invalid_argument("--data-dir is required");
    if (options.interval >= options.timeout) {
        throw std::invalid_argument("heartbeat interval must be shorter than heartbeat timeout");
    }
    return options;
}
}

int main(int argc, char** argv) {
    try {
        const auto config = parse(argc, argv);
        master::Store store(config.directory, config.config, config.timeout, config.interval);
        master::Monitor monitor(store, config.interval, config.probe_timeout);
        master::RecoveryMonitor recovery(store, config.recovery_grace, config.recovery_interval, config.recovery_timeout);
        Service service(store, monitor);
        brpc::Server server;
        if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) return EXIT_FAILURE;
        brpc::ServerOptions options;
        options.has_builtin_services = false;
        options.enabled_protocols = "baidu_std";
        options.max_concurrency = 64;
        if (server.Start(config.endpoint.c_str(), &options) != 0) return EXIT_FAILURE;
        server.RunUntilAskedToQuit();
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n'
                  << "usage: fmaster --data-dir=DIR [--listen=HOST:PORT] [--config=FILE]"
                     " [--heartbeat-timeout-ms=10000] [--heartbeat-interval-ms=2000] [--probe-timeout-ms=1000] [--recovery-grace-ms=600000] [--recovery-interval-ms=1000] [--recovery-rpc-timeout-ms=300000]\n";
        return EXIT_FAILURE;
    }
}
