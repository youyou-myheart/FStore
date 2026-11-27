#pragma once

#include "fmaster.pb.h"

#include <string>

namespace chunkstore {
class ChunkStore;

// FMaster polls this service on the FServer's chunk RPC endpoint. Identity is
// fixed at startup and checked before the requested disk paths are inspected.
class FServerStatusService final : public ::fstore::master::v1::FServerStatusService {
public:
    FServerStatusService(const std::string& server_id, const std::string& advertised_endpoint, ChunkStore* store = nullptr);

    void Heartbeat(google::protobuf::RpcController* controller,
                   const ::fstore::master::v1::ServerHeartbeatRequest* request,
                   ::fstore::master::v1::ServerHeartbeatResponse* response,
                   google::protobuf::Closure* done) override;

private:
    ChunkStore* store_;
    const std::string server_id_;
    const std::string endpoint_;
};

} // namespace chunkstore
