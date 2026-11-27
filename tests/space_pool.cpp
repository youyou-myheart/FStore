#include "space_client.hpp"

#include <brpc/closure_guard.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <barrier>
#include <atomic>
#include <future>
#include <iostream>

namespace wire = fstore::master::v1;

namespace {
void check(bool value, const char* message) { if (!value) throw std::runtime_error(message); }

// Controlled route cardinality tests cache eviction without launching hundreds
// of storage processes. No data I/O is issued to these endpoint fixtures.
class Routes final : public wire::FMasterService {
public:
    std::atomic<std::uint64_t> epoch{0};
private:
    void LocateChunk(google::protobuf::RpcController*, const wire::LocateChunkRequest* request,
                     wire::LocateChunkResponse* response, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        const auto id = chunkstore::ChunkId::parse(request->chunk_id());
        response->mutable_status();
        auto* route = response->mutable_chunk();
        route->set_chunk_id(request->chunk_id());
        auto* space = route->mutable_space();
        space->set_name("pool"); space->set_id(0); space->set_total_pgs(chunkstore::global_pg_count);
        space->mutable_spec()->set_replicas(1); space->mutable_spec()->set_chunk_size(65536);
        auto* pg = route->mutable_pg(); pg->set_active(true); pg->set_id(id.pg());
        pg->set_epoch(epoch.load());
        auto* replica = pg->add_replicas(); replica->set_server_id("s" + std::to_string(id.index));
        replica->set_endpoint("127.0.0.1:" + std::to_string(10000 + id.index));
    }
};

std::string id(unsigned index) { return chunkstore::ChunkId{1, 0, index}.str(); }
}

int main() {
    try {
        Routes routes;
        brpc::Server server;
        check(server.AddService(&routes, brpc::SERVER_DOESNT_OWN_SERVICE) == 0, "failed to add route service");
        check(server.Start("127.0.0.1:0", nullptr) == 0, "failed to start route service");
        chunkstore::SpaceClient client(butil::endpoint2str(server.listen_address()).c_str(), "pool");
        auto retained = client.open_chunk(id(0));
        for (unsigned i = 1; i < 256; ++i) client.open_chunk(id(i));
        auto stats = client.replica_pool_stats();
        check(stats.cached_clients == 256 && stats.misses == 256 && stats.hits == 0, "incorrect cache fill");
        client.open_chunk(id(255));
        check(client.replica_pool_stats().hits == 1, "resident client was not reused");
        client.open_chunk(id(256));
        client.open_chunk(id(0));
        stats = client.replica_pool_stats();
        check(stats.cached_clients == 256 && stats.misses == 258, "cache is unbounded or eviction failed");
        check(retained.id() == id(0), "eviction changed a live handle");
        // Simultaneous cache misses may construct competing clients, but only
        // one entry is retained; no duplicate insertion/eviction bookkeeping.
        std::barrier start(8);
        std::vector<std::future<void>> workers;
        for (int i = 0; i < 8; ++i) workers.push_back(std::async(std::launch::async, [&] {
            start.arrive_and_wait();
            for (int j = 0; j < 20; ++j) client.open_chunk(id(400));
        }));
        for (auto& worker : workers) worker.get();
        stats = client.replica_pool_stats();
        check(stats.cached_clients == 256 && stats.hits >= 153, "concurrent cache use failed");
        const auto misses = stats.misses;
        routes.epoch.store(7);
        const auto refreshed = client.open_chunk(id(400));
        check(refreshed.route().pg().epoch() == 7 && client.replica_pool_stats().misses == misses + 1,
              "new PG epoch reused old replica client");
        client.open_chunk(id(400));
        check(client.replica_pool_stats().misses == misses + 1 && retained.route().pg().epoch() == 0,
              "epoch cache miss repeated or existing handle changed");
        server.Stop(0); server.Join();
    } catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
