#include "chunk_client.hpp"
#include "chunk_repair.hpp"
#include "chunk_id.hpp"
#include "unit.hpp"
#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <atomic>
#include <unistd.h>

using namespace chunkstore;
namespace wire = rpc::v1;
namespace {
class Reply final : public wire::ChunkService {
public:
    bool frozen = true, has_state = true;
    std::optional<wire::CloseReason> reason = wire::PG_REPAIR;
    std::atomic<unsigned> reads{0};
    std::atomic<std::uint64_t> sync_length{0}, sync_epoch{0}, sync_expected{0};
    std::atomic<bool> sync_has_expected{false}, sync_has_status{true};
    void SyncChunk(google::protobuf::RpcController*, const wire::SyncRequest* request,
                   wire::SyncResponse* response, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        sync_epoch = request->pg_epoch(); sync_has_expected = request->has_expected_length();
        sync_expected = request->expected_length();
        if (sync_has_status) response->mutable_status();
        response->set_length(sync_length);
    }
    void ReadChunk(google::protobuf::RpcController* call, const wire::ReadRequest* request,
                   wire::ReadResponse* response, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); ++reads; response->mutable_status();
        if (has_state) response->set_frozen(frozen);
        if (reason) response->set_close_reason(*reason);
        static_cast<brpc::Controller*>(call)->response_attachment().append(std::string(request->length(), 'x'));
    }
};
struct Node {
    Reply reply; brpc::Server server;
    Node() { unit::check(server.AddService(&reply, brpc::SERVER_DOESNT_OWN_SERVICE) == 0 && server.Start("127.0.0.1:0", nullptr) == 0, "start reply server"); }
    ~Node() { server.Stop(0); server.Join(); }
    std::string endpoint() const { return butil::endpoint2str(server.listen_address()).c_str(); }
};
void verify() {
    Node a, b, c;
    const std::string id(30, '1');
    ChunkClient single(a.endpoint());
    unit::check(single.read_with_state(id, 0, 1).state == State::Sealed, "single SDK lost repair cause");
    ReplicatedChunkClient replicas({a.endpoint(), b.endpoint(), c.endpoint()});
    unit::check(replicas.read_chunk(id, 0, 1).to_string() == "x" && b.reply.reads == 0 && c.reply.reads == 0,
                "sealed reply did not use immutable fast read");
    b.reply.reason = wire::USER_FREEZE;
    c.reply.reason = wire::NOT_CLOSED; c.reply.frozen = false;
    auto replies = replicas.read_replicas(id, 0, 1);
    unit::check(replies[0].state == State::Sealed && replies[1].state == State::Frozen && replies[2].state == State::Open,
                "per-replica close reasons lost");
    unit::check(replies[0].frozen == true && replies[1].frozen == true && replies[2].frozen == false, "legacy readonly flags changed");
    a.reply.reason.reset();
    unit::check(single.read_with_state(id, 0, 1).state == State::Frozen, "old server reply incompatible");
    a.reply.has_state = false;
    unit::check(!single.read_with_state(id, 0, 1).state, "missing old state was invented");
    a.reply.has_state = true; a.reply.reason = static_cast<wire::CloseReason>(99);
    unit::expect<RpcError>([&] { single.read_with_state(id, 0, 1); });
    a.reply.reason = wire::PG_REPAIR; a.reply.frozen = false;
    replies = replicas.read_replicas(id, 0, 1);
    unit::check(replies[0].error && !replies[0].state && !replies[0].frozen && replies[0].data.empty(), "malformed state exposed a successful replica");
}
void seal_page() {
    char directory[] = "/tmp/fstore-seal-page-XXXXXX";
    if (!::mkdtemp(directory)) throw std::runtime_error("mkdtemp");
    struct Cleanup { const char* root; ~Cleanup() { std::filesystem::remove_all(root); } } cleanup{directory};
    ChunkStore store(directory);
    std::vector<std::string> ids;
    for (unsigned n = 1; n <= 3; ++n) {
        ids.push_back(ChunkId{n, 1, unhash_chunk_index(37)}.str());
        store.create_chunk(ids.back(), 100); store.append(ids.back(), "data");
    }
    store.freeze_chunk(ids[0]); store.inventory().fence(37, 1, 0, PGMode::Source);
    wire::SealPGRequest request; request.set_pg_id(37); request.set_recovery_token(1); request.set_limit(2);
    request.set_inspect_only(true);
    wire::PGInventoryResponse inspected; seal_pg(store, request, inspected);
    unit::check(inspected.open_count() == 2 && inspected.chunks(1).close_reason() == wire::NOT_CLOSED, "inspection closed a chunk");
    request.set_inspect_only(false);
    wire::PGInventoryResponse first; seal_pg(store, request, first);
    unit::check(first.open_count() == 1 && first.has_next_chunk() && first.chunks(0).close_reason() == wire::USER_FREEZE &&
                first.chunks(1).close_reason() == wire::PG_REPAIR, "SealPG returned stale counts or overwrote user cause");
    request.set_after_chunk(first.next_chunk());
    wire::PGInventoryResponse last; seal_pg(store, request, last);
    unit::check(last.open_count() == 0 && !last.has_next_chunk() && last.chunks(0).close_reason() == wire::PG_REPAIR, "last seal page did not close PG");
}
void sync_response_validation() {
    Node node;
    ClientOptions options; options.pg_epoch = 19;
    ChunkClient client(node.endpoint(), options);
    const std::string id(30, '1');
    unit::check(client.sync_chunk(id, 0) == 0 && node.reply.sync_has_expected && node.reply.sync_expected == 0 &&
                node.reply.sync_epoch == 19, "sync lost zero expected length or epoch");
    node.reply.sync_length = 7;
    unit::check(client.sync_chunk(id) == 7 && !node.reply.sync_has_expected, "unconditional sync lost length");
    unit::expect<RpcError>([&] { client.sync_chunk(id, 8); });
    node.reply.sync_has_status = false;
    unit::expect<RpcError>([&] { client.sync_chunk(id); });
}
}
int main() { return unit::run({{"SDK close state and compatibility", verify}, {"SealPG page state and counts", seal_page},
    {"SyncChunk response validation and request fields", sync_response_validation}}); }
