#include "fmaster_fixture.hpp"
#include "chunk_id.hpp"
#include "fmaster_placement.hpp"
#include "unit.hpp"
#include <fstream>
#include <iterator>
#include <unistd.h>

using namespace fmaster_test;
using unit::check;
namespace fs = std::filesystem;
namespace {
struct Directory {
    fs::path path;
    Directory() { char name[] = "/tmp/fstore-master-recovery-XXXXXX"; if (!::mkdtemp(name)) throw std::runtime_error("mkdtemp"); path = name; }
    ~Directory() { fs::remove_all(path); }
};
void healthy(master::Store& store, const std::string& id, bool capable = true) {
    wire::ListResponse state; store.list(state, id);
    const auto& server = state.topology().servers(0);
    wire::HeartbeatReport heartbeat; heartbeat.set_server_id(id); heartbeat.set_endpoint(server.endpoint());
    heartbeat.set_config_revision(state.topology().revision()); heartbeat.set_pg_recovery_capable(capable);
    for (const auto& disk : server.disks()) {
        auto* item = heartbeat.add_disks(); item->set_id(disk.id()); item->set_path(disk.path()); item->set_healthy(true);
        item->set_total_bytes(1000000); item->set_free_bytes(900000); item->set_available_bytes(800000);
    }
    store.record_heartbeat(heartbeat);
}
wire::ListRecoveriesResponse jobs(master::Store& store) {
    wire::ListRecoveriesRequest request; wire::ListRecoveriesResponse result; store.list_recoveries(request, result); return result;
}
std::uint64_t fail(master::Store& store, const std::string& id) {
    wire::ListResponse state; store.list(state, id);
    store.record_probe_failure(id, state.topology().servers(0).endpoint(), state.topology().revision(), "connection refused");
    const auto result = jobs(store);
    for (const auto& f : result.failures()) if (f.server_id() == id) return f.since_unix_ms();
    throw std::runtime_error("failure timer missing");
}
void configure(master::Store& store, bool spare = true) {
    for (unsigned n = 1; n <= 3; ++n) { node(store, n, "a"); healthy(store, "s" + std::to_string(n)); }
    wire::SpaceResponse response; store.create_space(creation("logs", wire::PlacementPolicy::SINGLE_DATACENTER), response);
    wire::BindPGsRequest unbind; unbind.set_name("logs"); unbind.set_start(1); unbind.set_count(199); store.unbind_pgs(unbind, response);
    if (spare) { node(store, 4, "a"); healthy(store, "s4"); }
}
void grace_restart_and_publication() {
    Directory root; std::uint64_t since, sequence, job_id;
    {
        master::Store store(root.path); configure(store); sequence = blob(store, "logs");
        since = fail(store, "s1");
        store.plan_recoveries(since + 599999); check(jobs(store).recoveries().empty(), "scheduled before ten minutes");
        store.plan_recoveries(since - 1); check(jobs(store).recoveries().empty(), "backwards clock bypassed grace");
    }
    {
        master::Store store(root.path);
        check(jobs(store).failures(0).since_unix_ms() == since, "failure timer not durable");
        for (const auto* name : {"s2", "s3", "s4"}) healthy(store, name);
        store.plan_recoveries(since + 600000); check(jobs(store).recoveries().empty(), "restart scheduled without fresh failure probe");
        check(fail(store, "s1") == since, "restart reset continuous failure timer");
        store.plan_recoveries(since + 600000);
        const auto result = jobs(store); check(result.recoveries_size() == 1, "did not schedule at ten minutes");
        const auto& job = result.recoveries(0); job_id = job.id();
        check(job.phase() == wire::PREPARING && job.failed_servers(0) == "s1" && job.new_pg().epoch() == job_id, "incorrect recovery plan");
        auto route = groups(store, "logs", 0, 1).pgs(0);
        check(route.recovering() && route.epoch() == 0, "published replacement before copy");
        unit::expect<master::Error>([&] { chunks(store, "logs", sequence, 0, 1); });
        store.advance_recovery(job_id, wire::PREPARING, wire::SEALING);
        store.advance_recovery(job_id, wire::SEALING, wire::COPYING);
        const auto cursor = chunkstore::ChunkId{sequence, 0, 0}.str();
        store.advance_recovery(job_id, wire::COPYING, wire::COPYING, cursor, 16, 65536);
        unit::expect<master::Error>([&] { store.advance_recovery(job_id, wire::COPYING, wire::COPYING, cursor); });
        unit::expect<master::Error>([&] { store.advance_recovery(job_id, wire::COPYING, wire::RECOVERED); });
        store.recovery_error(job_id, "interrupted transfer");
    }
    {
        master::Store store(root.path);
        auto job = jobs(store).recoveries(0);
        check(job.id() == job_id && job.phase() == wire::COPYING && !job.cursor().empty() && job.copied_chunks() == 16 && !job.error().empty(), "copy progress not durable");
        store.advance_recovery(job_id, wire::COPYING, wire::VERIFYING);
        store.advance_recovery(job_id, wire::VERIFYING, wire::ACTIVATING);
        check(groups(store, "logs", 0, 1).pgs(0).recovering(), "published before activation");
        store.advance_recovery(job_id, wire::ACTIVATING, wire::RECOVERED);
        auto route = groups(store, "logs", 0, 1).pgs(0);
        check(!route.recovering() && route.epoch() == job_id, "completed route not published");
        bool spare = false;
        for (const auto& node : route.replicas()) { check(node.server_id() != "s1", "failed node kept in route"); spare |= node.server_id() == "s4"; }
        check(spare, "replacement missing");
        wire::ServerHeartbeatRequest heartbeat; store.retirement_directives("s1", heartbeat);
        check(heartbeat.retired_pgs_size() == 1 && heartbeat.retired_pgs(0).epoch() == job_id, "returning node not retired");
        healthy(store, "s1"); check(jobs(store).failures().empty(), "healthy probe did not reset failure timer");
    }
    master::Store restarted(root.path);
    check(groups(restarted, "logs", 0, 1).pgs(0).epoch() == job_id && jobs(restarted).recoveries(0).phase() == wire::RECOVERED, "completed route not durable");
}
void eligibility_and_multiple_failures() {
    Directory root; master::Store store(root.path); configure(store, false);
    const auto since = fail(store, "s1");
    store.plan_recoveries(since + 600000); check(jobs(store).recoveries().empty(), "scheduled without spare");
    node(store, 4, "wrong-dc"); healthy(store, "s4");
    store.plan_recoveries(since + 600000); check(jobs(store).recoveries().empty(), "violated single-DC policy");
    node(store, 5, "a"); healthy(store, "s5", false);
    store.plan_recoveries(since + 600000); check(jobs(store).recoveries().empty(), "used incapable spare");
    healthy(store, "s5"); const auto second = fail(store, "s2");
    store.plan_recoveries(second + 600000); check(jobs(store).recoveries().empty(), "scheduled without enough spares");
    node(store, 6, "a"); healthy(store, "s6");
    store.plan_recoveries(second + 600000);
    check(jobs(store).recoveries_size() == 1 && jobs(store).recoveries(0).failed_servers_size() == 2, "multiple missing replicas not scheduled together");
}
void healthy_resets_and_no_source() {
    Directory root; master::Store store(root.path); configure(store);
    const auto since = fail(store, "s1"); healthy(store, "s1");
    store.plan_recoveries(since + 600000); check(jobs(store).recoveries().empty(), "recovered node still scheduled");
    fail(store, "s1"); fail(store, "s2"); const auto last = fail(store, "s3");
    store.plan_recoveries(last + 600000); check(jobs(store).recoveries().empty(), "scheduled with no data source");
}
void reject_inconsistent_metadata() {
    Directory root; master::Store store(root.path); configure(store);
    const auto since = fail(store, "s1"); store.plan_recoveries(since + 600000);
    std::ifstream input(root.path / "spaces.pb", std::ios::binary);
    const std::string bytes{std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
    check(bytes.substr(0, 8) == "FSPACE04", "recovery uses downgrade-compatible snapshot format");
    wire::SpaceCatalog original;
    check(original.ParseFromString(bytes.substr(16)), "snapshot parse");
    master::placement::validate_catalog(original);
    auto changed = original; changed.clear_recoveries();
    unit::expect<master::Error>([&] { master::placement::validate_catalog(changed); });
    changed = original; changed.mutable_recoveries(0)->mutable_new_pg()->clear_replicas();
    unit::expect<master::Error>([&] { master::placement::validate_catalog(changed); });
    changed = original; changed.mutable_recoveries(0)->set_cursor(chunkstore::ChunkId{1, 0, 0}.str());
    unit::expect<master::Error>([&] { master::placement::validate_catalog(changed); });
    changed = original; changed.mutable_spaces(0)->mutable_overrides(0)->set_epoch(1);
    unit::expect<master::Error>([&] { master::placement::validate_catalog(changed); });
    wire::BindPGsRequest unbind; unbind.set_name("logs"); unbind.set_start(0); unbind.set_count(1); wire::SpaceResponse response;
    check(unit::expect<master::Error>([&] { store.unbind_pgs(unbind, response); }).code() == wire::CONFLICT,
          "recovery PG allowed to be rebound with epoch zero");
}
}
int main() { return unit::run({{"ten-minute grace, restart and publication", grace_restart_and_publication},
                              {"placement and multiple failures", eligibility_and_multiple_failures}, {"healthy reset and no source", healthy_resets_and_no_source},
                              {"reject inconsistent recovery metadata", reject_inconsistent_metadata}}); }
