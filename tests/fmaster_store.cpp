#include "fmaster_store.hpp"

#include <butil/crc32c.h>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <thread>

namespace master = chunkstore::master;
namespace wire = fstore::master::v1;
using namespace std::chrono_literals;

namespace {
void check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

template<class Operation>
void expect_error(wire::Code code, Operation operation) {
    try {
        operation();
    } catch (const master::Error& error) {
        check(error.code() == code, "wrong master error code");
        return;
    }
    throw std::runtime_error("expected master error was not returned");
}

wire::ServerStatus status(master::Store& store, const std::string& id = "s1") {
    wire::ListResponse listed;
    store.list(listed);
    for (const auto& server : listed.servers()) {
        if (server.server_id() == id) return server;
    }
    throw std::runtime_error("configured server missing from list");
}

void check_unknown(const wire::ServerStatus& server) {
    check(!server.online() && server.last_heartbeat_unix_ms() == 0, "unobserved server appears online");
    check(server.disks_size() == 2, "unobserved disks were omitted");
    for (const auto& disk : server.disks()) {
        check(disk.health() == wire::DISK_UNKNOWN && !disk.healthy(), "unobserved disk was assigned health");
        check(disk.total_bytes() == 0 && disk.free_bytes() == 0 && disk.available_bytes() == 0 &&
              disk.observed_unix_ms() == 0, "unobserved disk retained old observations");
    }
}

void verify(const std::filesystem::path& root) {
    master::Store store(root, {}, 200ms, 20ms);
    wire::SetServerRequest server;
    server.set_id("s1");
    server.set_endpoint("127.0.0.1:8011");
    server.set_disabled(true);
    store.set_server(server);
    wire::SetDiskRequest disk;
    disk.set_server_id("s1");
    disk.mutable_disk()->set_id("d1");
    disk.mutable_disk()->set_path("/data/one");
    disk.mutable_disk()->set_disabled(true);
    store.set_disk(disk);
    disk.mutable_disk()->set_id("d2");
    disk.mutable_disk()->set_path("/data/two");
    const auto configured = store.set_disk(disk);
    check_unknown(status(store));

    store.record_probe_failure(server.id(), server.endpoint(), configured, "connection refused");
    auto listed = status(store);
    check_unknown(listed);
    check(listed.last_probe_unix_ms() > 0 && listed.error() == "connection refused", "initial probe failure lost");

    wire::HeartbeatReport heartbeat;
    heartbeat.set_server_id(server.id());
    heartbeat.set_endpoint(server.endpoint());
    heartbeat.set_config_revision(configured);
    auto* first = heartbeat.add_disks();
    first->set_id("d1");
    first->set_path("/data/one");
    first->set_healthy(true);
    first->set_health(wire::DISK_STALE);
    first->set_error("old error");
    first->set_observed_unix_ms(1);
    first->set_total_bytes(1000);
    first->set_free_bytes(500);
    first->set_available_bytes(400);
    auto* second = heartbeat.add_disks();
    second->set_id("d2");
    second->set_path("/data/two");
    second->set_healthy(false);
    second->set_error("disk I/O error");
    store.record_heartbeat(heartbeat);
    listed = status(store);
    check(listed.online() && listed.error().empty(), "successful probe did not recover disabled node");
    check(listed.last_probe_unix_ms() == listed.last_heartbeat_unix_ms(), "success timestamps differ");
    check(listed.disks(0).health() == wire::DISK_HEALTHY && listed.disks(0).healthy() &&
          listed.disks(0).error().empty(), "healthy disk state was not normalized");
    check(listed.disks(1).health() == wire::DISK_ERROR && !listed.disks(1).healthy(), "disk error hidden");
    check(listed.disks(0).observed_unix_ms() == listed.last_heartbeat_unix_ms(), "remote observation time was trusted");

    wire::SetServerRequest unrelated;
    unrelated.set_id("s2");
    unrelated.set_endpoint("127.0.0.1:8012");
    store.set_server(unrelated);
    // Unrelated configuration changes must not invalidate this node's probe.
    first->set_free_bytes(450);
    store.record_heartbeat(heartbeat);
    listed = status(store);
    check(listed.disks(0).free_bytes() == 450, "unrelated change discarded disk report");
    const auto observed = listed.disks(0).observed_unix_ms();
    const auto last_success = listed.last_heartbeat_unix_ms();

    auto invalid = heartbeat;
    invalid.mutable_disks()->RemoveLast();
    expect_error(wire::INVALID_ARGUMENT, [&] { store.record_heartbeat(invalid); });
    invalid = heartbeat;
    *invalid.mutable_disks(1) = invalid.disks(0);
    expect_error(wire::INVALID_ARGUMENT, [&] { store.record_heartbeat(invalid); });
    invalid = heartbeat;
    invalid.mutable_disks(0)->set_available_bytes(1001);
    expect_error(wire::INVALID_ARGUMENT, [&] { store.record_heartbeat(invalid); });
    invalid = heartbeat;
    invalid.mutable_disks(1)->set_path("/wrong/path");
    expect_error(wire::INVALID_ARGUMENT, [&] { store.record_heartbeat(invalid); });
    listed = status(store);
    check(listed.last_heartbeat_unix_ms() == last_success && listed.disks_size() == 2 &&
          listed.disks(0).observed_unix_ms() == observed, "invalid response changed observations");

    store.record_probe_failure(server.id(), server.endpoint(), configured, "RPC timeout");
    listed = status(store);
    check(listed.online(), "failed probe expired last successful heartbeat early");
    check(listed.last_heartbeat_unix_ms() == last_success && listed.last_probe_unix_ms() >= last_success &&
          listed.error() == "RPC timeout", "failed probe updated last success or lost error");
    check(listed.disks(0).health() == wire::DISK_HEALTHY, "fresh observations became stale on first failure");
    std::this_thread::sleep_for(250ms);
    listed = status(store);
    check(!listed.online(), "expired successful heartbeat kept node online");
    for (const auto& report : listed.disks()) {
        check(report.health() == wire::DISK_STALE && !report.healthy(), "expired observation kept old health");
        check(report.observed_unix_ms() == observed, "staleness changed observation time");
    }
    check(listed.disks(0).total_bytes() == 1000 && listed.disks(0).free_bytes() == 450,
          "staleness discarded last observed capacity");
    store.record_heartbeat(heartbeat);
    check(status(store).online() && status(store).error().empty(), "node did not recover after valid probe");

    disk.mutable_disk()->set_id("d1");
    disk.mutable_disk()->set_path("/data/replaced");
    const auto changed = store.set_disk(disk);
    expect_error(wire::CONFLICT, [&] { store.record_heartbeat(heartbeat); });
    expect_error(wire::CONFLICT, [&] {
        store.record_probe_failure(server.id(), server.endpoint(), configured, "old failure");
    });
    listed = status(store);
    check_unknown(listed);
    check(listed.last_probe_unix_ms() == 0 && listed.error().empty(), "stale probe updated new configuration");
    heartbeat.set_config_revision(changed);
    expect_error(wire::INVALID_ARGUMENT, [&] { store.record_heartbeat(heartbeat); });
    first->set_path("/data/replaced");
    store.record_heartbeat(heartbeat);
    check(status(store).disks(0).path() == "/data/replaced", "fresh disk report was rejected");

    server.set_endpoint("127.0.0.1:8013");
    const auto moved = store.set_server(server);
    expect_error(wire::CONFLICT, [&] { store.record_heartbeat(heartbeat); });
    heartbeat.set_config_revision(moved);
    expect_error(wire::CONFLICT, [&] { store.record_heartbeat(heartbeat); });
    expect_error(wire::CONFLICT, [&] {
        store.record_probe_failure(server.id(), "127.0.0.1:8011", moved, "wrong address");
    });
    check_unknown(status(store));
    heartbeat.set_endpoint(server.endpoint());
    store.record_heartbeat(heartbeat);
    const auto before_delete = heartbeat;
    wire::RemoveServerRequest remove;
    remove.set_id(server.id());
    store.remove_server(remove);
    expect_error(wire::NOT_FOUND, [&] { store.record_heartbeat(before_delete); });
    expect_error(wire::NOT_FOUND, [&] {
        store.record_probe_failure(server.id(), server.endpoint(), moved, "deleted node failure");
    });
    store.set_server(server);
    store.set_disk(disk);
    disk.mutable_disk()->set_id("d2");
    disk.mutable_disk()->set_path("/data/two");
    const auto recreated = store.set_disk(disk);
    expect_error(wire::CONFLICT, [&] { store.record_heartbeat(before_delete); });
    expect_error(wire::CONFLICT, [&] {
        store.record_probe_failure(server.id(), server.endpoint(), moved, "previous generation failure");
    });
    check_unknown(status(store));
    heartbeat.set_config_revision(recreated);
    store.record_heartbeat(heartbeat);
    check(status(store).online(), "new node generation did not accept current probe");
}

void verify_controls(const std::filesystem::path& root) {
    master::Store store(root);
    wire::SetServerRequest server;
    server.set_id("s1");
    server.set_endpoint("127.0.0.1:8111");
    store.set_server(server);
    server.set_id("s2");
    server.set_endpoint("127.0.0.1:8112");
    store.set_server(server);
    wire::SetDiskRequest disk;
    disk.set_server_id("s1");
    disk.mutable_disk()->set_id("d1");
    disk.mutable_disk()->set_path("/mnt/first");
    store.set_disk(disk);
    disk.mutable_disk()->set_id("d2");
    disk.mutable_disk()->set_path("/mnt/second");
    store.set_disk(disk);
    wire::ListResponse before, after;
    store.list(before);
    auto expected = before.topology();
    const auto initial_revision = expected.revision();

    wire::SetServerStateRequest server_state;
    server_state.set_id("s1");
    server_state.set_disabled(true);
    server_state.set_expected_revision(initial_revision);
    const auto server_revision = store.set_server_state(server_state);
    check(server_revision == initial_revision + 1, "server state mutation did not advance revision");
    expected.set_revision(server_revision);
    expected.mutable_servers(0)->set_disabled(true);
    store.list(after);
    check(after.topology().SerializeAsString() == expected.SerializeAsString(), "server state mutation changed other configuration");
    expect_error(wire::CONFLICT, [&] { store.set_server_state(server_state); });
    server_state.set_expected_revision(server_revision);
    check(store.set_server_state(server_state) == server_revision, "server state no-op changed revision");
    server_state.set_id("missing");
    expect_error(wire::NOT_FOUND, [&] { store.set_server_state(server_state); });

    wire::SetDiskStateRequest disk_state;
    disk_state.set_server_id("s1");
    disk_state.set_disk_id("d1");
    disk_state.set_disabled(true);
    disk_state.set_expected_revision(server_revision);
    const auto disk_revision = store.set_disk_state(disk_state);
    check(disk_revision == server_revision + 1, "disk state mutation did not advance revision");
    expected.set_revision(disk_revision);
    expected.mutable_servers(0)->mutable_disks(0)->set_disabled(true);
    store.list(after);
    check(after.topology().SerializeAsString() == expected.SerializeAsString(), "disk state mutation changed path or other configuration");
    expect_error(wire::CONFLICT, [&] { store.set_disk_state(disk_state); });
    disk_state.set_expected_revision(disk_revision);
    check(store.set_disk_state(disk_state) == disk_revision, "disk state no-op changed revision");
    disk_state.set_disk_id("missing");
    expect_error(wire::NOT_FOUND, [&] { store.set_disk_state(disk_state); });
    disk_state.set_server_id("missing");
    expect_error(wire::NOT_FOUND, [&] { store.set_disk_state(disk_state); });
    wire::SetServerRequest alias;
    alias.set_id("alias");
    alias.set_endpoint("localhost:8111");
    expect_error(wire::INVALID_ARGUMENT, [&] { store.set_server(alias); });

    wire::ListResponse selected;
    store.list(selected, "s1");
    check(selected.topology().revision() == disk_revision && selected.topology().servers_size() == 1 &&
          selected.servers_size() == 1, "filtered list has wrong revision or includes other servers");
    check(selected.topology().servers(0).SerializeAsString() == expected.servers(0).SerializeAsString() &&
          selected.servers(0).server_id() == "s1" && selected.servers(0).disks_size() == 2,
          "filtered list omitted selected configuration or disk state");
    expect_error(wire::NOT_FOUND, [&] { store.list(selected, "missing"); });
    store.list(selected);
    check(selected.topology().servers_size() == 2 && selected.servers_size() == 2,
          "unfiltered list changed after filtered request");

    std::ifstream persisted(root / "topology.pb", std::ios::binary);
    const std::string original_bytes((std::istreambuf_iterator<char>(persisted)), {});
    wire::ValidateConfigRequest validation;
    validation.mutable_topology()->set_revision(9000);
    auto* z = validation.mutable_topology()->add_servers();
    z->set_id("z");
    z->set_endpoint("127.0.0.1:8122");
    auto* candidate_disk = z->add_disks();
    candidate_disk->set_id("disk");
    candidate_disk->set_path("/data/../mnt//candidate/");
    auto* a = validation.mutable_topology()->add_servers();
    a->set_id("a");
    a->set_endpoint("127.0.0.1:8121");
    wire::ValidateConfigResponse validated;
    store.validate_config(validation, validated);
    check(validated.has_status() && validated.status().code() == wire::OK && validated.topology().revision() == 0,
          "validation did not return an uncommitted normalized configuration");
    check(validated.topology().servers(0).id() == "a" &&
          validated.topology().servers(1).disks(0).path() == "/mnt/candidate", "validation skipped normalization");
    auto normalized = validated.topology();
    auto invalid = validation;
    *invalid.mutable_topology()->add_servers() = invalid.topology().servers(0);
    expect_error(wire::INVALID_ARGUMENT, [&] { store.validate_config(invalid, validated); });
    wire::ValidateConfigRequest absent;
    expect_error(wire::INVALID_ARGUMENT, [&] { store.validate_config(absent, validated); });
    store.list(after);
    check(after.topology().SerializeAsString() == expected.SerializeAsString(), "validation mutated live configuration");
    std::ifstream current(root / "topology.pb", std::ios::binary);
    const std::string current_bytes((std::istreambuf_iterator<char>(current)), {});
    check(current_bytes == original_bytes, "validation persisted a candidate configuration");
    wire::ApplyConfigRequest apply;
    *apply.mutable_topology() = validation.topology();
    apply.set_expected_revision(disk_revision);
    const auto applied = store.apply_config(apply);
    store.list(after);
    normalized.set_revision(applied);
    check(after.topology().SerializeAsString() == normalized.SerializeAsString(),
          "validation normalization differs from apply normalization");
}

void verify_unavailable_hostname(const std::filesystem::path& root) {
    // A persisted hostname can become unavailable while metadata remains valid.
    // Use a reserved domain so this fixture never relies on a live DNS record.
    wire::Topology saved;
    saved.set_revision(7);
    auto* node = saved.add_servers();
    node->set_id("offline");
    node->set_endpoint("fstore-resolver-test.invalid:8111");
    node->set_disabled(true);
    auto* disk = node->add_disks();
    disk->set_id("disk");
    disk->set_path("/mnt/offline");
    auto* online = saved.add_servers();
    online->set_id("online");
    online->set_endpoint("127.0.0.1:8112");
    const auto payload = saved.SerializeAsString();
    std::string snapshot = "FMASTER1";
    for (const auto value : {static_cast<std::uint32_t>(payload.size()), butil::crc32c::Value(payload.data(), payload.size())}) {
        for (int shift = 24; shift >= 0; shift -= 8) snapshot.push_back(static_cast<char>(value >> shift));
    }
    snapshot += payload;
    std::filesystem::create_directories(root);
    {
        std::ofstream output(root / "topology.pb", std::ios::binary);
        output.write(snapshot.data(), snapshot.size());
        check(bool(output), "could not write saved hostname fixture");
    }
    master::Store store(root);
    wire::SetServerStateRequest server_state;
    server_state.set_id("online");
    server_state.set_disabled(true);
    check(store.set_server_state(server_state) == 8, "state update required hostname resolution");
    check(store.set_server_state(server_state) == 8, "hostname state no-op changed revision");
    wire::SetDiskStateRequest disk_state;
    disk_state.set_server_id("offline");
    disk_state.set_disk_id("disk");
    disk_state.set_disabled(true);
    check(store.set_disk_state(disk_state) == 9, "disk state update required hostname resolution");
    wire::SetDiskRequest set_disk;
    set_disk.set_server_id("offline");
    set_disk.mutable_disk()->set_id("disk");
    set_disk.mutable_disk()->set_path("/mnt/replaced");
    check(store.set_disk(set_disk) == 10, "disk path update required hostname resolution");
    wire::ListResponse listed;
    store.list(listed);
    wire::ApplyConfigRequest apply;
    *apply.mutable_topology() = listed.topology();
    apply.mutable_topology()->mutable_servers(0)->set_disabled(false);
    check(store.apply_config(apply) == 11, "unchanged endpoint apply required hostname resolution");
    wire::RemoveDiskRequest remove_disk;
    remove_disk.set_server_id("offline");
    remove_disk.set_disk_id("disk");
    check(store.remove_disk(remove_disk) == 12, "disk removal required hostname resolution");
    wire::RemoveServerRequest remove_server;
    remove_server.set_id("offline");
    check(store.remove_server(remove_server) == 13, "node removal required hostname resolution");
}

}

int main() {
    char directory[] = "/tmp/fstore-master-state-XXXXXX";
    if (!::mkdtemp(directory)) return 1;
    try {
        verify(directory);
        verify_controls(std::filesystem::path(directory) / "controls");
        verify_unavailable_hostname(std::filesystem::path(directory) / "hostname");
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        std::filesystem::remove_all(directory);
        return 1;
    }
    std::filesystem::remove_all(directory);
}
