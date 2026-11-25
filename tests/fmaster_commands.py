"""Exercise FMaster admin views and controls against real bRPC processes."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json
import signal
import subprocess
import tempfile
import time

from fmaster import BUILD, Process, endpoint


def verify(root):
    master_address, first_address, second_address, offline_address = [endpoint() for _ in range(4)]
    master_data = root / "master"
    first_disk, another_disk, second_disk = root / "first", root / "another", root / "second"
    for path in (master_data, first_disk, another_disk, second_disk):
        path.mkdir()
    missing = root / "missing"
    config = root / "cluster.pbtxt"
    config.write_text(f'''servers {{
  id: "s1" endpoint: {json.dumps(first_address)}
  disks {{ id: "d1" path: {json.dumps(str(first_disk))} }}
  disks {{ id: "d2" path: {json.dumps(str(another_disk))} }}
}}
servers {{
  id: "s2" endpoint: {json.dumps(second_address)}
  disks {{ id: "bad" path: {json.dumps(str(missing))} }}
}}
servers {{
  id: "offline" endpoint: {json.dumps(offline_address)}
  disks {{ id: "unknown" path: {json.dumps(str(root / "offline-disk"))} }}
}}
''')
    master = Process([BUILD / "fmaster", f"--data-dir={master_data}", f"--listen={master_address}",
                      f"--config={config}", "--heartbeat-interval-ms=5000", "--heartbeat-timeout-ms=10000",
                      "--probe-timeout-ms=300"], master_address, root / "master.log")
    first = Process([BUILD / "chunk_server", first_disk, first_address, "--server-id=s1"],
                    first_address, root / "first.log")
    second = Process([BUILD / "chunk_server", second_disk, second_address, "--server-id=s2"],
                     second_address, root / "second.log")

    def run(*args):
        return subprocess.run([BUILD / "fmaster_cli", master_address, *map(str, args)],
                              capture_output=True, timeout=8)

    def cli(*args, success=True):
        result = run(*args)
        assert (result.returncode == 0) == success, (args, result.stdout, result.stderr)
        return result.stdout

    def view(*args):
        return json.loads(cli(*args))

    def snapshot():
        return view("list")

    def configured(response):
        return {server["id"]: server for server in response["topology"]["servers"]}

    def statuses(response):
        return {server["server_id"]: server for server in response["servers"]}

    def revision():
        return int(snapshot()["topology"]["revision"])

    def wait_for(predicate, seconds=8):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            result = predicate()
            if result:
                return result
            time.sleep(.03)
        raise AssertionError("admin state did not converge: " + json.dumps(snapshot()))

    def observed(server):
        return int(statuses(snapshot())[server]["last_heartbeat_unix_ms"])

    def probe(server, count=1):
        before = observed(server)
        response = view("probe", server)
        assert int(response["scheduled_servers"]) == count
        wait_for(lambda: observed(server) > before, seconds=2)

    try:
        first.start()
        second.start()
        master.start()
        wait_for(lambda: statuses(snapshot())["s1"]["online"] and statuses(snapshot())["s2"]["online"])
        initial = snapshot()
        assert set(configured(initial)) == {"s1", "s2", "offline"}
        assert not statuses(initial)["offline"]["online"]
        summary = view("status", "--json")
        assert summary["revision"] == initial["topology"]["revision"]
        assert summary["servers"] == {"total": 3, "online": 2, "offline": 1, "enabled": 3, "disabled": 0}
        assert summary["disks"] == {"total": 4, "enabled": 4, "disabled": 0,
                                    "unknown": 1, "healthy": 2, "error": 1, "stale": 0}
        assert set(configured(view("servers", "--online", "--json"))) == {"s1", "s2"}
        assert set(configured(view("servers", "--json", "--offline"))) == {"offline"}
        assert set(configured(view("server", "s1", "--json"))) == {"s1"}
        assert set(statuses(view("server", "s1", "--json"))) == {"s1"}
        disks = configured(view("disks", "s1", "--json"))["s1"]["disks"]
        assert {disk["id"] for disk in disks} == {"d1", "d2"}
        detail = view("disk", "s1", "d2", "--json")
        assert [disk["id"] for disk in configured(detail)["s1"]["disks"]] == ["d2"]
        assert [disk["id"] for disk in statuses(detail)["s1"]["disks"]] == ["d2"]
        healthy = view("disks", "--health=healthy", "--json")
        assert set(configured(healthy)) == {"s1"}
        assert len(configured(healthy)["s1"]["disks"]) == 2
        unhealthy = view("errors", "--json")
        assert set(configured(unhealthy)) == {"s2", "offline"}
        assert statuses(unhealthy)["s2"]["disks"][0]["health"] == "DISK_ERROR"
        assert configured(view("errors", "s1", "--json")) == {}
        assert statuses(view("errors", "s1", "--json")) == {}
        help_result = subprocess.run([BUILD / "fmaster_cli", "--help"], capture_output=True, timeout=3)
        assert help_result.returncode == 0
        for command_name in (b"server-enable", b"disk-disable", b"capacity", b"validate", b"probe", b"watch"):
            assert command_name in help_result.stdout
        assert b"REVISION" in cli("status") and b"DISKS" in cli("status")
        server_table = cli("server", "s1")
        assert b"ENDPOINT" in server_table and first_address.encode() in server_table
        disk_table = cli("disk", "s1", "d1")
        assert b"HEALTH" in disk_table and str(first_disk).encode() in disk_table
        assert b"OBSERVED_MS" in cli("capacity", "s1")
        capacity = view("capacity", "--json")
        assert "total_bytes" not in capacity and len(capacity["disks"]) == 4
        first_capacity = [disk for disk in capacity["disks"] if disk["server_id"] == "s1"]
        assert len(first_capacity) == 2
        # Both directories use the same filesystem. Each retains its own observation.
        assert first_capacity[0]["total_bytes"] == first_capacity[1]["total_bytes"]
        for disk in first_capacity:
            assert int(disk["total_bytes"]) == int(disk["used_bytes"]) + int(disk["free_bytes"])
            assert 0 <= int(disk["available_bytes"]) <= int(disk["free_bytes"])
            assert int(disk["observed_unix_ms"]) > 0
        unknown = next(disk for disk in capacity["disks"] if disk["server_id"] == "offline")
        assert unknown["health"] == "unknown" and unknown["total_bytes"] is None
        assert int(unknown["observed_unix_ms"]) == 0
        failed_capacity = next(disk for disk in capacity["disks"] if disk["server_id"] == "s2")
        assert failed_capacity["health"] == "error" and failed_capacity["total_bytes"] is None
        assert int(failed_capacity["observed_unix_ms"]) > 0
        assert len(view("capacity", "s1", "--json")["disks"]) == 2
        for args in (("server", "missing"), ("disks", "missing"), ("disk", "s1", "missing"),
                     ("capacity", "missing"), ("errors", "missing"), ("probe", "missing")):
            if args[0] == "probe":
                cli(*args, success=False)
            else:
                cli(*args, "--json", success=False)

        # Export/import is a no-op, and validation cannot change live configuration.
        exported = root / "exported.pbtxt"
        exported.write_bytes(cli("config"))
        before = snapshot()["topology"]
        valid = view("validate", exported)
        assert int(valid["topology"]["revision"]) == 0
        assert int(cli("apply", exported)) == int(before["revision"])
        assert snapshot()["topology"] == before
        normalized = root / "normalized.pbtxt"
        normalized.write_text(f'''revision: 999
servers {{ id: "v" endpoint: "127.0.0.1:1"
  disks {{ id: "d" path: {json.dumps(str(root / "x" / ".." / "first"))} }}
}}
''')
        validation = view("validate", normalized)
        assert int(validation["topology"]["revision"]) == 0
        assert validation["topology"]["servers"][0]["disks"][0]["path"] == str(first_disk)
        normalized.write_text('servers { id: "v" endpoint: "127.0.0.1:1" }\n'
                              'servers { id: "v" endpoint: "127.0.0.1:2" }\n')
        cli("validate", normalized, success=False)
        assert snapshot()["topology"] == before

        # State-only changes preserve endpoint, paths and the other disks.
        original = configured(snapshot())["s1"]
        expected = revision()
        changed = int(cli("server-disable", "s1", f"--expected-revision={expected}"))
        assert changed == expected + 1
        disabled = configured(view("server", "s1", "--json"))["s1"]
        assert disabled["disabled"] and disabled["endpoint"] == original["endpoint"]
        assert disabled["disks"] == original["disks"]
        assert set(configured(view("servers", "--disabled", "--json"))) == {"s1"}
        assert view("status", "--json")["servers"]["disabled"] == 1
        assert int(cli("server-disable", "s1", f"--expected-revision={changed}")) == changed
        cli("server-enable", "s1", f"--expected-revision={expected}", success=False)
        cli("server-enable", "s1", f"--expected-revision={changed}")
        expected = revision()
        changed = int(cli("disk-disable", "s1", "d1", f"--expected-revision={expected}"))
        disks = {disk["id"]: disk for disk in configured(snapshot())["s1"]["disks"]}
        assert disks["d1"]["disabled"] and disks["d1"]["path"] == str(first_disk)
        assert not disks["d2"]["disabled"] and disks["d2"]["path"] == str(another_disk)
        assert view("status", "--json")["disks"]["disabled"] == 1
        assert int(cli("disk-disable", "s1", "d1", f"--expected-revision={changed}")) == changed
        cli("disk-enable", "s1", "d1", f"--expected-revision={expected}", success=False)
        cli("disk-enable", "s1", "d1", f"--expected-revision={changed}")
        cli("server-enable", "missing", success=False)
        cli("disk-enable", "s1", "missing", success=False)
        expected = revision()
        with ThreadPoolExecutor(max_workers=2) as pool:
            calls = [pool.submit(run, "server-disable", server, f"--expected-revision={expected}")
                     for server in ("s1", "s2")]
            results = [call.result() for call in calls]
        assert sum(result.returncode == 0 for result in results) == 1
        assert revision() == expected + 1
        cli("server-enable", "s1")
        cli("server-enable", "s2")

        # A targeted probe must refresh within 2 s while the periodic interval is 5 s.
        probe("s1")
        probe("s1")
        assert int(view("probe")["scheduled_servers"]) == 3
        wait_for(lambda: statuses(snapshot())["s2"]["online"])
        samples = cli("watch", "--json", "--interval-ms=20", "--count=2").decode().splitlines()
        assert len(samples) == 2
        assert all(json.loads(sample)["servers"]["total"] == 3 for sample in samples)

        # A recent failed probe is visible while the previous success still keeps the node online.
        probe("s1")
        previous_probe = int(statuses(snapshot())["s1"]["last_probe_unix_ms"])
        first.process.send_signal(signal.SIGSTOP)
        try:
            assert int(view("probe", "s1")["scheduled_servers"]) == 1
            failed = wait_for(lambda: (state := statuses(snapshot())["s1"]) and state["error"]
                              and int(state["last_probe_unix_ms"]) > previous_probe and state, seconds=2)
            assert failed["online"] and all(disk["health"] == "DISK_HEALTHY" for disk in failed["disks"])
            errors = view("errors", "s1", "--json")
            assert set(configured(errors)) == {"s1"}
            error_state = statuses(errors)["s1"]
            assert error_state["online"] and error_state["error"]
        finally:
            first.process.send_signal(signal.SIGCONT)
        assert int(view("probe", "s1")["scheduled_servers"]) == 1
        wait_for(lambda: not statuses(snapshot())["s1"]["error"], seconds=2)
        for args in (("watch", "--count=0"), ("watch", "--interval-ms=0"),
                     ("servers", "--online", "--offline"), ("servers", "--online", "--online"),
                     ("servers", "--enabled", "--disabled"), ("disks", "--health=bad"),
                     ("status", "extra"), ("disk", "s1"), ("server-disable",),
                     ("server-enable", "s1", "--expected-revision=bad"), ("validate", "missing.pbtxt")):
            cli(*args, success=False)
    finally:
        master.stop()
        second.stop()
        first.stop()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as directory:
        verify(Path(directory))
    print("FMaster admin views, state controls, validation, export, immediate probes and watch passed")
