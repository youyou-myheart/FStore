"""Exercise FMaster configuration, persistence and active FServer probes over bRPC."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

BUILD = Path(sys.argv[1]).resolve()


def endpoint():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{listener.getsockname()[1]}"


class Process:
    def __init__(self, command, address, log):
        self.command, self.address, self.log = command, address, log
        self.process = None

    def start(self):
        with self.log.open("ab") as output:
            self.process = subprocess.Popen(self.command, stdout=output, stderr=output)
        host, port = self.address.rsplit(":", 1)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            assert self.process.poll() is None, self.log.read_text(errors="replace")
            try:
                with socket.create_connection((host, int(port)), timeout=.1):
                    return
            except OSError:
                time.sleep(.02)
        raise RuntimeError("service did not start: " + self.log.read_text(errors="replace"))

    def stop(self, kill=False):
        if self.process is not None and self.process.poll() is None:
            self.process.send_signal(signal.SIGKILL if kill else signal.SIGINT)
            try:
                self.process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
                raise


def seed(path, server_endpoint, second_endpoint, disk, second_disk):
    path.write_text(f'''servers {{
  id: "s1"
  endpoint: {json.dumps(server_endpoint)}
  disks {{ id: "d1" path: {json.dumps(str(disk))} }}
}}
servers {{
  id: "s2"
  endpoint: {json.dumps(second_endpoint)}
  disks {{ id: "d1" path: {json.dumps(str(second_disk))} }}
}}
''')


def verify(root):
    master_endpoint, server_endpoint, second_endpoint = endpoint(), endpoint(), endpoint()
    data, disk, second_disk = root / "master", root / "disk1", root / "second-server-disk"
    data.mkdir()
    disk.mkdir()
    second_disk.mkdir()
    config = root / "cluster.pbtxt"
    seed(config, server_endpoint, second_endpoint, disk, second_disk)
    master = Process([BUILD / "fmaster", f"--data-dir={data}", f"--listen={master_endpoint}",
                      f"--config={config}", "--heartbeat-timeout-ms=1000", "--heartbeat-interval-ms=100",
                      "--probe-timeout-ms=1500"],
                     master_endpoint, root / "master.log")
    server = Process([BUILD / "chunk_server", disk, server_endpoint, "--server-id=s1"],
                     server_endpoint, root / "server.log")
    second = Process([BUILD / "chunk_server", second_disk, second_endpoint, "--server-id=s2"],
                     second_endpoint, root / "second-server.log")

    def command(*args):
        return subprocess.run([BUILD / "fmaster_cli", master_endpoint, *map(str, args)],
                              capture_output=True, timeout=8)

    def cli(*args, success=True):
        result = command(*args)
        assert (result.returncode == 0) == success, (args, result.stdout, result.stderr)
        return result.stdout

    def snapshot():
        return json.loads(cli("list"))

    def config_servers():
        return {entry["id"]: entry for entry in snapshot()["topology"]["servers"]}

    def reports():
        return {entry["server_id"]: entry for entry in snapshot()["servers"]}

    def revision():
        return int(snapshot()["topology"]["revision"])

    def wait_for(predicate):
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(.05)
        raise AssertionError("state did not converge: " + json.dumps(snapshot()))

    def disk_report(name):
        return next((entry for entry in reports().get("s1", {}).get("disks", [])
                     if entry["id"] == name), None)

    try:
        master.start()
        initial = snapshot()
        assert set(config_servers()) == {"s1", "s2"}
        assert all(not state["online"] for state in initial["servers"])
        for state in initial["servers"]:
            assert len(state["disks"]) == 1
            unknown = state["disks"][0]
            assert unknown["health"] == "DISK_UNKNOWN" and not unknown["healthy"]
            assert int(unknown["observed_unix_ms"]) == 0 and int(unknown["total_bytes"]) == 0
        assert config_servers()["s1"]["disks"][0]["path"] == str(disk)
        first_revision = revision()
        assert first_revision > 0
        assert int(cli("server-set", "s1", server_endpoint)) == first_revision

        # Validation rejects the complete mutation and preserves the revision.
        cli("server-set", "duplicate", server_endpoint, success=False)
        cli("disk-set", "s1", "relative", "relative/path", success=False)
        cli("disk-set", "s1", "duplicate", disk / ".", success=False)
        cli("disk-set", "missing", "d1", disk, success=False)
        assert snapshot()["topology"] == initial["topology"]
        invalid = root / "invalid.pbtxt"
        invalid.write_text('servers { id: "x" endpoint: "127.0.0.1:1" }\n'
                           'servers { id: "x" endpoint: "127.0.0.1:2" }\n')
        cli("apply", invalid, success=False)
        assert revision() == first_revision

        cli("server-set", "s1", server_endpoint, "disabled")
        assert config_servers()["s1"]["disabled"]
        assert len(config_servers()["s1"]["disks"]) == 1
        cli("server-set", "s1", server_endpoint, "enabled")
        cli("disk-set", "s1", "d1", disk, "disabled")
        assert config_servers()["s1"]["disks"][0]["disabled"]
        cli("disk-set", "s1", "d1", disk, "enabled")

        # Exactly one concurrent update can consume a configuration revision.
        expected = revision()
        with ThreadPoolExecutor(max_workers=2) as pool:
            calls = [pool.submit(command, "server-set", name, endpoint(),
                                 f"--expected-revision={expected}") for name in ("s3", "s4")]
            results = [call.result() for call in calls]
        assert sum(result.returncode == 0 for result in results) == 1
        assert revision() == expected + 1
        cli("server-remove", "s1", f"--expected-revision={expected}", success=False)
        cli("apply", config, f"--expected-revision={revision()}")
        assert set(config_servers()) == {"s1", "s2"}

        # A second master cannot take ownership of the same metadata directory.
        duplicate = subprocess.run([BUILD / "fmaster", f"--data-dir={data}", f"--listen={endpoint()}"],
                                   capture_output=True, timeout=5)
        assert duplicate.returncode != 0, duplicate.stdout

        server.start()
        wait_for(lambda: reports()["s1"]["online"])
        healthy = wait_for(lambda: (report := disk_report("d1")) and report["healthy"] and report)
        assert int(healthy["total_bytes"]) > 0
        assert 0 <= int(healthy["available_bytes"]) <= int(healthy["free_bytes"]) <= int(healthy["total_bytes"])
        assert int(reports()["s1"]["last_heartbeat_unix_ms"]) > 0
        assert healthy["health"] == "DISK_HEALTHY" and int(healthy["observed_unix_ms"]) > 0
        filesystem = os.statvfs(disk)
        assert int(healthy["total_bytes"]) == filesystem.f_blocks * filesystem.f_frsize
        cli("server-set", "s1", "localhost:" + server_endpoint.rsplit(":", 1)[1])
        wait_for(lambda: reports()["s1"]["online"] and disk_report("d1")["healthy"])
        cli("server-set", "s1", server_endpoint)
        wait_for(lambda: reports()["s1"]["online"] and disk_report("d1")["healthy"])
        ordinary_file = root / "ordinary-file"
        ordinary_file.write_text("status probes require a directory")
        direct = subprocess.run([BUILD / "fmaster_probe_test", server_endpoint, "s1", disk,
                                 ordinary_file, root / "missing-probe-path"], capture_output=True, timeout=8)
        assert direct.returncode == 0, direct.stderr

        second.start()
        wait_for(lambda: reports()["s2"]["online"])
        before = int(reports()["s1"]["last_heartbeat_unix_ms"])
        second.process.send_signal(signal.SIGSTOP)
        advanced = False
        try:
            # One probe can wait 1.5 s, longer than the 1 s offline timeout.
            # Independent scheduling must continue refreshing the responsive node.
            deadline = time.monotonic() + 2.3
            while time.monotonic() < deadline:
                first_status = reports()["s1"]
                assert first_status["online"], "slow node suppressed healthy-node probes"
                advanced |= int(first_status["last_heartbeat_unix_ms"]) > before
                time.sleep(.05)
            assert advanced
            stale = reports()["s2"]
            assert not stale["online"] and stale["error"]
            assert stale["disks"][0]["health"] == "DISK_STALE"
            assert not stale["disks"][0]["healthy"]
            assert int(stale["disks"][0]["observed_unix_ms"]) > 0
            assert int(stale["disks"][0]["total_bytes"]) > 0
        finally:
            second.process.send_signal(signal.SIGCONT)
        wait_for(lambda: reports()["s2"]["online"] and
                 reports()["s2"]["disks"][0]["health"] == "DISK_HEALTHY")

        # An old process must not claim the ID after its configured endpoint changes.
        cli("server-set", "s1", endpoint())
        deadline = time.monotonic() + .6
        while time.monotonic() < deadline:
            assert not reports()["s1"]["online"], "old endpoint re-established online state"
            time.sleep(.05)
        cli("server-set", "s1", server_endpoint)
        wait_for(lambda: reports()["s1"]["online"])

        extra = root / "disk2"
        extra.mkdir()
        cli("disk-set", "s1", "d2", extra)
        wait_for(lambda: (report := disk_report("d2")) and report["healthy"])
        missing = root / "missing-disk"
        cli("disk-set", "s1", "d2", missing)
        unhealthy = wait_for(lambda: (report := disk_report("d2")) and report["path"] == str(missing)
                            and report["health"] == "DISK_ERROR" and report)
        assert unhealthy["error"]
        cli("disk-remove", "s1", "d2")
        wait_for(lambda: disk_report("d2") is None)
        cli("server-set", "s1", server_endpoint, "disabled")
        assert config_servers()["s1"]["disabled"]
        wait_for(lambda: reports()["s1"]["online"])

        server.stop()
        second.stop()
        wait_for(lambda: not reports()["s1"]["online"])
        assert disk_report("d1")["health"] == "DISK_STALE"
        saved = snapshot()["topology"]
        master.stop(kill=True)
        # Restart must use committed metadata even if the bootstrap file is now invalid.
        config.write_text("stale bootstrap content is intentionally invalid\n")
        master.start()
        assert snapshot()["topology"] == saved
        assert all(not state["online"] and int(state["last_heartbeat_unix_ms"]) == 0
                   for state in reports().values())
        server.command[2] = "0.0.0.0:" + server_endpoint.rsplit(":", 1)[1]
        server.command.append("--advertise=" + server_endpoint)
        server.start()
        wait_for(lambda: reports()["s1"]["online"])
        cli("server-remove", "s1")
        time.sleep(.4)
        assert "s1" not in config_servers() and "s1" not in reports()
        cli("server-remove", "s2")
        assert config_servers() == {}
    finally:
        second.stop()
        server.stop()
        master.stop()

    # A corrupt persisted snapshot must fail startup instead of becoming empty configuration.
    corrupted = root / "corrupted-master"
    corrupted.mkdir()
    snapshot_file = corrupted / "topology.pb"
    shutil.copyfile(data / "topology.pb", snapshot_file)
    damaged = bytearray(snapshot_file.read_bytes())
    damaged[-1] ^= 1
    snapshot_file.write_bytes(damaged)
    result = subprocess.run([BUILD / "fmaster", f"--data-dir={corrupted}", f"--listen={endpoint()}"],
                            capture_output=True, timeout=5)
    assert result.returncode != 0, result.stdout


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as directory:
        verify(Path(directory))
    print("FMaster bootstrap, atomic configuration, persistence, concurrency and active disk status probes passed")
