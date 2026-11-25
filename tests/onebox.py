"""Exercise the persistent onebox CLI and active heartbeats against real local processes."""

import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
BUILD = Path(sys.argv[1]).resolve()
TOOL = ROOT / "tools" / "onebox.py"
spec = importlib.util.spec_from_file_location("fstore_onebox", TOOL)
onebox = importlib.util.module_from_spec(spec)
spec.loader.exec_module(onebox)


def command(root, operation, *args, success=True, inherit_library_path=True):
    environment = os.environ.copy()
    if not inherit_library_path:
        environment.pop("LD_LIBRARY_PATH", None)
    result = subprocess.run([sys.executable, "-B", TOOL, operation, "--root", root, *map(str, args)],
                            capture_output=True, timeout=30, env=environment)
    assert (result.returncode == 0) == success, (result.stdout, result.stderr)
    return json.loads(result.stdout) if success else result.stderr.decode()


def wait_for(root, predicate):
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        snapshot = command(root, "status")
        if predicate(snapshot):
            return snapshot
        time.sleep(.05)
    raise AssertionError(json.dumps(snapshot))


def verify(root):
    other = root / "isolated-cluster"
    try:
        first = command(root, "up", "--build-dir", BUILD)
        assert first["healthy"] and len(first["processes"]) == 4
        assert all(len(node["disks"]) == 2 for node in first["processes"][1:])
        if os.environ.get("LD_LIBRARY_PATH"):
            manifest = json.loads((root / "manifest.json").read_text())
            assert manifest["ld_library_path"] == os.environ["LD_LIBRARY_PATH"]
        assert command(root, "status", inherit_library_path=False)["healthy"]
        # An older or failed-start manifest may predate the runtime setting.
        if os.environ.get("LD_LIBRARY_PATH"):
            manifest.pop("ld_library_path")
            (root / "manifest.json").write_text(json.dumps(manifest))
        assert [p["pid"] for p in command(root, "up")["processes"]] == [p["pid"] for p in first["processes"]]
        if os.environ.get("LD_LIBRARY_PATH"):
            assert json.loads((root / "manifest.json").read_text())["ld_library_path"] == os.environ["LD_LIBRARY_PATH"]
        smoke = command(root, "check", inherit_library_path=False)
        assert smoke["healthy"] and smoke["repair_checked"] and smoke["replicas"] == 3
        stored = root / "fs1" / "disk1" / smoke["chunk_id"][:2] / (smoke["chunk_id"] + ".frozen")
        data = stored.read_bytes()
        assert len(data) == smoke["bytes"]
        independent = command(other, "up", "--build-dir", BUILD, "--servers", 1, "--disks-per-server", 1)
        assert independent["healthy"]
        assert {p["endpoint"] for p in independent["processes"]}.isdisjoint(
            p["endpoint"] for p in first["processes"])

        with onebox.Cluster(root).locked() as cluster:
            server = cluster.record("fs2")
            old_pid = server["pid"]
            cluster.stop(server, signal.SIGKILL)
        wait_for(root, lambda s: any(n["server_id"] == "fs2" and not n["online"]
                                    for n in s["master"]["servers"]))
        restarted = command(root, "up")
        assert restarted["healthy"]
        assert next(p["pid"] for p in restarted["processes"] if p["id"] == "fs2") != old_pid

        with onebox.Cluster(root).locked() as cluster:
            original = cluster.record("fs1")["disks"][1]["path"]
            cluster.master_command("disk-set", "fs1", "disk2", root / "missing-disk")
        broken = wait_for(root, lambda s: any(d["id"] == "disk2" and not d["healthy"] and d["error"]
                                            for n in s["master"]["servers"] if n["server_id"] == "fs1"
                                            for d in n["disks"]))
        assert not broken["healthy"]
        with onebox.Cluster(root).locked() as cluster:
            cluster.master_command("disk-set", "fs1", "disk2", original)
            added_disk = root / "fs1" / "disk3"
            added_disk.mkdir()
            cluster.master_command("disk-set", "fs1", "disk3", added_disk)
        saved = wait_for(root, lambda s: s["healthy"])["master"]["topology"]
        assert len(next(n["disks"] for n in saved["servers"] if n["id"] == "fs1")) == 3
        with onebox.Cluster(root).locked() as cluster:
            cluster.stop(cluster.record("master"), signal.SIGKILL)
        assert not command(root, "status")["healthy"]
        # FMaster must reload committed configuration, even with an obsolete bootstrap.
        (root / "cluster.pbtxt").write_text("obsolete bootstrap data\n")
        recovered = command(root, "up")
        assert recovered["healthy"] and recovered["master"]["topology"] == saved
        assert stored.read_bytes() == data

        # Identity mismatch must leave an unrelated reused PID untouched.
        with onebox.Cluster(root).locked() as cluster:
            record = cluster.record("fs1")
            stale = dict(record, start_ticks=str(int(record["start_ticks"]) - 1))
            onebox.signal_process(stale, signal.SIGKILL)
            assert onebox.owned(record)
        command(root, "down")
        command(root, "down")
        stopped = command(root, "status")
        assert all(not p["running"] for p in stopped["processes"])
        assert command(other, "status")["healthy"], "down affected another root"
        assert stored.read_bytes() == data
        restored = command(root, "up", inherit_library_path=False)
        assert restored["healthy"] and restored["master"]["topology"] == saved
        assert stored.read_bytes() == data
        with onebox.Cluster(root).locked() as cluster:
            cluster.master_command("disk-remove", "fs1", "disk3")
        removed = wait_for(root, lambda s: s["healthy"])["master"]["topology"]
        assert len(next(n["disks"] for n in removed["servers"] if n["id"] == "fs1")) == 2

        # Failure after starting earlier services must roll back only new processes.
        command(root, "down")
        manifest_path = root / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        original_command = manifest["processes"][-1]["command"]
        manifest["processes"][-1]["command"] = [str(root / "missing-executable")]
        manifest_path.write_text(json.dumps(manifest))
        command(root, "up", success=False)
        assert all(not p["running"] for p in command(root, "status")["processes"])
        manifest = json.loads(manifest_path.read_text())
        manifest["processes"][-1]["command"] = original_command
        manifest_path.write_text(json.dumps(manifest))
    finally:
        try:
            command(root, "down")
        finally:
            command(other, "down")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="fstore-onebox-test-") as directory:
        verify(Path(directory))
    print("onebox startup, smoke I/O, active disk reports, failure recovery and safe cleanup passed")
