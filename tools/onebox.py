#!/usr/bin/env python3
"""Run an isolated local FMaster and FServer cluster using real bRPC binaries."""

import argparse
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import uuid


def identity(pid):
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        command = Path(f"/proc/{pid}/cmdline").read_bytes().rstrip(b"\0").split(b"\0")
        return fields[0], fields[19], [os.fsdecode(word) for word in command]
    except (FileNotFoundError, ProcessLookupError):
        return None


def owned(record):
    value = identity(record.get("pid", 0))
    return bool(value and value[0] != "Z" and value[1] == record.get("start_ticks")
                and value[2] == record["command"])


def signal_process(record, signum):
    """Pin the process before checking ownership, so PID reuse cannot redirect a signal."""
    if not record.get("pid"):
        return
    try:
        descriptor = os.pidfd_open(record["pid"])
    except ProcessLookupError:
        return
    try:
        if owned(record):
            signal.pidfd_send_signal(descriptor, signum)
    except ProcessLookupError:
        pass
    finally:
        os.close(descriptor)


class Cluster:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.manifest_path = self.root / "manifest.json"
        self.children = {}
        self.manifest = None

    @contextmanager
    def locked(self):
        self.root.mkdir(parents=True, exist_ok=True)
        with (self.root / ".onebox.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if self.manifest_path.exists():
                self.manifest = json.loads(self.manifest_path.read_text())
                if self.manifest.get("root") != str(self.root):
                    raise RuntimeError("manifest belongs to a different root")
            yield self

    def save(self):
        temporary = self.root / "manifest.json.tmp"
        with temporary.open("w") as output:
            json.dump(self.manifest, output, indent=2)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        temporary.replace(self.manifest_path)

    def initialize(self, build, servers, disks):
        if servers < 1 or disks < 1:
            raise ValueError("--servers and --disks-per-server must be positive")
        if any(path.name != ".onebox.lock" for path in self.root.iterdir()):
            raise ValueError("root has files but no manifest; choose an empty --root")
        build = Path(build).resolve()
        for name in ("fmaster", "fmaster_cli", "chunk_server", "chunk_cli"):
            if not os.access(build / name, os.X_OK):
                raise RuntimeError(f"missing executable {build / name}; build FStore first")
        # Reserve all ports together to avoid selecting the same port twice.
        listeners = []
        try:
            for _ in range(servers + 1):
                listener = socket.socket()
                listener.bind(("127.0.0.1", 0))
                listeners.append(listener)
            endpoints = [f"127.0.0.1:{listener.getsockname()[1]}" for listener in listeners]
        finally:
            for listener in listeners:
                listener.close()
        config = self.root / "cluster.pbtxt"
        data = self.root / "master"
        data.mkdir(exist_ok=True)
        records = [{"id": "master", "endpoint": endpoints[0], "data_dir": str(data),
                    "log": str(self.root / "master.log"), "command": [str(build / "fmaster"),
                    f"--data-dir={data}", f"--listen={endpoints[0]}", f"--config={config}",
                    "--heartbeat-interval-ms=100", "--heartbeat-timeout-ms=1000", "--probe-timeout-ms=300"]}]
        topology = []
        for i in range(servers):
            name = f"fs{i + 1}"
            paths = [self.root / name / f"disk{j + 1}" for j in range(disks)]
            for path in paths:
                path.mkdir(parents=True, exist_ok=True)
            disk_records = [{"id": f"disk{j + 1}", "path": str(path)} for j, path in enumerate(paths)]
            records.append({"id": name, "endpoint": endpoints[i + 1], "data_dir": str(paths[0]),
                            "disks": disk_records, "log": str(self.root / f"{name}.log"),
                            "command": [str(build / "chunk_server"), str(paths[0]), endpoints[i + 1],
                                        f"--server-id={name}"]})
            disk_text = " ".join("disks { id: " + json.dumps(d["id"]) + " path: " +
                                 json.dumps(d["path"]) + " }" for d in disk_records)
            topology.append(f"servers {{ id: {json.dumps(name)} endpoint: {json.dumps(endpoints[i + 1])} {disk_text} }}")
        config.write_text("\n".join(topology) + "\n")
        self.manifest = {"version": 1, "root": str(self.root), "build_dir": str(build),
                         "config": str(config), "processes": records}
        self.save()

    def record(self, name):
        return next(record for record in self.manifest["processes"] if record["id"] == name)

    def environment(self):
        environment = os.environ.copy()
        library_path = self.manifest.get("ld_library_path")
        if library_path:
            environment["LD_LIBRARY_PATH"] = library_path
        return environment

    def command(self, binary, endpoint, *args, data=None):
        result = subprocess.run([str(Path(self.manifest["build_dir"]) / binary), endpoint, *map(str, args)],
                                input=data, capture_output=True, timeout=8, env=self.environment())
        if result.returncode:
            raise RuntimeError(result.stderr.decode(errors="replace").strip())
        return result.stdout

    def master_command(self, *args):
        return self.command("fmaster_cli", self.record("master")["endpoint"], *args)

    def snapshot(self):
        if self.manifest is None:
            raise RuntimeError("cluster is uninitialized; run up with --build-dir first")
        result = {"root": str(self.root), "master_endpoint": self.record("master")["endpoint"],
                  "processes": [dict(record, running=owned(record)) for record in self.manifest["processes"]],
                  "master": None, "master_error": ""}
        if owned(self.record("master")):
            try:
                result["master"] = json.loads(self.master_command("list"))
            except (RuntimeError, subprocess.TimeoutExpired) as error:
                result["master_error"] = str(error)
        else:
            result["master_error"] = "FMaster process is stopped"
        result["healthy"] = self.healthy(result)
        return result

    def healthy(self, snapshot):
        if not snapshot["master"] or not all(p["running"] for p in snapshot["processes"]):
            return False
        topology = {server["id"]: server for server in snapshot["master"]["topology"]["servers"]}
        reports = {server["server_id"]: server for server in snapshot["master"]["servers"]}
        for node in self.manifest["processes"][1:]:
            report, configured = reports.get(node["id"]), topology.get(node["id"])
            if not report or not configured or not report.get("online"):
                return False
            disk_reports = report.get("disks", [])
            observed = {disk["id"]: disk for disk in disk_reports}
            configured_disks = configured.get("disks", [])
            if (len(disk_reports) != len(configured_disks) or
                    observed.keys() != {disk["id"] for disk in configured_disks}):
                return False
            for disk in configured_disks:
                value = observed.get(disk["id"])
                if not value or value["path"] != disk["path"] or not value.get("healthy"):
                    return False
                total, free, available = (int(value.get(key, 0)) for key in
                                          ("total_bytes", "free_bytes", "available_bytes"))
                if total <= 0 or not 0 <= available <= free <= total:
                    return False
        return True

    def start(self, record):
        with Path(record["log"]).open("ab") as output:
            child = subprocess.Popen(record["command"], stdin=subprocess.DEVNULL,
                                     stdout=output, stderr=output, start_new_session=True,
                                     env=self.environment())
        self.children[child.pid] = child
        current = identity(child.pid)
        record.update(pid=child.pid, start_ticks=current[1] if current else "")
        self.save()
        return child

    def stop(self, record, signum=signal.SIGINT):
        signal_process(record, signum)
        deadline = time.monotonic() + 5
        while owned(record) and time.monotonic() < deadline:
            time.sleep(.02)
        if owned(record):
            signal_process(record, signal.SIGKILL)
            deadline = time.monotonic() + 5
            while owned(record) and time.monotonic() < deadline:
                time.sleep(.02)
            if owned(record):
                raise RuntimeError(f"process {record['id']} did not stop; inspect {record['log']}")
        child = self.children.get(record.get("pid"))
        if child:
            child.wait(timeout=5)

    def wait_healthy(self, timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            snapshot = self.snapshot()
            if snapshot["healthy"]:
                return snapshot
            if not all(p["running"] for p in snapshot["processes"]):
                break
            time.sleep(.05)
        logs = "\n".join(f"{p['id']} ({p['log']}):\n" + Path(p["log"]).read_text(errors="replace")[-2500:]
                         for p in self.manifest["processes"] if Path(p["log"]).exists())
        raise RuntimeError("cluster did not become healthy\n" + logs)

    def up(self, build=None, servers=3, disks=2):
        if self.manifest is None:
            if build is None:
                raise ValueError("--build-dir is required for a new cluster")
            self.initialize(build, servers, disks)
        elif build is not None and str(Path(build).resolve()) != self.manifest["build_dir"]:
            raise ValueError("existing cluster uses a different build directory; use a new --root")
        if not self.manifest.get("ld_library_path") and os.environ.get("LD_LIBRARY_PATH"):
            self.manifest["ld_library_path"] = os.environ["LD_LIBRARY_PATH"]
            self.save()
        started = []
        try:
            for record in self.manifest["processes"]:
                if not owned(record):
                    started.append(record)
                    self.start(record)
            return self.wait_healthy()
        except BaseException:
            for record in reversed(started):
                self.stop(record)
            raise

    def down(self):
        if self.manifest:
            for record in reversed(self.manifest["processes"]):
                self.stop(record)
        return {"root": str(self.root), "stopped": True}

    def check(self):
        snapshot = self.snapshot()
        if not snapshot["healthy"]:
            raise RuntimeError("cluster is unhealthy; inspect status and service logs")
        endpoints = [node["endpoint"] for node in self.manifest["processes"][1:]]
        replicas, key, data = ",".join(endpoints), uuid.uuid4().hex, os.urandom(128 * 1024)
        self.command("chunk_cli", replicas, "create", key, len(data))
        self.command("chunk_cli", replicas, "append", key, 0, 0, "-", data=data)
        if self.command("chunk_cli", replicas, "read", key, 0, len(data)) != data:
            raise RuntimeError("replicated open read returned different data")
        self.command("chunk_cli", replicas, "freeze", key, len(data))
        if self.command("chunk_cli", replicas, "read", key, 0, len(data)) != data:
            raise RuntimeError("frozen read returned different data")
        if len(endpoints) > 1:
            self.command("chunk_cli", endpoints[-1], "delete", key)
            self.command("chunk_cli", endpoints[0], "repair", key, endpoints[-1])
            if self.command("chunk_cli", endpoints[-1], "read", key, 0, len(data)) != data:
                raise RuntimeError("repaired data differs")
        return {"healthy": True, "chunk_id": key, "bytes": len(data), "replicas": len(endpoints),
                "repair_checked": len(endpoints) > 1}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("up", "status", "down", "check"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--build-dir", type=Path)
    parser.add_argument("--servers", type=int, default=3)
    parser.add_argument("--disks-per-server", type=int, default=2)
    args = parser.parse_args()
    try:
        with Cluster(args.root).locked() as cluster:
            if args.command == "up":
                result = cluster.up(args.build_dir, args.servers, args.disks_per_server)
            else:
                result = getattr(cluster, "snapshot" if args.command == "status" else args.command)()
            print(json.dumps(result, indent=2))
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
