"""Real server loss, PG sealing, paginated push, master crash and route fencing."""
from pathlib import Path
import json
import os
import subprocess
import tempfile
import time

from fmaster import BUILD, Process, endpoint


def verify(root):
    addresses = [endpoint() for _ in range(5)]
    disks = [root / f"disk{i}" for i in range(4)]
    for disk in disks:
        disk.mkdir()
    config = root / "cluster.pbtxt"
    config.write_text("\n".join(
        f'servers {{ id: "s{i}" endpoint: {json.dumps(addresses[i + 1])} datacenter: "a" '
        f'disks {{ id: "d" path: {json.dumps(str(disks[i]))} }} }}' for i in range(3)))
    master = Process([BUILD / "fmaster", f"--data-dir={root / 'master'}", f"--config={config}",
                      f"--listen={addresses[0]}", "--heartbeat-interval-ms=100", "--heartbeat-timeout-ms=1000",
                      "--probe-timeout-ms=300", "--recovery-grace-ms=1500", "--recovery-interval-ms=300"],
                     addresses[0], root / "master.log")
    nodes = [Process([BUILD / "chunk_server", disk, addresses[i + 1], f"--server-id=s{i}"],
                     addresses[i + 1], root / f"node{i}.log") for i, disk in enumerate(disks)]

    def run(binary, *args, success=True, data=None):
        result = subprocess.run([BUILD / binary, *map(str, args)], input=data, capture_output=True, timeout=30)
        assert (result.returncode == 0) == success, (binary, args, result.stdout, result.stderr)
        return result.stdout if success else result.stderr

    def cli(*args):
        return json.loads(run("fmaster_cli", addresses[0], *args))

    def local(node, pg):
        return json.loads(run("chunk_cli", addresses[node + 1], "pg-chunks", pg, "", 100))

    def wait_for(operation, timeout=20):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = operation()
            if value:
                return value
            time.sleep(.04)
        raise AssertionError(("did not converge", cli("recoveries"), [p.log.read_text()[-2000:] for p in nodes]))

    def healthy(count):
        state = cli("list")["servers"]
        return len(state) == count and all(s["online"] and not s["error"] and s["pg_recovery_capable"] for s in state)

    def job(name):
        jobs = cli("recoveries", name)["recoveries"]
        return jobs[0] if jobs else None

    try:
        for node in nodes:
            node.start()
        master.start()
        wait_for(lambda: healthy(3))
        cli("space-create", "logs", 3, 1048576, "--placement=single-dc")
        cli("pg-unbind", "logs", 1, 199)
        cli("space-create", "diverged", 3, 1048576, "--placement=single-dc")
        divergent_pg = cli("pgs", "diverged", 0, 1)["pgs"][0]["id"]
        cli("pg-unbind", "diverged", divergent_pg + 1, 199)
        # Neither initial placement pool contains the spare.
        cli("server-set", "s3", addresses[4], "--datacenter=a")
        cli("disk-set", "s3", "d", disks[3])
        wait_for(lambda: healthy(4))
        length, count = 262161, 33  # More than two recovery pages; each chunk exceeds initial copy block.
        ids = run("pg_recovery_client_test", "seed", addresses[0], "logs", count, length).decode().splitlines()
        assert len(ids) == count and int(local(1, 0)["open_count"]) > 0
        blob = cli("blob-allocate", "diverged")["blobs"][0]
        divergent_id = cli("allocate", "diverged", blob["seq_id"])["chunks"][0]["chunk_id"]
        for i in range(3):
            run("chunk_cli", addresses[i + 1], "create", divergent_id, 1048576)
        # A real partial replica write, not a forged on-disk image.
        run("chunk_cli", addresses[2], "append", divergent_id, 0, 0, "-", data=b"partial")
        initial = cli("pg", "logs", 0)["pgs"][0]
        assert {r["server_id"] for r in initial["replicas"]} == {"s0", "s1", "s2"}
        nodes[0].stop(kill=True)
        failed_at = time.monotonic()
        time.sleep(.35)
        assert job("logs") is None, "recovery started before configured grace"

        def copy_checkpoint():
            value = job("logs")
            return value if value and value["phase"] == "COPYING" and value["cursor"] else None

        checkpoint = wait_for(copy_checkpoint)
        assert time.monotonic() - failed_at >= 1.5
        assert cli("pg", "logs", 0)["pgs"][0]["recovering"]
        assert int(local(1, 0)["open_count"]) == 0, "copied before freezing whole PG"
        assert 0 < int(local(3, 0)["chunk_count"]) < count
        run("chunk_cli", addresses[2], "append", ids[1], length, 0, "-", success=False, data=b"late")
        run("chunk_cli", addresses[2], "create", ids[1], 1048576, success=False)
        # Crash after a completed page. Durable fences and cursor must survive.
        master.stop(kill=True)
        nodes[1].stop(kill=True)
        nodes[1].start()
        assert local(1, 0)["mode"] == "PG_SOURCE" and int(local(1, 0)["chunk_count"]) == count
        # The single-chunk control RPC is token fenced and preserves first cause.
        run("chunk_cli", addresses[2], "seal", ids[1], int(checkpoint["id"]) + 1, length, success=False)
        assert int(run("chunk_cli", addresses[2], "seal", ids[1], checkpoint["id"], length)) == length
        assert int(run("chunk_cli", addresses[2], "seal", ids[0], checkpoint["id"], length)) == length
        master.start()
        wait_for(lambda: (value := job("logs")) and value["phase"] == "RECOVERED")
        final = cli("pg", "logs", 0)["pgs"][0]
        assert not final["recovering"] and int(final["epoch"]) == int(checkpoint["id"])
        assert {r["server_id"] for r in final["replicas"]} == {"s1", "s2", "s3"}
        inventory = local(3, 0)
        assert int(inventory["chunk_count"]) == count and int(inventory["open_count"]) == 0
        reasons = {item["chunk_id"]: item["close_reason"] for item in inventory["chunks"]}
        for i, key in enumerate(ids):
            expected = "USER_FREEZE" if i % 2 == 0 else "PG_REPAIR"
            assert reasons[key] == expected
            for disk in disks[1:]:
                file = disk / key[:2] / (key + ".frozen")
                assert file.read_bytes() == b"x" * length
                assert os.getxattr(file, "user.chunkstore.close_reason") == expected.encode()
        # Fresh SDK handles propagate the new epoch; old direct clients fail.
        run("pg_recovery_client_test", "read", addresses[0], "logs", ids[0], length)
        run("pg_recovery_client_test", "read", addresses[0], "logs", ids[-1], length)
        run("pg_recovery_client_test", "read-sealed", addresses[0], "logs", ids[1], length)
        assert run("chunk_cli", addresses[4], "state", ids[1], f"--pg-epoch={final['epoch']}") == b"sealed\n"
        error = run("chunk_cli", addresses[4], "append", ids[1], length, 0, "-", f"--pg-epoch={final['epoch']}", success=False, data=b"late")
        assert b"chunk error (16)" in error
        deleted = int(run("chunk_cli", addresses[4], "delete", ids[1], f"--pg-epoch={final['epoch']}"))
        assert run("chunk_cli", addresses[4], "restore", ids[1], deleted, f"--pg-epoch={final['epoch']}") == b"sealed\n"
        nodes[3].stop(kill=True)
        # Observe the failed probe before restarting; otherwise a cached healthy
        # report can pass the wait while an older failed probe is still in flight.
        wait_for(lambda: any(s["server_id"] == "s3" and s["error"] for s in cli("list")["servers"]))
        nodes[3].start()
        assert run("chunk_cli", addresses[4], "state", ids[1], f"--pg-epoch={final['epoch']}") == b"sealed\n"
        wait_for(lambda: all(s["online"] and not s["error"] for s in cli("list")["servers"] if s["server_id"] != "s0"))
        run("chunk_cli", addresses[2], "read", ids[0], 0, 1, success=False)
        assert run("chunk_cli", addresses[4], "read", ids[0], 0, 1, f"--pg-epoch={final['epoch']}") == b"x"
        run("pg_recovery_client_test", "seed", addresses[0], "logs", 1, 64)
        blocked = wait_for(lambda: (value := job("diverged")) and "contents differ" in value["error"] and value)
        assert blocked["phase"] == "SEALING" and cli("pg", "diverged", divergent_pg)["pgs"][0]["recovering"]
        assert int(local(1, divergent_pg)["open_count"]) == 1 and int(local(3, divergent_pg)["chunk_count"]) == 0
        # Returning old member gets a retirement fence through the heartbeat.
        nodes[0].start()
        wait_for(lambda: healthy(4))
        wait_for(lambda: local(0, 0)["mode"] == "PG_RETIRED")
        run("chunk_cli", addresses[1], "read", ids[0], 0, 1, success=False)
        assert int(local(0, 0)["chunk_count"]) == count, "retirement unexpectedly removed data"
    finally:
        master.stop()
        for node in nodes:
            node.stop()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as directory:
        verify(Path(directory))
