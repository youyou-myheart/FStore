"""Real FMaster/FServer space placement, durable allocation and routed SDK I/O."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json
import subprocess
import tempfile
import time

from fmaster import BUILD, Process, endpoint


def index_hash(index):
    mask = (1 << 64) - 1
    index = ((index ^ (index >> 30)) * 0xbf58476d1ce4e5b9) & mask
    index = ((index ^ (index >> 27)) * 0x94d049bb133111eb) & mask
    return index ^ (index >> 31)


def verify(root):
    addresses = [endpoint() for _ in range(4)]
    data = root / "master"
    data.mkdir()
    disks = [root / f"disk{i}" for i in range(3)]
    for path in disks:
        path.mkdir()
    config = root / "cluster.pbtxt"
    config.write_text("\n".join(
        f'servers {{ id: "s{i}" endpoint: {json.dumps(addresses[i + 1])} '
        f'datacenter: "{dc}" disks {{ id: "d" path: {json.dumps(str(disks[i]))} }} }}'
        for i, dc in enumerate(("a", "a", "b"))))
    master = Process([BUILD / "fmaster", f"--data-dir={data}", f"--config={config}",
                      f"--listen={addresses[0]}", "--heartbeat-interval-ms=100",
                      "--heartbeat-timeout-ms=1000", "--probe-timeout-ms=300"], addresses[0], root / "master.log")
    nodes = [Process([BUILD / "chunk_server", disks[i], addresses[i + 1], f"--server-id=s{i}"],
                     addresses[i + 1], root / f"node{i}.log") for i in range(3)]

    def cli(*args, success=True):
        result = subprocess.run([BUILD / "fmaster_cli", addresses[0], *map(str, args)], capture_output=True, timeout=10)
        assert (result.returncode == 0) == success, (args, result.stdout, result.stderr)
        return json.loads(result.stdout) if success else result.stderr

    def new_chunks(name, count):
        allocated = cli("blob-allocate", name)["blobs"][0]
        return cli("allocate", name, allocated["seq_id"], allocated["start_cursor"], count)

    def data_cli(address, *args):
        result = subprocess.run([BUILD / "chunk_cli", address, *map(str, args)], capture_output=True, timeout=10)
        assert result.returncode == 0, (args, result.stdout, result.stderr)
        return result.stdout

    def wait_healthy():
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            state = cli("list")
            if len(state["servers"]) == 3 and all(s["online"] and all(d["healthy"] for d in s["disks"]) for s in state["servers"]):
                return
            time.sleep(.05)
        raise AssertionError(state)

    try:
        for node in nodes:
            node.start()
        master.start()
        wait_healthy()
        empty = cli("spaces")
        assert empty["spaces"] == []
        created = cli("space-create", "logs", 3, 65536, "--placement=balanced-dc",
                      "--datacenters=a,b", "--min-datacenters=2", "--expected-revision=1")
        assert created["space"]["total_pgs"] == 10000000 and created["space"]["bound_pgs"] == 200
        assert created["space"]["spec"]["placement_policy"]["mode"] == "BALANCED_DATACENTERS"
        assert cli("space", "logs")["spaces"][0] == created["space"]
        assert not cli("pg", "logs", 200)["pgs"][0]["active"]
        page = cli("pgs", "logs", 0, 200)
        before = page["pgs"]
        assert len(before) == 200 and "next_start" not in page
        for pg in before:
            assert len({r["server_id"] for r in pg["replicas"]}) == 3
            assert len({r["datacenter"] for r in pg["replicas"]}) == 2
        cli("space-create", "bad", 3, 65536, "--placement=distinct-dc", success=False)
        cli("space-create", "bad", 3, 65536, "--datacenters=a", success=False)
        cli("pg", "logs", 10000000, success=False)
        cli("pgs", "logs", 0, 1001, success=False)
        cli("allocate", "logs", 1, 0, 1025, success=False)
        other = cli("space-create", "other", 3, 65536, "--placement=balanced-dc")
        assert cli("pgs", "other", 0, 1)["pgs"][0]["id"] == 200
        owners = cli("pg-pool", 198, 4)
        assert owners["bound_pgs"] == 400 and owners["total_pgs"] == 10000000
        assert [p["space_name"] for p in owners["pgs"]] == ["logs", "logs", "other", "other"]
        cli("pg-bind", "logs", 199, 2, success=False)
        cli("pg-unbind", "other", 200, 100)
        cli("pg-bind", "other", 5000000, 100)
        assert cli("pgs", "other", 399, 2)["pgs"][1]["id"] == 5000000
        cli("pg-bind", "logs", 7000000, 200, "--expected-revision=1", success=False)
        revision = cli("space", "logs")["revision"]
        updated = cli("pg-bind", "logs", 7000000, 200, f"--expected-revision={revision}")
        assert updated["space"]["bound_pgs"] == 400
        assert cli("pg-bind", "logs", 7000000, 200)["revision"] == updated["revision"]
        assert cli("pgs", "logs", 0, 200)["pgs"] == before
        blob = cli("blob-allocate", "logs")["blobs"][0]
        automatic = cli("allocate", "logs", blob["seq_id"])
        assert automatic == cli("allocate", "logs", blob["seq_id"], "auto")
        assert automatic == cli("allocate", "logs", blob["seq_id"], blob["start_cursor"])
        assert automatic["chunks"][0]["pg"]["id"] == int(blob["start_cursor"])
        assert len(cli("allocate", "logs", blob["seq_id"], "auto", 3)["chunks"]) == 3
        with ThreadPoolExecutor(max_workers=6) as executor:
            batches = list(executor.map(lambda _: new_chunks("logs", 401), range(6)))
        chunks = [chunk for batch in batches for chunk in batch["chunks"]]
        ids = {chunk["chunk_id"] for chunk in chunks}
        assert len(ids) == 2406
        for chunk in chunks:
            key = chunk["chunk_id"]
            assert len(key) == 30 and int(key[12:14], 16) == created["space"]["id"]
            pg = index_hash(int(key[14:], 16)) % 10000000
            assert pg == chunk["pg"]["id"] and (pg < 200 or 7000000 <= pg < 7000200)
        assert any(c["pg"]["id"] >= 7000000 for c in chunks)
        assert len({int(batch["chunks"][0]["chunk_id"][:12], 16) for batch in batches}) == 6
        sample = chunks[0]
        sample_seq = int(sample["chunk_id"][:12], 16)
        continuation = cli("allocate", "logs", sample_seq, batches[0]["next_cursor"], 401)
        assert not ids.intersection(c["chunk_id"] for c in continuation["chunks"])
        ids.update(c["chunk_id"] for c in continuation["chunks"])
        assert continuation == cli("allocate", "logs", sample_seq, batches[0]["next_cursor"], 401)
        cli("allocate", "other", sample_seq, 0, 1, success=False)
        cli("allocate", "logs", sample_seq, 1 << 64, 1, success=False)
        cli("pg-unbind", "logs", 0, 1, success=False)
        assert cli("locate", "logs", sample["chunk_id"])["chunk"]["pg"] == sample["pg"]
        # Protected placement cannot be silently changed by the generic topology APIs.
        cli("server-remove", "s0", success=False)
        cli("server-set", "s0", addresses[1], "--datacenter=z", success=False)
        cli("server-set", "s0", endpoint(), success=False)
        cli("server-disable", "s0")
        cli("allocate", "logs", sample_seq, 0, 1, success=False)
        cli("server-enable", "s0")
        wait_healthy()
        # Exercise the SDK against all actual replica processes, including size limits.
        sdk = subprocess.run([BUILD / "space_client_test", addresses[0], "logs"], capture_output=True, timeout=15)
        assert sdk.returncode == 0, (sdk.stdout, sdk.stderr)
        physical_id = sdk.stdout.decode().strip()
        assert len(physical_id) == 30
        for disk in disks:
            assert list(disk.rglob(physical_id[2:] + ".frozen")) or list(disk.rglob(physical_id + ".frozen")), list(disk.rglob("*.frozen"))
        # The same 120-bit identifier works through restore and frozen repair.
        token = int(data_cli(addresses[3], "delete", physical_id))
        assert data_cli(addresses[3], "restore", physical_id, token) == b"frozen\n"
        data_cli(addresses[3], "delete", physical_id)
        assert int(data_cli(addresses[1], "repair", physical_id, addresses[3])) == 4096
        assert data_cli(addresses[3], "read", physical_id, 0, 4096) == b"x" * 4096
        # Unused Spaces can release all bindings; allocation then fails clearly.
        cli("pg-unbind", "other", 300, 100)
        cli("pg-unbind", "other", 5000000, 100)
        cli("blob-allocate", "other", success=False)
        cli("locate", "other", physical_id, success=False)
        # Real SIGKILL after acknowledged allocations, then recovery from durable metadata.
        master.stop(kill=True)
        master.start()
        wait_healthy()
        assert cli("pgs", "logs", 0, 200)["pgs"] == before
        after = new_chunks("logs", 100)["chunks"]
        assert not ids.intersection(c["chunk_id"] for c in after)
        assert int(after[0]["chunk_id"][:12], 16) > max(int(key[:12], 16) for key in ids | {physical_id})
        assert cli("locate", "logs", physical_id)["chunk"]["pg"]["active"]
        cli("pg-bind", "logs", 9999900, 100)
        assert cli("pg", "logs", 9999999)["pgs"][0]["active"]
        assert (data / "spaces.pb").stat().st_size < 16384
        master.stop()
        contents = bytearray((data / "spaces.pb").read_bytes())
        contents[-1] ^= 1
        (data / "spaces.pb").write_bytes(contents)
        invalid = subprocess.run(master.command, capture_output=True, timeout=5)
        assert invalid.returncode != 0 and b"catalog" in invalid.stderr
    finally:
        master.stop()
        for node in nodes:
            node.stop()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as temporary:
        verify(Path(temporary))
    print("Space policies, exclusive global PG bindings, allocation, crash recovery and routed replica I/O passed")
