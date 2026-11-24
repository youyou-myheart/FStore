"""Run against three real bRPC servers, with independent chunk directories."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path
import os
import signal
import subprocess
import tempfile
import time

from repair import BUILD, Node


def cli(replicas, *args, data=None, errors=()):
    result = subprocess.run([BUILD / "chunk_cli", replicas, *map(str, args)],
                            input=data, capture_output=True, timeout=25)
    if errors:
        assert result.returncode != 0, result.stdout
        for error in errors:
            assert error in result.stderr.decode(), result.stderr
    else:
        assert result.returncode == 0, result.stderr
    return result.stdout


def verify(nodes):
    replicas = ",".join(n.endpoint for n in nodes)
    a, b, c = nodes
    key = "a" * 32
    data = b"\x00abc\xff"
    length = len(data)
    duplicate = f"{a.endpoint},localhost:{a.port}"
    cli(duplicate, "create", key, length, errors=["duplicate replica"])
    a.cli("read", key, 0, 0, error=b"chunk error (2)")
    assert cli(replicas, "create", key, length) == b"open\n"

    c.stop()
    cli(replicas, "append", key, 0, 0, "-", data=data, errors=[
        a.endpoint + " ack 5", b.endpoint + " ack 5", c.endpoint + " RPC error"])
    c.start()
    # The two full replicas may seal. The short one must stay writable.
    cli(replicas, "freeze", key, length, errors=[
        a.endpoint + " ack 5", b.endpoint + " ack 5", c.endpoint + " chunk error (15)"])
    cli(replicas, "create", key, length, errors=[
        a.endpoint + " ack frozen", b.endpoint + " ack frozen", c.endpoint + " ack open"])
    assert cli(replicas, "append", key, 0, 0, "-", data=data) == b"0 5\n"
    for node in nodes:
        assert node.cli("read", key, 0, length) == data
    cli(replicas, "append", key, 0, 0, "-", data=b"wrong", errors=["committed bytes differ"])
    cli(replicas, "append", key, 4, 0, "-", data=b"more", errors=["range is not fully committed"])
    cli(replicas, "append", key, length, 0, "-", data=b"!", errors=[c.endpoint + " chunk error (8)"])
    assert cli(replicas, "freeze", key, length) == b"5\n"
    assert cli(replicas, "freeze", key, length) == b"5\n"

    key = "b" * 32
    cli(replicas, "create", key, 10)
    cli(replicas, "append", key, 1, 10, "-", data=b"z",
        errors=[n.endpoint + " chunk error (11)" for n in nodes])
    for node in (b, c):
        node.cli("append", key, 0, 0, "-", data=b"x")
    with ThreadPoolExecutor(max_workers=1) as pool:
        # A needs its prefix. B and C must receive the same RPC before A replies.
        suffix = pool.submit(cli, replicas, "append", key, 1, 10000, "-", data=b"y")
        deadline = time.monotonic() + 8
        for node in (b, c):
            path = node.root / "bb" / (key + ".open")
            while path.read_bytes() != b"xy":
                assert time.monotonic() < deadline, "fan-out stalled behind first replica"
                time.sleep(.02)
        a.cli("append", key, 0, 0, "-", data=b"x")
        assert suffix.result() == b"1 1\n"
    for node in nodes:
        assert node.cli("read", key, 0, 2) == b"xy"

    # Concurrent admissions for the same ID must keep one owner until every
    # parked range is complete, even when other workers are idle.
    key = "d" * 32
    ordered = bytes(range(12))
    cli(replicas, "create", key, len(ordered))
    with ThreadPoolExecutor(max_workers=len(ordered)) as pool:
        calls = [pool.submit(cli, replicas, "append", key, i, 10000, "-", data=ordered[i:i+1])
                 for i in reversed(range(len(ordered)))]
        for call in calls:
            call.result()
    for node in nodes:
        assert node.cli("read", key, 0, len(ordered)) == ordered

    key = "c" * 32
    payload = os.urandom(1024 * 1024 + 17)
    cli(replicas, "create", key, len(payload))
    for _ in range(2):
        cli(replicas, "append", key, 0, 0, "-", data=payload)
    for node in nodes:
        assert node.cli("read", key, 0, len(payload)) == payload
    changed_tail = payload[:-1] + bytes([payload[-1] ^ 1])
    cli(replicas, "append", key, 0, 0, "-", data=changed_tail,
        errors=[n.endpoint + " chunk error (10)" for n in nodes])


def verify_reads(nodes):
    replicas = ",".join(n.endpoint for n in nodes)
    a, b, c = nodes
    key = "e" * 32
    cli(replicas, "create", key, 32)
    cli(replicas, "append", key, 0, 0, "-", data=b"hello")
    assert cli(replicas, "read", key, 0, 5) == b"hello"
    assert cli(replicas, "read", key, 5, 0) == b""
    # A single success cannot form a quorum. Two identical reads can.
    c.cli("append", key, 5, 0, "-", data=b"x")
    cli(replicas, "read", key, 0, 6, errors=["read quorum not reached", "required 2"])
    b.cli("append", key, 5, 0, "-", data=b"x")
    assert cli(replicas, "read", key, 0, 6) == b"hellox"
    # The majority may exclude the first replica, even when it returns data.
    a.cli("append", key, 5, 0, "-", data=b"y")
    assert cli(replicas, "read", key, 0, 6) == b"hellox"
    cli(f"{a.endpoint},{b.endpoint}", "read", key, 0, 6,
        errors=["read quorum not reached", "required 2"])
    a.stop()
    try:
        assert cli(replicas, "read", key, 0, 6) == b"hellox"
    finally:
        a.start()
    c.stop()
    try:
        assert cli(replicas, "read", key, 0, 5) == b"hello"
        cli(replicas, "read", key, 0, 6, errors=["read quorum not reached", "RPC error"])
    finally:
        c.start()
    # All three successful replies can still disagree.
    for node, value in zip(nodes, (b"a", b"b", b"c")):
        node.cli("append", key, 6, 0, "-", data=value)
    cli(replicas, "read", key, 0, 7, errors=["read quorum not reached", "matching replicas 1/3"])

    def quick_read(expected):
        # A paused server never replies. The SDK's normal timeout is 5 s.
        result = subprocess.run([BUILD / "chunk_cli", replicas, "read", key, "0", str(len(expected))],
                                capture_output=True, timeout=2)
        assert result.returncode == 0 and result.stdout == expected, result.stderr

    def sdk_reads(expected, targets):
        result = subprocess.run([BUILD / "replicated_read_test", key, expected,
                                 *(n.endpoint for n in targets)], capture_output=True, timeout=3)
        assert result.returncode == 0, result.stderr

    sdk_reads(b"hello", [a])  # Quorum for a one-replica SDK is one.
    c.process.send_signal(signal.SIGSTOP)
    try:
        quick_read(b"hello")  # Matching A and B must not wait for C.
        sdk_reads(b"hello", nodes)
        b.cli("freeze", key, 7)
        quick_read(b"helloxb")  # A differs, B is frozen, C never replies.
        sdk_reads(b"helloxb", nodes)
        a.stop()
        try:
            quick_read(b"helloxb")  # First replica fails, another frozen replica suffices.
        finally:
            a.start()
    finally:
        c.process.send_signal(signal.SIGCONT)
    # Frozen data outside the requested range does not count as a successful read.
    cli(replicas, "read", key, 0, 8, errors=["read quorum not reached", "chunk error (4)"])
    for preferred in nodes:
        order = [preferred] + [node for node in nodes if node is not preferred]
        assert cli(",".join(n.endpoint for n in order), "read", key, 0, 7) == b"helloxb"
    # Empty frozen ranges also use the single-replica path.
    a.stop()
    c.stop()
    try:
        assert cli(replicas, "read", key, 7, 0) == b""
    finally:
        a.start()
        c.start()


def verify_replica_reads(nodes):
    def run(mode):
        result = subprocess.run([BUILD / "replica_reads_test", mode, *(n.endpoint for n in nodes)],
                                capture_output=True, timeout=10)
        assert result.returncode == 0, result.stderr

    run("setup")
    c = nodes[2]
    c.stop()
    try:
        run("unavailable")
    finally:
        c.start()
    c.process.send_signal(signal.SIGSTOP)
    try:
        run("timeout")
    finally:
        c.process.send_signal(signal.SIGCONT)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
        nodes = []
        for name in ("a", "b", "c"):
            node = Node(Path(directory) / name)
            stack.callback(node.stop)
            nodes.append(node)
        verify(nodes)
        verify_reads(nodes)
        verify_replica_reads(nodes)
    print("replica writes, frozen/quorum reads, per-replica results and slow-replica handling passed")
