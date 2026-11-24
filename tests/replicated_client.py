"""Run against three real bRPC servers, with independent chunk directories."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path
import os
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


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
        nodes = []
        for name in ("a", "b", "c"):
            node = Node(Path(directory) / name)
            stack.callback(node.stop)
            nodes.append(node)
        verify(nodes)
    print("three-replica commit, replay, conditional freeze and concurrent fan-out passed")
