"""Run against the compiled server with python3 tests/trash.py BUILD_DIR."""

import os
from pathlib import Path
import subprocess
import tempfile
import time

from repair import BUILD, Node


def wait_until_gone(path):
    deadline = time.monotonic() + 70
    while path.exists() and time.monotonic() < deadline:
        time.sleep(.1)
    assert not path.exists(), path


def verify(node):
    key = "a" * 30
    shard = node.root / "aa"
    trash = shard / ".trash"
    node.cli("create", key, 4)
    node.cli("append", key, 0, 0, "-", data=b"abc")
    # An old data mtime must not shorten the 24-hour retention.
    os.utime(shard / (key + ".open"), (1, 1))
    node.cli("append", key, 3, 0, "-", data=b"d")
    before = time.time_ns()
    token = int(node.cli("delete", key))
    assert before <= token <= time.time_ns()
    entry = trash / f"{key}.{token}.open"
    assert entry.read_bytes() == b"abcd"
    assert os.getxattr(entry, "user.chunkstore.capacity") == b"4"
    for args in [("read", key, 0, 0), ("freeze", key), ("delete", key)]:
        node.cli(*args, error=b"chunk error (2)")
    node.cli("append", key, 0, 0, "-", data=b"x", error=b"chunk error (2)")
    node.stop()
    node.start()
    assert entry.read_bytes() == b"abcd"
    node.cli("create", key, 1)
    node.cli("append", key, 0, 0, "-", data=b"z")
    node.cli("restore", key, token, error=b"chunk error (14)")
    node.cli("freeze", key)
    second = int(node.cli("delete", key))
    assert node.cli("restore", key, token) == b"open\n"
    assert node.cli("read", key, 0, 4) == b"abcd"
    node.cli("append", key, 4, 0, "-", data=b"x", error=b"chunk error (8)")
    fresh = int(node.cli("delete", key))
    assert node.cli("restore", key, second) == b"frozen\n"
    assert node.cli("read", key, 0, 1) == b"z"
    node.cli("append", key, 1, 0, "-", data=b"x", error=b"chunk error (3)")

    gap_id = "b" * 32
    node.cli("create", gap_id, 16)
    pending = subprocess.Popen([BUILD / "chunk_cli", node.endpoint, "append", gap_id,
                                "4", "5000", "-"], stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    pending.stdin.write(b"x")
    pending.stdin.close()
    pending.stdin = None
    time.sleep(.15)
    assert pending.poll() is None
    node.cli("delete", gap_id)
    _, error = pending.communicate(timeout=3)
    assert pending.returncode != 0 and b"chunk error (2)" in error

    key96 = "ab" * 12
    node.cli("create", key96, 4)
    node.cli("append", key96, 0, 0, "-", data=b"blob")
    token96 = int(node.cli("delete", key96))
    assert node.cli("restore", key96, token96) == b"open\n"
    assert node.cli("read", key96, 0, 4) == b"blob"
    token96 = int(node.cli("delete", key96))

    # Simulate elapsed wall-clock time using isolated entries in the test directory.
    node.stop()
    old = time.time_ns() - 25 * 3600 * 10**9
    aged = trash / f"{key}.{old}.open"
    (trash / f"{key}.{fresh}.open").rename(aged)
    trash96 = node.root / "ab" / ".trash"
    aged96 = trash96 / f"{key96}.{old}.open"
    (trash96 / f"{key96}.{token96}.open").rename(aged96)
    unrelated = trash / "unrelated"
    unrelated.write_bytes(b"keep")
    future = trash / f"{key}.{time.time_ns() + 1000}.open"
    future.write_bytes(b"keep")
    node.start()
    wait_until_gone(aged)
    wait_until_gone(aged96)
    node.cli("restore", key, old, error=b"chunk error (2)")
    # Add an expired entry after startup to exercise the periodic worker.
    aged.write_bytes(b"expired")
    aged96.write_bytes(b"expired96")
    node.cli("restore", key, old, error=b"chunk error (2)")
    wait_until_gone(aged)
    wait_until_gone(aged96)
    assert future.read_bytes() == unrelated.read_bytes() == b"keep"
    assert node.cli("read", key, 0, 1) == b"z"


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as root:
        server = Node(Path(root) / "data")
        try:
            verify(server)
        finally:
            server.stop()
    print("trash lifecycle, restore, startup and periodic expiry passed")
