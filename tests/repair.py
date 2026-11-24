"""Run the bRPC repair integration checks with: python3 tests/repair.py BUILD_DIR."""

import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time

BUILD = Path(sys.argv[1]).resolve()
BLOCK = 8 * 1024 * 1024


class Node:
    def __init__(self, directory, *server_args):
        self.root = directory
        self.server_args = server_args
        self.root.mkdir()
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.endpoint = f"127.0.0.1:{self.port}"
        self.start()

    def start(self):
        self.process = subprocess.Popen([BUILD / "chunk_server", self.root, self.endpoint, *self.server_args])
        for _ in range(200):
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=.1):
                    return
            except ConnectionRefusedError:
                assert self.process.poll() is None
                time.sleep(.02)
        raise RuntimeError("server did not start")

    def stop(self):
        self.process.send_signal(signal.SIGINT)
        self.process.wait(timeout=10)

    def cli(self, *args, data=None, error=None):
        result = subprocess.run([BUILD / "chunk_cli", self.endpoint, *map(str, args)],
                                input=data, capture_output=True, timeout=30)
        if error is None:
            assert result.returncode == 0, result.stderr
        else:
            assert result.returncode != 0 and error in result.stderr, result.stderr
        return result.stdout


def verify(source, target):
    key = "a" * 32
    contents = os.urandom(2 * BLOCK + 73)
    def sync():
        return int(source.cli("repair", key, target.endpoint))

    source.cli("create", key, len(contents))
    source.cli("repair", key, target.endpoint, error=b"chunk error (12)")
    target.cli("read", key, 0, 0, error=b"chunk error (2)")
    for offset in range(0, len(contents), BLOCK):
        source.cli("append", key, offset, 0, "-", data=contents[offset:offset + BLOCK])
    source.cli("repair", key, target.endpoint, error=b"chunk error (12)")
    source.cli("freeze", key)
    assert sync() == len(contents)
    assert sync() == len(contents)
    assert target.cli("create", key, len(contents)) == b"frozen\n"

    target.stop()
    path = target.root / "aa" / (key + ".frozen")
    damaged = b"broken!"
    path.write_bytes(damaged)
    target.start()
    assert target.cli("read", key, 0, len(damaged)) == damaged
    assert sync() == len(contents)
    target.stop()
    assert path.read_bytes() == contents
    assert int(os.getxattr(path, "user.chunkstore.capacity")) == len(contents)
    target.start()
    for offset in range(0, len(contents), BLOCK):
        expected = contents[offset:offset + BLOCK]
        assert target.cli("read", key, offset, len(expected)) == expected

    key = "b" * 32
    for node, value in [(source, b"abc"), (target, b"abc")]:
        node.cli("create", key, 16)
        node.cli("append", key, 0, 0, "-", data=value)
    source.cli("freeze", key)
    source.cli("repair", key, target.endpoint, error=b"chunk error (12)")
    assert target.cli("read", key, 0, 3) == b"abc"
    assert target.cli("append", key, 3, 0, "-", data=b"d") == b"3 1\n"

    key = "d" * 32
    source.cli("create", key, 1)
    source.cli("freeze", key)
    assert sync() == 0
    assert target.cli("create", key, 1) == b"frozen\n"
    source.cli("repair", "c" * 32, target.endpoint, error=b"chunk error (2)")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as directory:
        source = Node(Path(directory) / "source")
        try:
            target = Node(Path(directory) / "target")
            try:
                verify(source, target)
            finally:
                target.stop()
        finally:
            source.stop()
    print("two-node repair passed")
