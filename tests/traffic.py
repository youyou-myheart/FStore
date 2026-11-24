"""Run real online, trash and replication traffic via bRPC: python3 tests/traffic.py BUILD_DIR."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

BUILD = Path(sys.argv[1]).resolve()
MIB = 1024 * 1024


def start(root, rate, stack):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    endpoint = f"127.0.0.1:{port}"
    server = subprocess.Popen([BUILD / "chunk_server", root, endpoint, "32", str(rate)])
    def stop():
        server.send_signal(signal.SIGINT)
        server.wait(timeout=20)
    stack.callback(stop)
    for _ in range(200):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=.1):
                return endpoint
        except ConnectionRefusedError:
            assert server.poll() is None
            time.sleep(.025)
    raise RuntimeError("server did not start")


def cli(endpoint, *args):
    return subprocess.check_output([BUILD / "chunk_cli", endpoint, *map(str, args)], timeout=55)


def stats(node):
    report = cli(node, "stats").decode()
    values = []
    for lane in ("online", "trash", "copy_normal", "copy_high", "copy_low"):
        block = re.search(rf"{lane} \{{([^}}]*)\}}", report).group(1)
        charged = re.search(r"granted_bytes: (\d+)", block)
        values.append(int(charged.group(1)) if charged else 0)
    return values


def seed(root, key, size):
    folder = root / key[:2]
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / (key + ".frozen")
    with path.open("wb") as chunk:
        chunk.truncate(size)
    os.setxattr(path, "user.chunkstore.capacity", str(size).encode())


with tempfile.TemporaryDirectory() as directory, ExitStack() as servers:
    root = Path(directory)
    source_root, target_root = root / "source", root / "target"
    priorities = ("normal", "high", "low")
    keys = [f"{i:032x}" for i in range(3)]
    for key in keys:
        seed(source_root, key, 4 * MIB)
    online = "e" * 32
    seed(target_root, online, MIB)
    trash = target_root / "aa" / ".trash"
    trash.mkdir(parents=True)
    expired = trash / ("a" * 32 + ".1.frozen")
    with expired.open("wb") as chunk:
        chunk.truncate(96 * MIB)

    source = start(source_root, 256, servers)
    target = start(target_root, 16, servers)
    finish = threading.Event()
    def read_load():
        while not finish.is_set():
            assert len(cli(target, "read", online, 0, MIB)) == MIB
    with ThreadPoolExecutor(max_workers=7) as pool:
        readers = [pool.submit(read_load) for _ in range(4)]
        transfers = [pool.submit(cli, source, "repair", key, target, priority)
                     for key, priority in zip(keys, priorities)]
        try:
            time.sleep(.45)
            before = stats(target)
            time.sleep(1.5)
            after = stats(target)
        finally:
            finish.set()
        for reader in readers:
            reader.result()
        delta = [y - x for x, y in zip(before, after)]
        total = sum(delta)
        shares = [100 * value / total for value in delta]
        print("node charged percentages (online, trash, normal, high, low):", shares)
        assert abs(shares[0] - 80) < 4 and abs(shares[1] - 10) < 3
        assert abs(sum(shares[2:]) - 10) < 3
        assert shares[3] > shares[2] > shares[4] > 0
        for transfer in transfers:
            assert int(transfer.result()) == 4 * MIB
    for key in keys:
        assert cli(target, "read", key, 0, 4) == bytes(4)
    assert all(v > 0 for v in stats(source)[2:]), "sender did not charge replication lanes"
