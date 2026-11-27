"""Append sync startup options and real RPC behavior in both modes."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import subprocess
import tempfile

from repair import BUILD, Node


def verify(root, mode):
    flags = [] if mode == "default" else ["--append-sync=" + mode]
    source = Node(root / "source", *flags)
    try:
        target = Node(root / "target", "--append-sync=none")
        try:
            key = "00000000000101" + "0" * 16
            source.cli("create", key, 8)
            assert source.cli("sync", key, 0) == b"0\n"
            with ThreadPoolExecutor(max_workers=8) as pool:
                calls = [pool.submit(source.cli, "append", key, i, 3000, "-", data=bytes([i]))
                         for i in reversed(range(8))]
                for call in calls:
                    call.result()
            assert source.cli("read", key, 0, 8) == bytes(range(8))
            assert source.cli("sync", key) == b"8\n"
            assert source.cli("sync", key, 8) == b"8\n"
            assert source.cli("state", key) == b"open\n"
            source.cli("sync", key, 0, error=b"chunk error (15)")
            source.cli("sync", key, 7, error=b"chunk error (15)")
            source.cli("sync", key, 8, "--pg-epoch=1", error=b"chunk error (6)")
            source.cli("append", key, 8, 0, "-", data=b"x", error=b"chunk error (8)")
            source.cli("freeze", key)
            assert source.cli("sync", key, 8) == b"8\n"
            source.cli("append", key, 8, 0, "-", data=b"x", error=b"chunk error (3)")
            assert int(source.cli("repair", key, target.endpoint)) == 8
            target.stop()
            target.start()
            assert target.cli("read", key, 0, 8) == bytes(range(8))
            assert target.cli("state", key) == b"frozen\n"
            endpoints = source.endpoint + "," + target.endpoint
            def replicated(*args, error=None):
                result = subprocess.run([BUILD / "chunk_cli", endpoints, *map(str, args)], capture_output=True, timeout=15)
                if error is None:
                    assert result.returncode == 0, result.stderr
                else:
                    assert result.returncode and error in result.stderr, result.stderr
                return result.stdout
            assert replicated("sync", key, 8) == b"8\n"
            assert replicated("sync", key) == b"8\n"
            fresh = "00000000000201" + "0" * 16
            source.cli("sync", fresh, error=b"chunk error (2)")
            for node in (source, target):
                node.cli("create", fresh, 16)
                node.cli("append", fresh, 0, 0, "-", data=b"abc")
            assert replicated("sync", fresh, 3) == b"3\n"
            assert source.cli("state", fresh) == target.cli("state", fresh) == b"open\n"
            source.cli("append", fresh, 3, 0, "-", data=b"d")
            replicated("sync", fresh, error=b"replica commit incomplete")
            replicated("sync", fresh, 4, error=b"chunk error (15)")
            target.cli("append", fresh, 3, 0, "-", data=b"d")
            assert replicated("sync", fresh, 4) == b"4\n"
            token = target.cli("delete", fresh).strip().decode()
            replicated("sync", fresh, 4, error=b"chunk error (2)")
            target.cli("restore", fresh, token)
            assert replicated("sync", fresh, 4) == b"4\n"
            # A sync covers completed bytes, not a queued append with an unfilled gap.
            with ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(source.cli, "append", fresh, 5, 3000, "-", data=b"f")
                assert source.cli("sync", fresh, 4) == b"4\n"
                source.cli("sync", fresh, 6, error=b"chunk error (15)")
                source.cli("append", fresh, 4, 0, "-", data=b"e")
                pending.result()
            assert source.cli("sync", fresh, 6) == b"6\n"
            assert source.cli("state", fresh) == b"open\n"
        finally:
            target.stop()
    finally:
        source.stop()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        for mode in ("default", "always", "none"):
            case = root / mode
            case.mkdir()
            verify(case, mode)
        for flags in (["--append-sync="], ["--append-sync=bogus"],
                      ["--append-sync=none", "--append-sync=always"]):
            result = subprocess.run([BUILD / "chunk_server", root, *flags], capture_output=True, timeout=3)
            assert result.returncode and b"--append-sync" in result.stderr, result.stderr
    print("append sync defaults, options, ordered writes, close and repair passed")
