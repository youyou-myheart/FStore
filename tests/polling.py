"""Exercise disk worker waits through the bRPC server."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import subprocess
import tempfile
import time

from repair import BUILD, Node


def verify(directory, mode):
    node = Node(directory, "--worker-poll=" + mode, "--poll-workers=2")
    try:
        key = "a" * 32
        node.cli("create", key, 8)
        # A gap must expire even if the spin budget is several seconds or infinite.
        begin = time.monotonic()
        node.cli("append", key, 1, 80, "-", data=b"b", error=b"chunk error (11)")
        elapsed = time.monotonic() - begin
        assert .07 <= elapsed < 2, (mode, elapsed)
        node.cli("read", key, 0, 1, error=b"chunk error (4)")

        # A burst after an idle interval must also wake a sleeping hybrid worker.
        time.sleep(.05)
        with ThreadPoolExecutor(max_workers=8) as pool:
            calls = [pool.submit(node.cli, "append", key, i, 2000, "-", data=bytes([i]))
                     for i in reversed(range(8))]
            for call in calls:
                call.result()
        assert node.cli("read", key, 0, 8) == bytes(range(8))
    finally:
        node.stop()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as root:
        for mode in ("50us", "3000000us", "busy"):
            verify(Path(root) / mode, mode)
        for options in (["--worker-poll=bogus"], ["--worker-poll=busy", "--poll-workers=0"]):
            result = subprocess.run([BUILD / "chunk_server", root, *options],
                                    capture_output=True, timeout=3)
            assert result.returncode and b"--" in result.stderr, result.stderr
    print("hybrid and busy polling, gap expiry, burst wakeup and idle shutdown passed")
