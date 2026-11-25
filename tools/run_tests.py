"""Configure, build and run the FStore test suites with CTest."""

import argparse
import os
from pathlib import Path
import subprocess


def positive(value):
    count = int(value)
    if count <= 0:
        raise argparse.ArgumentTypeError("jobs must be positive")
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, default=Path("build"))
    parser.add_argument("--suite", choices=("all", "unit", "integration", "e2e"), default="all")
    parser.add_argument("--jobs", type=positive, default=min(os.cpu_count() or 1, 2))
    parser.add_argument("--skip-build", action="store_true", help="run against an existing test build")
    parser.add_argument("--cmake-arg", action="append", default=[], help="extra configure argument, e.g. --cmake-arg=-DCMAKE_PREFIX_PATH=/opt/brpc")
    args = parser.parse_args()
    source = Path(__file__).resolve().parents[1]
    build = args.build_dir.resolve()
    if not args.skip_build:
        subprocess.run(["cmake", "-S", str(source), "-B", str(build), "-DCMAKE_BUILD_TYPE=Release",
                        "-DBUILD_TESTING=ON", *args.cmake_arg], check=True)
        subprocess.run(["cmake", "--build", str(build), "--parallel", str(args.jobs)], check=True)
    if not (build / "CTestTestfile.cmake").is_file():
        parser.error("build directory has no CTest configuration; build with BUILD_TESTING=ON")
    reports = build / "test-results"
    reports.mkdir(exist_ok=True)
    command = ["ctest", "--test-dir", str(build), "--output-on-failure", "--no-tests=error",
               "--parallel", str(args.jobs), "--output-log", str(reports / (args.suite + ".log"))]
    if args.suite != "all":
        command.extend(["-L", "^" + args.suite + "$"])
    # CMake 3.20 remains supported; newer CTest versions also emit JUnit XML.
    if "--output-junit" in subprocess.check_output(["ctest", "--help"], text=True):
        command.extend(["--output-junit", str(reports / (args.suite + ".xml"))])
    return subprocess.run(command).returncode


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, subprocess.CalledProcessError) as error:
        raise SystemExit(str(error))
