#!/usr/bin/env python3
"""Build the pinned LightGBM CLI against the payload's Open MPI wheel."""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import subprocess
import tarfile
import tempfile
import urllib.request
from pathlib import Path


LIGHTGBM_VERSION = "4.7.0"
LIGHTGBM_URL = "https://files.pythonhosted.org/packages/63/8e/4db5e29290d7e619c307fdb8dab0a0514090af2ce3ec483050e024ec6126/lightgbm-4.7.0.tar.gz"
LIGHTGBM_SHA256 = "f8e20f682c9aabd000bcf4a7ed8aa6f473c1adfecccae34ec24e823d156f4af0"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def download_source(destination: Path) -> None:
    request = urllib.request.Request(
        LIGHTGBM_URL,
        headers={"User-Agent": "snowflake-ml-jobs-distributed-sample/1.0"},
    )
    digest = hashlib.sha256()
    with urllib.request.urlopen(request, timeout=120) as response:
        with destination.open("wb") as output:
            while chunk := response.read(1024 * 1024):
                digest.update(chunk)
                output.write(chunk)
    if digest.hexdigest() != LIGHTGBM_SHA256:
        raise RuntimeError("The downloaded LightGBM source checksum did not match.")


def extract_source(archive: Path, destination: Path) -> Path:
    with tarfile.open(archive, "r:gz") as source_archive:
        destination_resolved = destination.resolve()
        for member in source_archive.getmembers():
            member_path = (destination / member.name).resolve()
            if destination_resolved not in member_path.parents and member_path != destination_resolved:
                raise RuntimeError(f"Unsafe path in LightGBM archive: {member.name}")
        source_archive.extractall(destination)
    source_dir = destination / f"lightgbm-{LIGHTGBM_VERSION}"
    if not (source_dir / "CMakeLists.txt").is_file():
        raise RuntimeError("The LightGBM source archive is incomplete.")
    return source_dir


def redirect_cli_output(source_dir: Path) -> None:
    """Keep the CLI binary separate from the sdist's Python package directory."""
    cmake_lists = source_dir / "CMakeLists.txt"
    original = "set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR})"
    replacement = 'set(EXECUTABLE_OUTPUT_PATH "${CMAKE_BINARY_DIR}/bin")'
    content = cmake_lists.read_text(encoding="utf-8")
    if content.count(original) != 1:
        raise RuntimeError(
            "The pinned LightGBM CMake executable output setting was not found."
        )
    cmake_lists.write_text(content.replace(original, replacement), encoding="utf-8")


def main() -> None:
    args = parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.output.is_file():
        print(f"[build] reusing {args.output}", flush=True)
        return

    mpicc = shutil.which("mpicc")
    mpicxx = shutil.which("mpicxx")
    cmake = shutil.which("cmake")
    ninja = shutil.which("ninja")
    missing = [
        name
        for name, path in (
            ("mpicc", mpicc),
            ("mpicxx", mpicxx),
            ("cmake", cmake),
            ("ninja", ninja),
        )
        if path is None
    ]
    if missing:
        raise RuntimeError("Missing build tools: " + ", ".join(missing))

    with tempfile.TemporaryDirectory(prefix="lightgbm-mpi-") as temp_dir:
        temp_path = Path(temp_dir)
        archive = temp_path / "lightgbm.tar.gz"
        download_source(archive)
        source_dir = extract_source(archive, temp_path / "source")
        build_dir = temp_path / "build"
        redirect_cli_output(source_dir)
        environment = {**os.environ, "CC": mpicc, "CXX": mpicxx}
        subprocess.run(
            [
                cmake,
                "-S",
                str(source_dir),
                "-B",
                str(build_dir),
                "-G",
                "Ninja",
                "-DCMAKE_BUILD_TYPE=Release",
                "-DUSE_MPI=ON",
                "-DBUILD_CLI=ON",
            ],
            env=environment,
            check=True,
        )
        subprocess.run(
            [cmake, "--build", str(build_dir), "--target", "lightgbm", "--parallel"],
            env=environment,
            check=True,
        )
        built_binary = build_dir / "bin" / "lightgbm"
        if not built_binary.is_file():
            raise RuntimeError(f"LightGBM build did not produce {built_binary}.")
        shutil.copy2(built_binary, args.output)
        args.output.chmod(0o755)
    print(f"[build] LightGBM {LIGHTGBM_VERSION} MPI CLI -> {args.output}", flush=True)


if __name__ == "__main__":
    main()
