#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import os
import random
from pathlib import Path


def write_random_text_file(path: Path, size_bytes: int, chunk_raw_bytes: int = 256 * 1024) -> None:
    """
    Write a .txt file with printable ASCII content (base64-encoded random bytes)
    to an exact target size in bytes.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    remaining = size_bytes
    with path.open("wb") as f:
        while remaining > 0:
            raw = os.urandom(chunk_raw_bytes)
            txt = base64.b64encode(raw)  # ASCII bytes
            if len(txt) > remaining:
                f.write(txt[:remaining])
                remaining = 0
            else:
                f.write(txt)
                remaining -= len(txt)


def kb(n: int) -> int:
    return n * 1024


def mb(n: int) -> int:
    return n * 1024 * 1024


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate test .txt files with random small/large sizes.")
    p.add_argument("--root", default="data/source", help="Root directory on host (mounted to SFTP /upload). Default: data/source")
    p.add_argument("--subdir", default="a/b/c", help="Subdirectory under root. Default: a/b/c")

    p.add_argument("--small-count", type=int, default=5, help="Number of small files (~KB). Default: 5")
    p.add_argument("--small-min-kb", type=int, default=1, help="Min small file size in KB. Default: 1")
    p.add_argument("--small-max-kb", type=int, default=32, help="Max small file size in KB. Default: 32")

    p.add_argument("--large-count", type=int, default=1, help="Number of large files (~100MB). Default: 1")
    p.add_argument("--large-base-mb", type=int, default=100, help="Base large file size in MB. Default: 100")
    p.add_argument("--large-jitter-mb", type=int, default=5, help="Random jitter +/- MB around base. Default: 5")
    p.add_argument("--prefix", default="file", help="Filename prefix. Default: file")
    p.add_argument("--seed", type=int, default=None, help="Random seed for reproducible output")

    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    if args.small_min_kb <= 0 or args.small_max_kb < args.small_min_kb:
        raise ValueError("Invalid small file KB range.")
    if args.large_base_mb <= 0 or args.large_jitter_mb < 0:
        raise ValueError("Invalid large file MB settings.")

    base_dir = Path(args.root) / args.subdir
    base_dir.mkdir(parents=True, exist_ok=True)

    created = []

    # Small files (~KB)
    for i in range(1, args.small_count + 1):
        size_kb = random.randint(args.small_min_kb, args.small_max_kb)
        size_bytes = kb(size_kb)
        file_path = base_dir / f"{args.prefix}_small_{i:03d}_{size_kb}kb.txt"
        write_random_text_file(file_path, size_bytes)
        created.append((str(file_path), size_bytes))

    # Large files (~100MB with jitter)
    for i in range(1, args.large_count + 1):
        jitter = random.randint(-args.large_jitter_mb, args.large_jitter_mb)
        size_mb = max(1, args.large_base_mb + jitter)
        size_bytes = mb(size_mb)
        file_path = base_dir / f"{args.prefix}_large_{i:03d}_{size_mb}mb.txt"
        write_random_text_file(file_path, size_bytes)
        created.append((str(file_path), size_bytes))

    print("Created files:")
    for path, size_bytes in created:
        print(f"- {path} ({size_bytes} bytes)")


if __name__ == "__main__":
    main()