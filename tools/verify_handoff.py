#!/usr/bin/env python3
"""Verify every release asset listed in a handoff manifest."""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

import orjson


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()

    manifest = orjson.loads(args.manifest.read_bytes())
    failures = []
    for asset in manifest["assets"]:
        path = args.directory / asset["name"]
        if not path.is_file():
            failures.append({"name": asset["name"], "error": "missing"})
            continue
        if path.stat().st_size != asset["bytes"]:
            failures.append({"name": asset["name"], "error": "size mismatch"})
            continue
        if sha256(path) != asset["sha256"]:
            failures.append({"name": asset["name"], "error": "sha256 mismatch"})

    if failures:
        print(orjson.dumps({"verified": False, "failures": failures}).decode())
        raise SystemExit(1)
    print(orjson.dumps({"verified": True, "assets": len(manifest["assets"])}).decode())


if __name__ == "__main__":
    main()
