#!/usr/bin/env python3
"""Write a portable checksum manifest for handoff release assets."""

from __future__ import annotations

import argparse
import hashlib
from datetime import UTC, datetime
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
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--release-tag", default="catalog-handoff-v1")
    parser.add_argument("files", nargs="+", type=Path)
    args = parser.parse_args()

    assets = []
    seen_names = set()
    for path in sorted((item.resolve() for item in args.files), key=lambda item: item.name):
        if not path.is_file():
            raise FileNotFoundError(path)
        if path.name in seen_names:
            raise ValueError(f"duplicate release filename: {path.name}")
        seen_names.add(path.name)
        assets.append({"name": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)})

    manifest = {
        "format_version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "release_tag": args.release_tag,
        "assets": assets,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(f"{args.output.suffix}.tmp")
    temporary.write_bytes(orjson.dumps(manifest, option=orjson.OPT_INDENT_2))
    temporary.replace(args.output)
    print(orjson.dumps(manifest, option=orjson.OPT_INDENT_2).decode())


if __name__ == "__main__":
    main()
