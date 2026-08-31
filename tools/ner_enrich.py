#!/usr/bin/env python3
"""Resumable, sharded GLiNER inference over a compact catalogue file."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from pathlib import Path

import duckdb

from ph_catalog.ner_rules import ENTITY_LABELS, compact_name, is_self_name


def source_expression(path: Path) -> str:
    escaped = str(path.resolve()).replace("'", "''")
    if path.suffix.casefold() == ".parquet":
        return f"read_parquet('{escaped}')"
    if path.suffix.casefold() == ".csv":
        return f"read_csv_auto('{escaped}', header=true)"
    raise ValueError("--source must be a .parquet or extracted products.csv file")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def make_text(tagline: str, description: str) -> str:
    return "\n".join(
        part
        for part in (
            f"Tagline: {tagline}" if tagline else "",
            f"Description: {description}" if description else "",
        )
        if part
    )


def choose_device(torch: object, requested: str) -> str:
    if requested != "auto":
        return requested
    return "mps" if torch.backends.mps.is_available() else "cpu"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--model", default="urchade/gliner_small-v2.1")
    parser.add_argument("--device", choices=("auto", "cpu", "mps"), default="auto")
    parser.add_argument("--threshold", type=float, default=0.65)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--shard-size", type=int, default=10_000)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    if not args.source.is_file():
        raise FileNotFoundError(args.source)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parts_dir = args.output_dir / "parts"
    parts_dir.mkdir(exist_ok=True)

    os.environ.setdefault("PYTORCH_ENABLE_MPS_FALLBACK", "1")

    import pyarrow as pa
    import pyarrow.parquet as pq
    import torch
    from gliner import GLiNER

    expression = source_expression(args.source)
    con = duckdb.connect()
    relation = con.execute(
        f"""
        SELECT slug, coalesce(name, ''), coalesce(tagline, ''), coalesce(description, '')
        FROM {expression}
        WHERE length(trim(coalesce(tagline, '') || ' ' || coalesce(description, ''))) >= 40
        ORDER BY slug
        """
    )
    products = relation.fetchall()
    if args.limit is not None:
        products = products[: args.limit]

    manifest_path = args.output_dir / "manifest.json"
    manifest = {
        "source_filename": args.source.name,
        "source_sha256": sha256_file(args.source),
        "products": len(products),
        "model": args.model,
        "labels": list(ENTITY_LABELS),
        "threshold": args.threshold,
        "batch_size": args.batch_size,
        "shard_size": args.shard_size,
    }
    if manifest_path.exists():
        existing = json.loads(manifest_path.read_text())
        legacy_source = existing.pop("source", None)
        if legacy_source is not None:
            existing["source_filename"] = Path(legacy_source).name
        if existing != manifest:
            raise RuntimeError("existing output manifest differs; choose a new --output-dir")
    else:
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

    device = choose_device(torch, args.device)
    model = GLiNER.from_pretrained(args.model)
    model.to(device)
    model.eval()

    started = time.perf_counter()
    completed = 0
    for shard_start in range(0, len(products), args.shard_size):
        shard_number = shard_start // args.shard_size
        shard_path = parts_dir / f"part-{shard_number:06d}.parquet"
        shard = products[shard_start : shard_start + args.shard_size]
        if shard_path.exists():
            completed += len(shard)
            print(json.dumps({"event": "shard_skipped", "completed": completed}), flush=True)
            continue

        raw_rows = []
        for batch_start in range(0, len(shard), args.batch_size):
            batch = shard[batch_start : batch_start + args.batch_size]
            texts = [make_text(tagline, description) for _, _, tagline, description in batch]
            predictions = model.inference(
                texts,
                labels=list(ENTITY_LABELS),
                threshold=args.threshold,
                batch_size=args.batch_size,
            )
            for (slug, name, _, _), entities in zip(batch, predictions, strict=True):
                deduplicated = {}
                for entity in entities:
                    entity_text = str(entity["text"])
                    if (
                        is_self_name(entity_text, name)
                        or compact_name(entity_text) == "producthunt"
                    ):
                        continue
                    key = (compact_name(entity_text), str(entity["label"]).casefold())
                    if key not in deduplicated or entity["score"] > deduplicated[key]["score"]:
                        deduplicated[key] = entity
                for entity in deduplicated.values():
                    raw_rows.append(
                        {
                            "slug": slug,
                            "entity_text": str(entity["text"]),
                            "label": str(entity["label"]),
                            "score": float(entity["score"]),
                            "start": int(entity["start"]),
                            "end": int(entity["end"]),
                            "model": args.model,
                        }
                    )

        temporary = shard_path.with_suffix(".parquet.tmp")
        schema = pa.schema(
            [
                ("slug", pa.string()),
                ("entity_text", pa.string()),
                ("label", pa.string()),
                ("score", pa.float32()),
                ("start", pa.int32()),
                ("end", pa.int32()),
                ("model", pa.string()),
            ]
        )
        table = pa.Table.from_pylist(raw_rows, schema=schema)
        pq.write_table(table, temporary, compression="zstd", compression_level=19)
        temporary.replace(shard_path)
        completed += len(shard)
        elapsed = time.perf_counter() - started
        print(
            json.dumps(
                {
                    "event": "shard_complete",
                    "shard": shard_number,
                    "completed": completed,
                    "total": len(products),
                    "candidates": len(raw_rows),
                    "products_per_second": completed / elapsed if elapsed else None,
                }
            ),
            flush=True,
        )


if __name__ == "__main__":
    main()
