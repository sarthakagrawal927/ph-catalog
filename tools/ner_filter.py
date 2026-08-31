#!/usr/bin/env python3
"""Aggregate GLiNER shards and apply the audited precision rules."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import duckdb

from ph_catalog.ner_rules import PRECISION_FILTER_VERSION, classify_entity


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parts-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--audit-output", type=Path)
    parser.add_argument("--audit-size", type=int, default=100)
    args = parser.parse_args()

    parts = sorted(args.parts_dir.glob("part-*.parquet"))
    if not parts:
        raise FileNotFoundError(f"no part-*.parquet files under {args.parts_dir}")
    args.output.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    parquet_list = (
        "["
        + ",".join(f"'{str(path.resolve()).replace(chr(39), chr(39) * 2)}'" for path in parts)
        + "]"
    )
    rows = con.execute(
        f"""
        WITH source AS (SELECT * FROM read_parquet({parquet_list})),
        support AS (
          SELECT label, lower(entity_text) entity_key, count(DISTINCT slug) support_products
          FROM source GROUP BY 1, 2
        )
        SELECT e.slug, e.entity_text, e.label, e.score, s.support_products
        FROM source e
        JOIN support s ON s.label=e.label AND s.entity_key=lower(e.entity_text)
        """
    ).fetchall()

    output = []
    seen = set()
    for slug, entity_text, label, score, support_products in rows:
        result = classify_entity(label, entity_text, support_products, score)
        if result is None:
            continue
        entity_type, canonical_value = result
        key = (slug, entity_type, canonical_value)
        if key in seen:
            continue
        seen.add(key)
        output.append(
            {
                "slug": slug,
                "entity_type": entity_type,
                "canonical_value": canonical_value,
                "source_span": entity_text,
                "score": float(score),
                "support_products": int(support_products),
                "method": f"gliner_small_v2_1+precision_filters_{PRECISION_FILTER_VERSION}",
            }
        )
    output.sort(key=lambda row: (row["slug"], row["entity_type"], row["canonical_value"]))

    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(
        pa.Table.from_pylist(output), args.output, compression="zstd", compression_level=19
    )
    report = {
        "raw_candidates": len(rows),
        "retained_assignments": len(output),
        "retained_products": len({row["slug"] for row in output}),
        "parts": len(parts),
        "precision_filter_version": PRECISION_FILTER_VERSION,
        "output": str(args.output),
    }
    print(json.dumps(report, indent=2))

    if args.audit_output:
        args.audit_output.parent.mkdir(parents=True, exist_ok=True)
        audit = sorted(
            output,
            key=lambda row: hashlib.sha256(
                f"ner-audit-v1|{row['slug']}|{row['entity_type']}|{row['canonical_value']}".encode()
            ).hexdigest(),
        )[: args.audit_size]
        args.audit_output.write_text(json.dumps(audit, indent=2) + "\n")


if __name__ == "__main__":
    main()
