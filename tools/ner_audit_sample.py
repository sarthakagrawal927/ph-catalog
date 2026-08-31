#!/usr/bin/env python3
"""Create a deterministic, type-stratified entity audit sample with context."""

from __future__ import annotations

import argparse
import hashlib
from collections import defaultdict
from pathlib import Path

import duckdb
import orjson


def _escaped(path: Path) -> str:
    return str(path.resolve()).replace("'", "''")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--entities", type=Path, required=True)
    parser.add_argument("--products", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--per-type", type=int, default=10)
    parser.add_argument("--rare-per-type", type=int, default=5)
    parser.add_argument("--seed", default="fresh-stratified-audit-v1")
    args = parser.parse_args()
    if args.per_type < 1 or args.rare_per_type < 0:
        parser.error("--per-type must be positive and --rare-per-type cannot be negative")

    connection = duckdb.connect()
    rows = connection.execute(
        f"""
        SELECT e.slug, e.entity_type, e.canonical_value, e.source_span, e.score,
               e.support_products, e.method, p.name, p.tagline, p.description
        FROM read_parquet('{_escaped(args.entities)}') e
        JOIN read_parquet('{_escaped(args.products)}') p USING (slug)
        """
    ).fetchall()
    connection.close()

    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        (
            slug,
            entity_type,
            canonical_value,
            source_span,
            score,
            support_products,
            method,
            name,
            tagline,
            description,
        ) = row
        grouped[entity_type].append(
            {
                "slug": slug,
                "entity_type": entity_type,
                "canonical_value": canonical_value,
                "source_span": source_span,
                "score": float(score),
                "support_products": int(support_products),
                "method": method,
                "name": name,
                "tagline": tagline,
                "description": description,
                "type_correct": None,
                "product_relevant": None,
                "review_note": None,
            }
        )

    selected = []
    for entity_type in sorted(grouped):
        candidates = sorted(
            grouped[entity_type],
            key=lambda item: hashlib.sha256(
                (
                    f"{args.seed}|{item['slug']}|{item['entity_type']}|"
                    f"{item['canonical_value']}"
                ).encode()
            ).hexdigest(),
        )
        random_items = candidates[: args.per_type]
        for item in random_items:
            item["audit_stratum"] = "random_assignment"
        selected.extend(random_items)

        selected_keys = {
            (item["slug"], item["entity_type"], item["canonical_value"])
            for item in random_items
        }
        rare_candidates = sorted(
            (
                item
                for item in grouped[entity_type]
                if item["support_products"] <= 5
                and (item["slug"], item["entity_type"], item["canonical_value"])
                not in selected_keys
            ),
            key=lambda item: hashlib.sha256(
                (
                    f"{args.seed}|rare|{item['slug']}|{item['entity_type']}|"
                    f"{item['canonical_value']}"
                ).encode()
            ).hexdigest(),
        )[: args.rare_per_type]
        for item in rare_candidates:
            item["audit_stratum"] = "rare_support_lte_5"
        selected.extend(rare_candidates)

    report = {
        "sample_method": "deterministic SHA-256 sample stratified by entity_type",
        "seed": args.seed,
        "per_type": args.per_type,
        "rare_per_type": args.rare_per_type,
        "sample_size": len(selected),
        "available_by_type": {
            entity_type: len(candidates) for entity_type, candidates in sorted(grouped.items())
        },
        "items": selected,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(f"{args.output.suffix}.tmp")
    temporary.write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2))
    temporary.replace(args.output)
    print(orjson.dumps({key: value for key, value in report.items() if key != "items"}).decode())


if __name__ == "__main__":
    main()
