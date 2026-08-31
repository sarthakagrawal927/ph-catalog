#!/usr/bin/env python3
"""Build the standalone internal analytics mart and a compact JSON summary."""

from __future__ import annotations

import argparse
from pathlib import Path

import orjson

from ph_catalog.analytics import build_analytics


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog-db", type=Path, required=True)
    parser.add_argument("--trusted-labels", type=Path, required=True)
    parser.add_argument("--entities", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--summary-output", type=Path, required=True)
    args = parser.parse_args()

    result = build_analytics(
        args.catalog_db,
        args.trusted_labels,
        args.output,
        args.summary_output,
        entities=args.entities,
    )
    print(orjson.dumps(result, option=orjson.OPT_INDENT_2).decode())


if __name__ == "__main__":
    main()
