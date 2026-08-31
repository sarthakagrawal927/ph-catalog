#!/usr/bin/env python3
"""Apply an explicit manual review file and summarize NER audit evidence."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from pathlib import Path

import orjson


def _rate(correct: int, total: int) -> float | None:
    return correct / total if total else None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=Path, required=True)
    parser.add_argument("--review", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    report = orjson.loads(args.sample.read_bytes())
    review = orjson.loads(args.review.read_bytes())
    exceptions = {int(index): verdict for index, verdict in review["exceptions"].items()}
    items = report["items"]

    invalid = sorted(set(exceptions) - set(range(1, len(items) + 1)))
    if invalid:
        raise ValueError(f"review contains out-of-range item indexes: {invalid}")

    for index, item in enumerate(items, start=1):
        verdict = exceptions.get(index, review["default_verdict"])
        item["type_correct"] = bool(verdict["type_correct"])
        item["product_relevant"] = bool(verdict["product_relevant"])
        item["review_note"] = verdict["review_note"]

    totals = Counter()
    by_type: dict[str, Counter[str]] = defaultdict(Counter)
    by_stratum: dict[str, Counter[str]] = defaultdict(Counter)
    for item in items:
        totals["items"] += 1
        totals["type_correct"] += item["type_correct"]
        totals["product_relevant"] += item["product_relevant"]
        for grouping, key in (
            (by_type, item["entity_type"]),
            (by_stratum, item["audit_stratum"]),
        ):
            grouping[key]["items"] += 1
            grouping[key]["type_correct"] += item["type_correct"]
            grouping[key]["product_relevant"] += item["product_relevant"]

    def summarized(groups: dict[str, Counter[str]]) -> dict[str, dict[str, int | float | None]]:
        return {
            key: {
                **counts,
                "type_precision": _rate(counts["type_correct"], counts["items"]),
                "relevance_rate": _rate(counts["product_relevant"], counts["items"]),
            }
            for key, counts in sorted(groups.items())
        }

    report["review"] = {
        "method": review["method"],
        "reviewer": review["reviewer"],
        "rules_frozen_before_sampling": bool(review["rules_frozen_before_sampling"]),
        "notes": review["notes"],
    }
    report["summary"] = {
        **totals,
        "type_precision": _rate(totals["type_correct"], totals["items"]),
        "relevance_rate": _rate(totals["product_relevant"], totals["items"]),
        "by_entity_type": summarized(by_type),
        "by_audit_stratum": summarized(by_stratum),
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(f"{args.output.suffix}.tmp")
    temporary.write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2))
    temporary.replace(args.output)
    print(orjson.dumps(report["summary"], option=orjson.OPT_INDENT_2).decode())


if __name__ == "__main__":
    main()
