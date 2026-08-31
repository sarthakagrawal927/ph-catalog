from pathlib import Path
from subprocess import run

import orjson


def test_finalize_ner_audit_applies_exceptions_and_summarizes(tmp_path: Path) -> None:
    sample = tmp_path / "sample.json"
    review = tmp_path / "review.json"
    output = tmp_path / "output.json"
    sample.write_bytes(
        orjson.dumps(
            {
                "items": [
                    {"entity_type": "hardware", "audit_stratum": "random_assignment"},
                    {"entity_type": "hardware", "audit_stratum": "rare_support_lte_5"},
                ]
            }
        )
    )
    review.write_bytes(
        orjson.dumps(
            {
                "method": "manual",
                "reviewer": "test",
                "rules_frozen_before_sampling": True,
                "notes": "test",
                "default_verdict": {
                    "type_correct": True,
                    "product_relevant": True,
                    "review_note": "accepted",
                },
                "exceptions": {
                    "2": {
                        "type_correct": False,
                        "product_relevant": True,
                        "review_note": "wrong type",
                    }
                },
            }
        )
    )

    run(
        [
            "uv",
            "run",
            "python",
            "tools/finalize_ner_audit.py",
            "--sample",
            str(sample),
            "--review",
            str(review),
            "--output",
            str(output),
        ],
        check=True,
    )

    report = orjson.loads(output.read_bytes())
    assert report["summary"]["type_precision"] == 0.5
    assert report["summary"]["relevance_rate"] == 1.0
    assert report["items"][1]["review_note"] == "wrong type"
