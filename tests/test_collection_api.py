from pathlib import Path

import pytest

from ph_catalog.collection_api import (
    CollectionRecord,
    CollectionScanConfig,
    CollectionScanner,
    CollectionSummary,
)
from ph_catalog.database import CatalogDatabase


def test_collection_cursor_uses_product_hunt_offset_encoding() -> None:
    assert CollectionScanner._cursor(1) == "MTAw"
    assert CollectionScanner._offset_cursor(250) == "MjUw"


def test_collection_config_rejects_invalid_workers() -> None:
    with pytest.raises(ValueError, match="collection workers"):
        CollectionScanConfig(workers=9)


def test_collection_batches_resume_and_deduplicate_products(tmp_path: Path) -> None:
    with CatalogDatabase(tmp_path / "catalog.duckdb") as database:
        database.prepare_collection_scan("public-collections-v1", 2, 1)
        added = database.apply_collection_page_batch(
            "public-collections-v1",
            [
                (
                    0,
                    [
                        CollectionSummary(
                            50,
                            150,
                            (
                                CollectionRecord(7, "demo"),
                                CollectionRecord(8, "second"),
                            ),
                        ),
                        CollectionSummary(
                            51, 1, (CollectionRecord(7, "demo"),)
                        ),
                    ],
                )
            ],
        )

        assert added == 2
        assert database.pending_collection_pages("public-collections-v1", 10) == []
        assert database.pending_collection_overflow(10) == [(50, 100, 150)]

        overflow_added = database.apply_collection_overflow_batch(
            [(50, 100, [CollectionRecord(8, "second"), CollectionRecord(9, "third")])]
        )
        assert overflow_added == 1
        assert database.pending_collection_overflow(10) == []
        assert database.collection_counts() == {
            "unique_candidates": 3,
            "new_product_ids": 3,
            "collections_reported": 2,
            "root_pages_scanned": 1,
            "collections_scanned": 2,
            "memberships_seen": 5,
            "reported_memberships": 151,
            "scans_complete": 1,
            "overflow_collections": 1,
            "overflow_complete": 1,
            "overflow_pages_scanned": 1,
        }
