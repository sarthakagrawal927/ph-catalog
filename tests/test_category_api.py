import pytest

from ph_catalog.category_api import CategoryRecord, CategoryScanConfig, CategoryScanner
from ph_catalog.database import CatalogDatabase


def test_category_page_discovers_unique_slugs() -> None:
    html = b"""
    <a href="/categories/ai-agents">AI Agents</a>
    <a href="/categories/ai-agents?ref=footer">Duplicate</a>
    <a href="/categories/code-review-tools">Code review</a>
    <a href="/products/not-a-category">Product</a>
    """

    assert CategoryScanner._parse_category_slugs(html) == [
        "ai-agents",
        "code-review-tools",
    ]


def test_category_scan_config_rejects_oversized_page_batches() -> None:
    with pytest.raises(ValueError, match="category page batch size"):
        CategoryScanConfig(page_batch_size=26)


def test_category_batch_advances_cursors_and_deduplicates_products(tmp_path) -> None:
    with CatalogDatabase(tmp_path / "catalog.duckdb") as database:
        database.prepare_category_scan("ai-agents", 2, 1)
        database.prepare_category_scan("automation", 2, 1)

        added = database.apply_category_page_batch(
            [
                (
                    "ai-agents",
                    1,
                    [CategoryRecord(7, "demo"), CategoryRecord(8, "second")],
                ),
                (
                    "automation",
                    1,
                    [CategoryRecord(7, "demo"), CategoryRecord(8, "second")],
                ),
            ]
        )

        assert added == 2
        assert database.category_counts() == {
            "memberships": 4,
            "unique_candidates": 2,
            "new_product_ids": 2,
            "categories": 2,
            "categories_complete": 2,
            "pages_scanned": 2,
            "reported_memberships": 4,
        }
        assert database.pending_category_pages(10) == []
        assert database.connection.execute(
            "SELECT product_id, slug FROM source_products ORDER BY product_id"
        ).fetchall() == [(7, "demo"), (8, "second")]
