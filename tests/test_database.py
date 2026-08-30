from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

from ph_catalog.database import CatalogDatabase
from ph_catalog.models import (
    CatalogRecord,
    FetchResult,
    ProductData,
    ProductStatus,
    SitemapProduct,
)
from ph_catalog.parsing import content_hash


def product(slug: str) -> SitemapProduct:
    return SitemapProduct(slug, f"https://www.producthunt.com/products/{slug}")


def fetched(requested: str, canonical: str | None = None) -> FetchResult:
    canonical = canonical or requested
    url = f"https://www.producthunt.com/products/{canonical}"
    data = ProductData(
        slug=canonical,
        producthunt_url=url,
        name=canonical.title(),
        tagline="Tagline",
        description="Description",
        website_url=f"https://{canonical}.example",
        categories=["Developer Tools"],
    )
    return FetchResult(
        requested_slug=requested,
        canonical_slug=canonical,
        requested_url=f"https://www.producthunt.com/products/{requested}",
        canonical_url=url,
        attempt=1,
        status=ProductStatus.FETCHED,
        http_status=200,
        proxy_label="direct",
        elapsed_ms=10,
        data=data,
        content_hash="abc",
    )


def test_repeated_import_is_deduplicated_and_fetched_rows_are_skipped(tmp_path: Path) -> None:
    with CatalogDatabase(tmp_path / "catalog.duckdb") as database:
        assert database.import_products([product("alpha"), product("alpha")]) == (1, 2)
        assert database.import_products([product("alpha")]) == (0, 1)
        claimed = database.claim_products(10, 8)
        assert len(claimed) == 1
        database.apply_results([fetched("alpha")])
        assert database.claim_products(10, 8) == []
        assert database.duplicate_counts() == {"slug": 0, "producthunt_url": 0}


def test_interrupted_claim_is_recovered_without_consuming_an_attempt(tmp_path: Path) -> None:
    path = tmp_path / "catalog.duckdb"
    with CatalogDatabase(path) as database:
        database.import_products([product("alpha")])
        assert database.claim_products(1, 8)[0].attempt == 1

    with CatalogDatabase(path) as database:
        resumed = database.claim_products(1, 8)
        assert [(row.slug, row.attempt) for row in resumed] == [("alpha", 1)]


def test_redirect_moves_original_slug_to_alias_table(tmp_path: Path) -> None:
    with CatalogDatabase(tmp_path / "catalog.duckdb") as database:
        database.import_products([product("old"), product("canonical")])
        database.claim_products(10, 8)
        database.apply_results([fetched("old", "canonical")])
        database.import_products([product("old")])
        rows = database.connection.execute("SELECT slug FROM products ORDER BY slug").fetchall()
        alias = database.connection.execute(
            "SELECT alias_slug, canonical_slug FROM product_aliases"
        ).fetchone()
        assert rows == [("canonical",)]
        assert alias == ("old", "canonical")


def test_snapshot_contains_only_catalogue_fields(tmp_path: Path) -> None:
    db_path = tmp_path / "catalog.duckdb"
    output = tmp_path / "snapshot.parquet"
    with CatalogDatabase(db_path) as database:
        database.import_products([product("alpha")])
        database.claim_products(10, 8)
        database.apply_results([fetched("alpha")])
        assert database.snapshot(output) == 1

    connection = duckdb.connect()
    columns = [row[0] for row in connection.execute(f"DESCRIBE '{output}'").fetchall()]
    assert columns == [
        "slug",
        "name",
        "tagline",
        "description",
        "website_url",
        "categories",
        "producthunt_url",
    ]


def test_restore_snapshot_seeds_fetched_rows_idempotently(tmp_path: Path) -> None:
    source_path = tmp_path / "source.duckdb"
    snapshot = tmp_path / "snapshot.parquet"
    with CatalogDatabase(source_path) as source:
        source.import_products([product("alpha")])
        source.claim_products(10, 8)
        source.apply_results([fetched("alpha")])
        source.snapshot(snapshot)

    with CatalogDatabase(tmp_path / "restored.duckdb") as restored:
        assert restored.restore_snapshot(snapshot) == 1
        assert restored.restore_snapshot(snapshot) == 0
        assert restored.connection.execute(
            "SELECT slug, status, attempts, http_status, tagline_source "
            "FROM products"
        ).fetchone() == ("alpha", "fetched", 0, 200, "snapshot")


def test_website_enrichment_fills_only_missing_fields_with_provenance(
    tmp_path: Path,
) -> None:
    with CatalogDatabase(tmp_path / "catalog.duckdb") as database:
        database.import_products([product("alpha")])
        database.claim_products(1, 8)
        database.apply_results([fetched("alpha")])
        database.connection.execute(
            "UPDATE products SET tagline = '', description = '' WHERE slug = 'alpha'"
        )

        assert database.claim_website_enrichments(10, 3) == [
            (
                "alpha",
                "Alpha",
                "https://alpha.example",
                1,
                True,
                True,
            )
        ]
        result = SimpleNamespace(
            slug="alpha",
            requested_url="https://alpha.example",
            final_url="https://www.alpha.example/",
            attempt=1,
            outcome="enriched",
            http_status=200,
            tagline="A careful website-derived tagline",
            tagline_source="title",
            description="A detailed website-derived description for this test product.",
            description_source="og_description",
            response_hash="response-hash",
            error=None,
        )
        assert database.apply_website_enrichments([result], apply_fields=False) == {
            "processed": 1,
            "taglines_applied": 0,
            "descriptions_applied": 0,
        }
        assert database.connection.execute(
            "SELECT tagline, description FROM products WHERE slug = 'alpha'"
        ).fetchone() == ("", "")
        assert database.apply_stored_website_enrichments(["alpha"]) == {
            "processed": 1,
            "taglines_applied": 1,
            "descriptions_applied": 1,
        }
        enriched = database.connection.execute(
            """
            SELECT tagline, tagline_source, description, description_source,
                   content_hash
            FROM products WHERE slug = 'alpha'
            """
        ).fetchone()
        assert enriched[:4] == (
            "A careful website-derived tagline",
            "website:title",
            "A detailed website-derived description for this test product.",
            "website:og_description",
        )
        assert enriched[4]
        assert database.claim_website_enrichments(10, 3) == []
        assert database.website_enrichment_counts() == {
            "enriched": 1,
            "taglines_applied": 1,
            "descriptions_applied": 1,
        }


def test_incomplete_copy_products_are_quarantined_and_not_reimported(
    tmp_path: Path,
) -> None:
    with CatalogDatabase(tmp_path / "catalog.duckdb") as database:
        database.import_products([product("alpha")])
        database.claim_products(1, 8)
        database.apply_results([fetched("alpha")])
        database.connection.execute("UPDATE products SET tagline = '' WHERE slug = 'alpha'")
        database.connection.execute(
            """
            INSERT INTO product_aliases (
                alias_slug, canonical_slug, alias_url, canonical_url
            ) VALUES (
                'old-alpha', 'alpha',
                'https://www.producthunt.com/products/old-alpha',
                'https://www.producthunt.com/products/alpha'
            )
            """
        )

        assert database.exclude_incomplete_copy_products() == {
            "excluded_products": 1,
            "excluded_aliases": 1,
        }
        assert database.total_products() == 0
        assert database.exclusion_counts() == {"products": 1, "aliases": 1}
        assert database.connection.execute(
            "SELECT slug, exclusion_reason FROM excluded_products"
        ).fetchone() == (
            "alpha",
            "missing tagline or description after website enrichment",
        )
        assert database.import_products([product("alpha")]) == (0, 1)
        assert database.exclude_incomplete_copy_products() == {
            "excluded_products": 0,
            "excluded_aliases": 0,
        }


def test_catalog_batch_and_cursor_commit_atomically(tmp_path: Path) -> None:
    with CatalogDatabase(tmp_path / "catalog.duckdb") as database:
        state = database.prepare_manifest_scan("test", 100, 200)
        assert state["next_id"] == 100
        data = ProductData(
            slug="alpha",
            producthunt_url="https://www.producthunt.com/products/alpha",
            name="Alpha",
            tagline="The first",
            description="A complete Product Hunt product.",
            website_url="https://alpha.example",
            categories=["Developer Tools"],
        )
        database.apply_catalog_batch(
            "test", 100, 110, [CatalogRecord(105, data, content_hash(data))], 9
        )
        assert database.manifest_scan_state("test") == {
            "next_id": 110,
            "stop_id": 200,
            "requests": 1,
            "discovered": 1,
            "unavailable": 9,
        }
        assert database.counts()["fetched"] == 1
        assert database.connection.execute(
            "SELECT product_id, slug FROM source_products"
        ).fetchall() == [(105, "alpha")]

        # Replaying the same source record under the next cursor remains deduplicated.
        database.apply_catalog_batch(
            "test", 110, 120, [CatalogRecord(105, data, content_hash(data))], 9
        )
        assert database.total_products() == 1


def test_search_batch_bulk_imports_only_new_source_ids(tmp_path: Path) -> None:
    with CatalogDatabase(tmp_path / "catalog.duckdb") as database:
        database.prepare_search_scan("a", 2)
        database.connection.execute(
            "INSERT INTO source_products (product_id, slug) VALUES (1, 'known')"
        )
        records = [
            SimpleNamespace(product_id=1, slug="known", name="Known", tagline="Old"),
            SimpleNamespace(product_id=2, slug="new", name="New", tagline="Fresh"),
        ]
        database.apply_search_batch("a", 1, 3, records, 2)

        assert database.search_counts() == {
            "unique_candidates": 2,
            "new_product_ids": 1,
            "queries": 1,
            "queries_complete": 1,
            "pages_scanned": 2,
        }
        assert database.connection.execute(
            "SELECT product_id, slug FROM source_products ORDER BY product_id"
        ).fetchall() == [(1, "known"), (2, "new")]
        assert database.connection.execute(
            "SELECT slug, name, tagline FROM products"
        ).fetchall() == [("new", "New", "Fresh")]


def test_post_id_batch_and_cursor_commit_atomically(tmp_path: Path) -> None:
    with CatalogDatabase(tmp_path / "catalog.duckdb") as database:
        database.prepare_manifest_scan("post-scan", 10, 13)
        resolutions = [
            SimpleNamespace(
                post_slug="10",
                post_id=10,
                product_id=99,
                product_slug="archived-product",
                outcome="resolved",
            ),
            SimpleNamespace(
                post_slug="11",
                post_id=11,
                product_id=None,
                product_slug=None,
                outcome="no_product",
            ),
            SimpleNamespace(
                post_slug="12",
                post_id=None,
                product_id=None,
                product_slug=None,
                outcome="unavailable",
            ),
        ]

        assert database.apply_post_id_batch("post-scan", 10, 13, resolutions) == 1
        assert database.manifest_scan_state("post-scan") == {
            "next_id": 13,
            "stop_id": 13,
            "requests": 1,
            "discovered": 1,
            "unavailable": 2,
        }
        assert database.connection.execute(
            "SELECT product_id, slug FROM source_products"
        ).fetchall() == [(99, "archived-product")]

        with pytest.raises(ValueError, match="cursor changed"):
            database.apply_post_id_batch("post-scan", 10, 13, resolutions)


def test_archive_recovery_claim_and_apply_are_resumable(tmp_path: Path) -> None:
    with CatalogDatabase(tmp_path / "catalog.duckdb") as database:
        database.apply_archive_scan(
            "wayback-products", "products", {"recover-me", "no-capture"}, 2
        )
        database.connection.execute("UPDATE products SET status = 'unavailable'")

        claimed = database.claim_archive_recoveries(10, 3)
        assert claimed == [("no-capture", 1), ("recover-me", 1)]

        data = ProductData(
            slug="recover-me",
            producthunt_url="https://www.producthunt.com/products/recover-me",
            name="Recovered",
            tagline="Preserved tagline",
            description="Preserved archived description.",
            website_url="https://recovered.example",
            categories=["Developer Tools"],
        )
        database.apply_archive_recoveries(
            [
                SimpleNamespace(
                    slug="no-capture",
                    attempt=1,
                    outcome="no_capture",
                    capture_timestamp=None,
                    http_status=None,
                    error=None,
                    data=None,
                    content_hash=None,
                ),
                SimpleNamespace(
                    slug="recover-me",
                    attempt=1,
                    outcome="recovered",
                    capture_timestamp="20240102030405",
                    http_status=200,
                    error=None,
                    data=data,
                    content_hash=content_hash(data),
                ),
            ]
        )

        assert database.connection.execute(
            "SELECT slug, status FROM products ORDER BY slug"
        ).fetchall() == [("no-capture", "unavailable"), ("recover-me", "fetched")]
        assert database.claim_archive_recoveries(10, 3) == []
        assert database.archive_recovery_counts() == {
            "no_capture": 1,
            "recovered": 1,
            "eligible_unavailable": 1,
        }
        assert database.requeue_archive_recoveries(["no_capture"]) == 1
        assert database.claim_archive_recoveries(10, 3) == [("no-capture", 1)]


def test_archived_post_recovery_adds_canonical_product(tmp_path: Path) -> None:
    with CatalogDatabase(tmp_path / "catalog.duckdb") as database:
        database.apply_archive_scan("wayback-posts", "posts", {"old-launch"}, 1)
        database.connection.execute(
            "INSERT INTO archive_post_resolutions (post_slug, outcome) "
            "VALUES ('old-launch', 'unavailable')"
        )
        assert database.claim_archive_post_recoveries(10, 3) == [("old-launch", 1)]

        data = ProductData(
            slug="canonical-product",
            producthunt_url="https://www.producthunt.com/products/canonical-product",
            name="Canonical Product",
            tagline="Recovered from its launch",
            description="A complete archived product record.",
            website_url="https://canonical.example",
            categories=["Productivity"],
        )
        added = database.apply_archive_post_recoveries(
            [
                SimpleNamespace(
                    post_slug="old-launch",
                    attempt=1,
                    outcome="recovered",
                    capture_timestamp="20220102030405",
                    http_status=200,
                    error=None,
                    data=data,
                    content_hash=content_hash(data),
                )
            ]
        )

        assert added == 1
        assert database.connection.execute(
            "SELECT slug, status FROM products"
        ).fetchall() == [("canonical-product", "fetched")]
        assert database.connection.execute(
            "SELECT outcome, product_slug FROM archive_post_resolutions"
        ).fetchall() == [("resolved", "canonical-product")]
