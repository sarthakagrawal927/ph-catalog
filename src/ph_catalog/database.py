from __future__ import annotations

from collections.abc import Iterable, Sequence
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Protocol

import duckdb
import orjson

from ph_catalog.models import (
    CatalogRecord,
    FetchResult,
    PendingProduct,
    ProductData,
    ProductStatus,
    SitemapProduct,
)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS products (
    slug VARCHAR PRIMARY KEY,
    producthunt_url VARCHAR NOT NULL UNIQUE,
    name VARCHAR,
    tagline VARCHAR,
    description VARCHAR,
    website_url VARCHAR,
    categories VARCHAR[] DEFAULT [],
    status VARCHAR NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'fetched', 'retry', 'unavailable', 'parse_failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    http_status INTEGER,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    fetched_at TIMESTAMPTZ,
    content_hash VARCHAR,
    source_lastmod TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ,
    last_error VARCHAR
);

CREATE TABLE IF NOT EXISTS product_aliases (
    alias_slug VARCHAR PRIMARY KEY,
    canonical_slug VARCHAR NOT NULL,
    alias_url VARCHAR NOT NULL UNIQUE,
    canonical_url VARCHAR NOT NULL,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    http_status INTEGER
);

CREATE TABLE IF NOT EXISTS crawl_runs (
    run_id UUID PRIMARY KEY,
    kind VARCHAR NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    finished_at TIMESTAMPTZ,
    outcome VARCHAR,
    metrics JSON
);

CREATE TABLE IF NOT EXISTS crawl_errors (
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    slug VARCHAR NOT NULL,
    attempt INTEGER NOT NULL,
    http_status INTEGER,
    error VARCHAR NOT NULL,
    proxy_label VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS source_products (
    product_id BIGINT PRIMARY KEY,
    slug VARCHAR NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS manifest_scans (
    scan_name VARCHAR PRIMARY KEY,
    next_id BIGINT NOT NULL,
    stop_id BIGINT NOT NULL,
    requests BIGINT NOT NULL DEFAULT 0,
    discovered BIGINT NOT NULL DEFAULT 0,
    unavailable BIGINT NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS archive_candidates (
    path_kind VARCHAR NOT NULL CHECK (path_kind IN ('products', 'posts')),
    slug VARCHAR NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (path_kind, slug)
);

CREATE TABLE IF NOT EXISTS archive_scans (
    index_id VARCHAR NOT NULL,
    path_kind VARCHAR NOT NULL CHECK (path_kind IN ('products', 'posts')),
    captures BIGINT NOT NULL,
    unique_candidates BIGINT NOT NULL,
    malformed_lines BIGINT NOT NULL DEFAULT 0,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (index_id, path_kind)
);

ALTER TABLE archive_scans ADD COLUMN IF NOT EXISTS malformed_lines BIGINT DEFAULT 0;

CREATE TABLE IF NOT EXISTS wayback_scans (
    path_kind VARCHAR PRIMARY KEY CHECK (path_kind IN ('products', 'posts')),
    resume_key VARCHAR,
    requests BIGINT NOT NULL DEFAULT 0,
    captures BIGINT NOT NULL DEFAULT 0,
    complete BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS archive_recoveries (
    slug VARCHAR PRIMARY KEY,
    outcome VARCHAR NOT NULL
        CHECK (outcome IN ('claimed', 'recovered', 'no_capture', 'parse_failed', 'retry')),
    attempts INTEGER NOT NULL DEFAULT 0,
    capture_timestamp VARCHAR,
    http_status INTEGER,
    last_error VARCHAR,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS common_crawl_recovery_scans (
    crawl_id VARCHAR PRIMARY KEY,
    cdx_rows BIGINT NOT NULL,
    matched_slugs BIGINT NOT NULL,
    transferred_bytes BIGINT NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS common_crawl_captures (
    slug VARCHAR NOT NULL,
    crawl_id VARCHAR NOT NULL,
    capture_timestamp VARCHAR NOT NULL,
    url VARCHAR NOT NULL,
    filename VARCHAR NOT NULL,
    byte_offset BIGINT NOT NULL,
    byte_length BIGINT NOT NULL,
    mime VARCHAR,
    outcome VARCHAR NOT NULL DEFAULT 'pending'
        CHECK (outcome IN ('pending', 'recovered', 'parse_failed', 'fetch_failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    http_status INTEGER,
    last_error VARCHAR,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (slug, crawl_id)
);

CREATE TABLE IF NOT EXISTS common_crawl_post_recovery_scans (
    crawl_id VARCHAR PRIMARY KEY,
    cdx_rows BIGINT NOT NULL,
    matched_slugs BIGINT NOT NULL,
    transferred_bytes BIGINT NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS common_crawl_post_captures (
    post_slug VARCHAR NOT NULL,
    crawl_id VARCHAR NOT NULL,
    capture_timestamp VARCHAR NOT NULL,
    url VARCHAR NOT NULL,
    filename VARCHAR NOT NULL,
    byte_offset BIGINT NOT NULL,
    byte_length BIGINT NOT NULL,
    mime VARCHAR,
    outcome VARCHAR NOT NULL DEFAULT 'pending'
        CHECK (outcome IN ('pending', 'recovered', 'parse_failed', 'fetch_failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    http_status INTEGER,
    last_error VARCHAR,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (post_slug, crawl_id)
);

CREATE TABLE IF NOT EXISTS archive_post_recoveries (
    post_slug VARCHAR PRIMARY KEY,
    outcome VARCHAR NOT NULL
        CHECK (outcome IN ('claimed', 'recovered', 'no_capture', 'parse_failed', 'retry')),
    attempts INTEGER NOT NULL DEFAULT 0,
    capture_timestamp VARCHAR,
    product_slug VARCHAR,
    http_status INTEGER,
    last_error VARCHAR,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS archive_post_resolutions (
    post_slug VARCHAR PRIMARY KEY,
    post_id BIGINT,
    product_id BIGINT,
    product_slug VARCHAR,
    outcome VARCHAR NOT NULL CHECK (outcome IN ('resolved', 'no_product', 'unavailable')),
    resolved_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS search_candidates (
    product_id BIGINT PRIMARY KEY,
    slug VARCHAR NOT NULL,
    name VARCHAR,
    tagline VARCHAR,
    new_product_id BOOLEAN NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS search_scans (
    query VARCHAR PRIMARY KEY,
    pages_count INTEGER NOT NULL,
    next_page INTEGER NOT NULL DEFAULT 1,
    requests BIGINT NOT NULL DEFAULT 0,
    pages_scanned BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS category_candidates (
    category_slug VARCHAR NOT NULL,
    product_id BIGINT NOT NULL,
    slug VARCHAR NOT NULL,
    new_product_id BOOLEAN NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (category_slug, product_id)
);

CREATE TABLE IF NOT EXISTS category_scans (
    category_slug VARCHAR PRIMARY KEY,
    total_count INTEGER NOT NULL,
    pages_count INTEGER NOT NULL,
    next_page INTEGER NOT NULL DEFAULT 1,
    pages_scanned BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS collection_candidates (
    product_id BIGINT PRIMARY KEY,
    slug VARCHAR NOT NULL,
    new_product_id BOOLEAN NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS collection_scans (
    scan_name VARCHAR PRIMARY KEY,
    total_count INTEGER NOT NULL,
    pages_count INTEGER NOT NULL,
    next_page INTEGER NOT NULL DEFAULT 0,
    pages_scanned BIGINT NOT NULL DEFAULT 0,
    collections_seen BIGINT NOT NULL DEFAULT 0,
    memberships_seen BIGINT NOT NULL DEFAULT 0,
    reported_memberships BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS collection_overflow_scans (
    collection_id BIGINT PRIMARY KEY,
    products_count INTEGER NOT NULL,
    next_offset INTEGER NOT NULL DEFAULT 100,
    pages_scanned BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS external_products (
    slug VARCHAR PRIMARY KEY,
    source VARCHAR NOT NULL,
    name VARCHAR,
    tagline VARCHAR,
    description VARCHAR,
    website_url VARCHAR,
    categories VARCHAR[],
    producthunt_url VARCHAR NOT NULL,
    product_id BIGINT,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

ALTER TABLE products ADD COLUMN IF NOT EXISTS tagline_source VARCHAR;
ALTER TABLE products ADD COLUMN IF NOT EXISTS description_source VARCHAR;

UPDATE products SET tagline_source = 'producthunt'
WHERE tagline_source IS NULL AND tagline IS NOT NULL AND trim(tagline) <> '';

UPDATE products SET description_source = 'producthunt'
WHERE description_source IS NULL AND description IS NOT NULL AND trim(description) <> '';

CREATE TABLE IF NOT EXISTS website_enrichments (
    slug VARCHAR PRIMARY KEY,
    requested_url VARCHAR NOT NULL,
    final_url VARCHAR,
    outcome VARCHAR NOT NULL DEFAULT 'claimed'
        CHECK (outcome IN (
            'claimed', 'enriched', 'no_metadata', 'invalid_url', 'unavailable',
            'blocked', 'non_html', 'too_large', 'retry'
        )),
    attempts INTEGER NOT NULL DEFAULT 0,
    http_status INTEGER,
    tagline_candidate VARCHAR,
    tagline_source VARCHAR,
    description_candidate VARCHAR,
    description_source VARCHAR,
    applied_tagline BOOLEAN NOT NULL DEFAULT false,
    applied_description BOOLEAN NOT NULL DEFAULT false,
    response_hash VARCHAR,
    last_error VARCHAR,
    fetched_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS excluded_products (
    slug VARCHAR PRIMARY KEY,
    producthunt_url VARCHAR NOT NULL,
    name VARCHAR,
    tagline VARCHAR,
    description VARCHAR,
    website_url VARCHAR,
    categories VARCHAR[],
    status VARCHAR NOT NULL,
    attempts INTEGER NOT NULL,
    http_status INTEGER,
    first_seen_at TIMESTAMPTZ NOT NULL,
    fetched_at TIMESTAMPTZ,
    content_hash VARCHAR,
    source_lastmod TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ,
    last_error VARCHAR,
    tagline_source VARCHAR,
    description_source VARCHAR,
    exclusion_reason VARCHAR NOT NULL,
    excluded_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS excluded_product_aliases (
    alias_slug VARCHAR PRIMARY KEY,
    canonical_slug VARCHAR NOT NULL,
    alias_url VARCHAR NOT NULL,
    canonical_url VARCHAR NOT NULL,
    discovered_at TIMESTAMPTZ NOT NULL,
    http_status INTEGER,
    exclusion_reason VARCHAR NOT NULL,
    excluded_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE INDEX IF NOT EXISTS products_queue_idx
ON products (status, next_retry_at, source_lastmod);
"""


class PostResolutionLike(Protocol):
    post_slug: str
    outcome: str
    post_id: int | None
    product_id: int | None
    product_slug: str | None


class SearchRecordLike(Protocol):
    product_id: int
    slug: str
    name: str | None
    tagline: str | None


class CategoryRecordLike(Protocol):
    product_id: int
    slug: str


class CollectionSummaryLike(Protocol):
    collection_id: int
    products_count: int
    products: Sequence[CategoryRecordLike]


class ArchiveRecoveryLike(Protocol):
    slug: str
    attempt: int
    outcome: str
    capture_timestamp: str | None
    http_status: int | None
    error: str | None
    data: ProductData | None
    content_hash: str | None


class CommonCrawlCaptureLike(Protocol):
    slug: str
    crawl_id: str
    capture_timestamp: str
    url: str
    filename: str
    byte_offset: int
    byte_length: int
    mime: str | None


class ArchivePostRecoveryLike(Protocol):
    post_slug: str
    attempt: int
    outcome: str
    capture_timestamp: str | None
    http_status: int | None
    error: str | None
    data: ProductData | None
    content_hash: str | None


class WebsiteEnrichmentLike(Protocol):
    slug: str
    requested_url: str
    final_url: str | None
    attempt: int
    outcome: str
    http_status: int | None
    tagline: str | None
    tagline_source: str | None
    description: str | None
    description_source: str | None
    response_hash: str | None
    error: str | None


class CatalogDatabase:
    """The crawler's sole DuckDB writer; all mutations are transaction-batched."""

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = duckdb.connect(str(path))
        self.connection.execute(SCHEMA_SQL)
        self.connection.execute(
            "UPDATE products SET categories = [] "
            "WHERE status = 'fetched' AND categories IS NULL"
        )
        self._recover_interrupted_claims()

    def _recover_interrupted_claims(self) -> None:
        self.connection.execute(
            """
            UPDATE products
            SET status = 'pending', attempts = greatest(0, attempts - 1)
            WHERE status = 'retry'
              AND next_retry_at IS NULL
              AND last_error IS NULL
            """
        )
        self.connection.execute(
            """
            UPDATE website_enrichments
            SET outcome = 'retry', attempts = greatest(0, attempts - 1),
                updated_at = now()
            WHERE outcome = 'claimed'
            """
        )

    def close(self) -> None:
        self.connection.execute("CHECKPOINT")
        self.connection.close()

    def __enter__(self) -> CatalogDatabase:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def import_products(self, products: Iterable[SitemapProduct]) -> tuple[int, int]:
        rows = [(product.slug, product.producthunt_url, product.lastmod) for product in products]
        if not rows:
            return 0, 0
        before = self.connection.execute("SELECT count(*) FROM products").fetchone()[0]
        self.connection.execute("BEGIN")
        try:
            self.connection.execute(
                """
                INSERT INTO products (slug, producthunt_url, source_lastmod)
                SELECT incoming.slug, incoming.producthunt_url, incoming.source_lastmod
                FROM (
                    SELECT
                        unnest(?::VARCHAR[]) AS slug,
                        unnest(?::VARCHAR[]) AS producthunt_url,
                        unnest(?::TIMESTAMPTZ[]) AS source_lastmod
                ) AS incoming
                WHERE NOT EXISTS (
                    SELECT 1 FROM product_aliases
                    WHERE alias_slug = incoming.slug
                )
                  AND NOT EXISTS (
                      SELECT 1 FROM excluded_products
                      WHERE excluded_products.slug = incoming.slug
                  )
                ON CONFLICT (slug) DO UPDATE SET
                    source_lastmod = CASE
                        WHEN excluded.source_lastmod IS NULL THEN products.source_lastmod
                        WHEN products.source_lastmod IS NULL THEN excluded.source_lastmod
                        ELSE greatest(products.source_lastmod, excluded.source_lastmod)
                    END
                """,
                [
                    [slug for slug, _, _ in rows],
                    [url for _, url, _ in rows],
                    [lastmod for _, _, lastmod in rows],
                ],
            )
            self.connection.execute("COMMIT")
        except BaseException:
            with suppress(duckdb.TransactionException):
                self.connection.execute("ROLLBACK")
            raise
        after = self.connection.execute("SELECT count(*) FROM products").fetchone()[0]
        return after - before, len(rows)

    def prepare_manifest_scan(self, name: str, start_id: int, stop_id: int) -> dict[str, int]:
        if start_id < 1 or stop_id <= start_id:
            raise ValueError("manifest scan requires 1 <= start_id < stop_id")
        self.connection.execute(
            """
            INSERT INTO manifest_scans (scan_name, next_id, stop_id)
            VALUES (?, ?, ?)
            ON CONFLICT (scan_name) DO UPDATE SET
                stop_id = greatest(manifest_scans.stop_id, excluded.stop_id),
                updated_at = now()
            """,
            [name, start_id, stop_id],
        )
        row = self.connection.execute(
            """
            SELECT next_id, stop_id, requests, discovered, unavailable
            FROM manifest_scans WHERE scan_name = ?
            """,
            [name],
        ).fetchone()
        assert row is not None
        return {
            "next_id": int(row[0]),
            "stop_id": int(row[1]),
            "requests": int(row[2]),
            "discovered": int(row[3]),
            "unavailable": int(row[4]),
        }

    def apply_catalog_batch(
        self,
        scan_name: str,
        expected_start_id: int,
        next_id: int,
        records: Sequence[CatalogRecord],
        unavailable: int,
    ) -> None:
        """Atomically save API records and advance the crash-safe numeric cursor."""
        self.connection.execute("BEGIN")
        try:
            state = self.connection.execute(
                "SELECT next_id FROM manifest_scans WHERE scan_name = ?", [scan_name]
            ).fetchone()
            if state is None or int(state[0]) != expected_start_id:
                raise ValueError("manifest scan cursor changed unexpectedly")

            if records:
                self.connection.executemany(
                    """
                    INSERT INTO source_products (product_id, slug)
                    VALUES (?, ?)
                    ON CONFLICT (product_id) DO UPDATE SET
                        slug = excluded.slug,
                        last_seen_at = now()
                    """,
                    [(record.product_id, record.data.slug) for record in records],
                )
                product_rows = []
                for record in records:
                    data = record.data
                    fetched = data.is_complete_enough
                    product_rows.append(
                        (
                            data.slug,
                            data.producthunt_url,
                            data.name,
                            data.tagline,
                            data.description,
                            data.website_url,
                            data.categories,
                            "fetched" if fetched else "pending",
                            200 if fetched else None,
                            record.content_hash if fetched else None,
                            data.slug,
                        )
                    )
                self.connection.executemany(
                    """
                    INSERT INTO products (
                        slug, producthunt_url, name, tagline, description, website_url,
                        categories, status, http_status, fetched_at, content_hash
                    )
                    SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?,
                           CASE WHEN ? = 'fetched' THEN now() ELSE NULL END, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM product_aliases WHERE alias_slug = ?
                    )
                    ON CONFLICT (slug) DO UPDATE SET
                        name = coalesce(excluded.name, products.name),
                        tagline = coalesce(excluded.tagline, products.tagline),
                        description = coalesce(excluded.description, products.description),
                        website_url = coalesce(excluded.website_url, products.website_url),
                        categories = CASE
                            WHEN excluded.categories IS NOT NULL
                             AND length(excluded.categories) > 0
                            THEN excluded.categories ELSE products.categories END,
                        status = CASE WHEN excluded.status = 'fetched'
                            THEN 'fetched' ELSE products.status END,
                        http_status = CASE WHEN excluded.status = 'fetched'
                            THEN 200 ELSE products.http_status END,
                        fetched_at = CASE WHEN excluded.status = 'fetched'
                            THEN now() ELSE products.fetched_at END,
                        content_hash = CASE WHEN excluded.status = 'fetched'
                            THEN excluded.content_hash ELSE products.content_hash END,
                        next_retry_at = CASE WHEN excluded.status = 'fetched'
                            THEN NULL ELSE products.next_retry_at END,
                        last_error = CASE WHEN excluded.status = 'fetched'
                            THEN NULL ELSE products.last_error END
                    """,
                    [
                        (
                            slug,
                            url,
                            name,
                            tagline,
                            description,
                            website_url,
                            categories,
                            status,
                            http_status,
                            status,
                            row_hash,
                            alias_slug,
                        )
                        for (
                            slug,
                            url,
                            name,
                            tagline,
                            description,
                            website_url,
                            categories,
                            status,
                            http_status,
                            row_hash,
                            alias_slug,
                        ) in product_rows
                    ],
                )

            self.connection.execute(
                """
                UPDATE manifest_scans SET
                    next_id = ?, requests = requests + 1,
                    discovered = discovered + ?, unavailable = unavailable + ?,
                    updated_at = now()
                WHERE scan_name = ?
                """,
                [next_id, len(records), unavailable, scan_name],
            )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def total_products(self) -> int:
        return int(self.connection.execute("SELECT count(*) FROM products").fetchone()[0])

    def archive_scan_complete(self, index_id: str, path_kind: str) -> bool:
        return bool(
            self.connection.execute(
                "SELECT count(*) FROM archive_scans WHERE index_id = ? AND path_kind = ?",
                [index_id, path_kind],
            ).fetchone()[0]
        )

    def apply_archive_scan(
        self,
        index_id: str,
        path_kind: str,
        candidates: set[str],
        captures: int,
        malformed_lines: int = 0,
    ) -> None:
        """Commit one archive index only after all of its candidates are durable."""
        rows = [(path_kind, slug) for slug in sorted(candidates)]
        slugs = [slug for _, slug in rows]
        self.connection.execute("BEGIN")
        try:
            if rows:
                self.connection.execute(
                    """
                    INSERT INTO archive_candidates (path_kind, slug)
                    SELECT ?, unnest(?)
                    ON CONFLICT (path_kind, slug) DO UPDATE SET last_seen_at = now()
                    """,
                    [path_kind, slugs],
                )
                if path_kind == "products":
                    self.connection.execute(
                        """
                        INSERT INTO products (slug, producthunt_url)
                        SELECT candidate.slug,
                               'https://www.producthunt.com/products/' || candidate.slug
                        FROM unnest(?) AS candidate(slug)
                        WHERE NOT EXISTS (
                            SELECT 1 FROM product_aliases
                            WHERE alias_slug = candidate.slug
                        )
                        ON CONFLICT (slug) DO NOTHING
                        """,
                        [slugs],
                    )
            self.connection.execute(
                """
                INSERT INTO archive_scans (
                    index_id, path_kind, captures, unique_candidates, malformed_lines
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (index_id, path_kind) DO NOTHING
                """,
                [index_id, path_kind, captures, len(candidates), malformed_lines],
            )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def archive_counts(self) -> dict[str, int]:
        rows = dict(
            self.connection.execute(
                "SELECT path_kind, count(*) FROM archive_candidates GROUP BY path_kind"
            ).fetchall()
        )
        products = int(rows.get("products", 0))
        posts = int(rows.get("posts", 0))
        scans = int(self.connection.execute("SELECT count(*) FROM archive_scans").fetchone()[0])
        malformed = int(
            self.connection.execute(
                "SELECT coalesce(sum(malformed_lines), 0) FROM archive_scans"
            ).fetchone()[0]
        )
        wayback = self.connection.execute(
            """
            SELECT count(*) FILTER (WHERE complete), coalesce(sum(requests), 0),
                   coalesce(sum(captures), 0)
            FROM wayback_scans
            """
        ).fetchone()
        resolution_rows = dict(
            self.connection.execute(
                "SELECT outcome, count(*) FROM archive_post_resolutions GROUP BY outcome"
            ).fetchall()
        )
        missing_products = int(
            self.connection.execute(
                """
                SELECT count(*) FROM archive_candidates AS archive
                LEFT JOIN source_products AS source USING (slug)
                WHERE archive.path_kind = 'products' AND source.slug IS NULL
                """
            ).fetchone()[0]
        )
        return {
            "unique_candidates": products + posts,
            "products": products,
            "posts": posts,
            "product_candidates_absent_from_live_ids": missing_products,
            "scans_complete": scans,
            "malformed_index_lines": malformed,
            "wayback_paths_complete": int(wayback[0]),
            "wayback_requests": int(wayback[1]),
            "wayback_captures": int(wayback[2]),
            "post_resolved": int(resolution_rows.get("resolved", 0)),
            "post_no_product": int(resolution_rows.get("no_product", 0)),
            "post_unavailable": int(resolution_rows.get("unavailable", 0)),
        }

    def wayback_scan_state(self, path_kind: str) -> dict[str, object]:
        self.connection.execute(
            """
            INSERT INTO wayback_scans (path_kind) VALUES (?)
            ON CONFLICT (path_kind) DO NOTHING
            """,
            [path_kind],
        )
        row = self.connection.execute(
            """
            SELECT resume_key, requests, captures, complete
            FROM wayback_scans WHERE path_kind = ?
            """,
            [path_kind],
        ).fetchone()
        assert row is not None
        return {
            "resume_key": row[0],
            "requests": int(row[1]),
            "captures": int(row[2]),
            "complete": bool(row[3]),
        }

    def apply_wayback_batch(
        self,
        path_kind: str,
        candidates: set[str],
        captures: int,
        next_resume_key: str | None,
    ) -> None:
        rows = [(path_kind, slug) for slug in sorted(candidates)]
        slugs = [slug for _, slug in rows]
        self.connection.execute("BEGIN")
        try:
            if rows:
                self.connection.execute(
                    """
                    INSERT INTO archive_candidates (path_kind, slug)
                    SELECT ?, unnest(?)
                    ON CONFLICT (path_kind, slug) DO UPDATE SET last_seen_at = now()
                    """,
                    [path_kind, slugs],
                )
                if path_kind == "products":
                    self.connection.execute(
                        """
                        INSERT INTO products (slug, producthunt_url)
                        SELECT candidate.slug,
                               'https://www.producthunt.com/products/' || candidate.slug
                        FROM unnest(?) AS candidate(slug)
                        WHERE NOT EXISTS (
                            SELECT 1 FROM product_aliases
                            WHERE alias_slug = candidate.slug
                        )
                        ON CONFLICT (slug) DO NOTHING
                        """,
                        [slugs],
                    )
            self.connection.execute(
                """
                UPDATE wayback_scans SET
                    resume_key = ?, requests = requests + 1,
                    captures = captures + ?, complete = ?, updated_at = now()
                WHERE path_kind = ?
                """,
                [next_resume_key, captures, next_resume_key is None, path_kind],
            )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def claim_archive_recoveries(
        self, limit: int, max_attempts: int
    ) -> list[tuple[str, int]]:
        rows = self.connection.execute(
            """
            SELECT products.slug, coalesce(recovery.attempts, 0) + 1
            FROM products
            LEFT JOIN archive_recoveries AS recovery
              ON recovery.slug = products.slug
            WHERE products.status = 'unavailable'
              AND regexp_matches(
                    products.slug, '^[a-z0-9]([a-z0-9._-]*[a-z0-9])?$'
                  )
              AND coalesce(recovery.attempts, 0) < ?
              AND (recovery.slug IS NULL OR recovery.outcome IN ('claimed', 'retry'))
            ORDER BY products.slug
            LIMIT ?
            """,
            [max_attempts, limit],
        ).fetchall()
        if not rows:
            return []
        self.connection.execute("BEGIN")
        try:
            self.connection.executemany(
                """
                INSERT INTO archive_recoveries (slug, outcome, attempts)
                VALUES (?, 'claimed', ?)
                ON CONFLICT (slug) DO UPDATE SET
                    outcome = 'claimed', attempts = excluded.attempts,
                    last_error = NULL, updated_at = now()
                """,
                rows,
            )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise
        return [(str(slug), int(attempt)) for slug, attempt in rows]

    def common_crawl_recovery_scan_complete(self, crawl_id: str) -> bool:
        return bool(
            self.connection.execute(
                "SELECT 1 FROM common_crawl_recovery_scans WHERE crawl_id = ?",
                [crawl_id],
            ).fetchone()
        )

    def apply_common_crawl_recovery_scan(
        self,
        crawl_id: str,
        captures: Sequence[CommonCrawlCaptureLike],
        cdx_rows: int,
        transferred_bytes: int,
    ) -> None:
        self.connection.execute("BEGIN")
        try:
            if captures:
                self.connection.executemany(
                    """
                    INSERT INTO common_crawl_captures (
                        slug, crawl_id, capture_timestamp, url, filename,
                        byte_offset, byte_length, mime
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (slug, crawl_id) DO UPDATE SET
                        capture_timestamp = excluded.capture_timestamp,
                        url = excluded.url,
                        filename = excluded.filename,
                        byte_offset = excluded.byte_offset,
                        byte_length = excluded.byte_length,
                        mime = excluded.mime,
                        outcome = CASE
                            WHEN common_crawl_captures.outcome = 'recovered'
                            THEN 'recovered' ELSE 'pending' END,
                        last_error = NULL,
                        updated_at = now()
                    """,
                    [
                        (
                            item.slug,
                            item.crawl_id,
                            item.capture_timestamp,
                            item.url,
                            item.filename,
                            item.byte_offset,
                            item.byte_length,
                            item.mime,
                        )
                        for item in captures
                    ],
                )
            self.connection.execute(
                """
                INSERT INTO common_crawl_recovery_scans (
                    crawl_id, cdx_rows, matched_slugs, transferred_bytes
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT (crawl_id) DO UPDATE SET
                    cdx_rows = excluded.cdx_rows,
                    matched_slugs = excluded.matched_slugs,
                    transferred_bytes = excluded.transferred_bytes,
                    completed_at = now()
                """,
                [crawl_id, cdx_rows, len(captures), transferred_bytes],
            )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def pending_common_crawl_captures(
        self, limit: int, max_attempts: int
    ) -> list[tuple[str, str, str, str, str, int, int, str | None, int]]:
        return [
            (
                str(row[0]),
                str(row[1]),
                str(row[2]),
                str(row[3]),
                str(row[4]),
                int(row[5]),
                int(row[6]),
                str(row[7]) if row[7] is not None else None,
                int(row[8]),
            )
            for row in self.connection.execute(
                """
                SELECT slug, crawl_id, capture_timestamp, url, filename,
                       byte_offset, byte_length, mime, attempts + 1
                FROM (
                    SELECT capture.*, row_number() OVER (
                        PARTITION BY capture.slug
                        ORDER BY capture.capture_timestamp DESC
                    ) AS capture_rank
                    FROM common_crawl_captures AS capture
                    JOIN products USING (slug)
                    WHERE products.status = 'unavailable'
                      AND capture.outcome IN ('pending', 'fetch_failed')
                      AND capture.attempts < ?
                ) AS ranked
                WHERE capture_rank = 1
                ORDER BY slug
                LIMIT ?
                """,
                [max_attempts, limit],
            ).fetchall()
        ]

    def apply_common_crawl_capture_outcome(
        self,
        slug: str,
        crawl_id: str,
        outcome: str,
        attempt: int,
        http_status: int | None,
        error: str | None,
    ) -> None:
        if outcome not in {"recovered", "parse_failed", "fetch_failed"}:
            raise ValueError("invalid Common Crawl capture outcome")
        self.connection.execute(
            """
            UPDATE common_crawl_captures SET
                outcome = ?, attempts = ?, http_status = ?, last_error = ?,
                updated_at = now()
            WHERE slug = ? AND crawl_id = ?
            """,
            [outcome, attempt, http_status, error, slug, crawl_id],
        )

    def common_crawl_recovery_counts(self) -> dict[str, int]:
        row = self.connection.execute(
            """
            SELECT (SELECT count(*) FROM common_crawl_recovery_scans),
                   count(*),
                   (
                       SELECT count(*)
                       FROM common_crawl_captures AS pending
                       JOIN products USING (slug)
                       WHERE pending.outcome = 'pending'
                         AND products.status = 'unavailable'
                   ),
                   count(*) FILTER (WHERE outcome = 'recovered'),
                   count(*) FILTER (WHERE outcome = 'parse_failed'),
                   count(*) FILTER (WHERE outcome = 'fetch_failed')
            FROM common_crawl_captures
            """
        ).fetchone()
        return {
            "scans_complete": int(row[0]),
            "captures": int(row[1]),
            "pending": int(row[2]),
            "recovered": int(row[3]),
            "parse_failed": int(row[4]),
            "fetch_failed": int(row[5]),
        }

    def common_crawl_post_recovery_scan_complete(self, crawl_id: str) -> bool:
        return bool(
            self.connection.execute(
                "SELECT 1 FROM common_crawl_post_recovery_scans WHERE crawl_id = ?",
                [crawl_id],
            ).fetchone()
        )

    def apply_common_crawl_post_recovery_scan(
        self,
        crawl_id: str,
        captures: Sequence[CommonCrawlCaptureLike],
        cdx_rows: int,
        transferred_bytes: int,
    ) -> None:
        self.connection.execute("BEGIN")
        try:
            if captures:
                self.connection.executemany(
                    """
                    INSERT INTO common_crawl_post_captures (
                        post_slug, crawl_id, capture_timestamp, url, filename,
                        byte_offset, byte_length, mime
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (post_slug, crawl_id) DO UPDATE SET
                        capture_timestamp = excluded.capture_timestamp,
                        url = excluded.url,
                        filename = excluded.filename,
                        byte_offset = excluded.byte_offset,
                        byte_length = excluded.byte_length,
                        mime = excluded.mime,
                        outcome = CASE
                            WHEN common_crawl_post_captures.outcome = 'recovered'
                            THEN 'recovered' ELSE 'pending' END,
                        last_error = NULL,
                        updated_at = now()
                    """,
                    [
                        (
                            item.slug,
                            item.crawl_id,
                            item.capture_timestamp,
                            item.url,
                            item.filename,
                            item.byte_offset,
                            item.byte_length,
                            item.mime,
                        )
                        for item in captures
                    ],
                )
            self.connection.execute(
                """
                INSERT INTO common_crawl_post_recovery_scans (
                    crawl_id, cdx_rows, matched_slugs, transferred_bytes
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT (crawl_id) DO UPDATE SET
                    cdx_rows = excluded.cdx_rows,
                    matched_slugs = excluded.matched_slugs,
                    transferred_bytes = excluded.transferred_bytes,
                    completed_at = now()
                """,
                [crawl_id, cdx_rows, len(captures), transferred_bytes],
            )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def pending_common_crawl_post_captures(
        self, limit: int, max_attempts: int
    ) -> list[tuple[str, str, str, str, str, int, int, str | None, int]]:
        return [
            (
                str(row[0]),
                str(row[1]),
                str(row[2]),
                str(row[3]),
                str(row[4]),
                int(row[5]),
                int(row[6]),
                str(row[7]) if row[7] is not None else None,
                int(row[8]),
            )
            for row in self.connection.execute(
                """
                SELECT post_slug, crawl_id, capture_timestamp, url, filename,
                       byte_offset, byte_length, mime, attempts + 1
                FROM (
                    SELECT capture.*, row_number() OVER (
                        PARTITION BY capture.post_slug
                        ORDER BY capture.capture_timestamp DESC
                    ) AS capture_rank
                    FROM common_crawl_post_captures AS capture
                    JOIN archive_post_resolutions AS resolution
                      ON resolution.post_slug = capture.post_slug
                    WHERE resolution.outcome = 'unavailable'
                      AND capture.outcome IN ('pending', 'fetch_failed')
                      AND capture.attempts < ?
                ) AS ranked
                WHERE capture_rank = 1
                ORDER BY post_slug
                LIMIT ?
                """,
                [max_attempts, limit],
            ).fetchall()
        ]

    def apply_common_crawl_post_capture_outcome(
        self,
        post_slug: str,
        crawl_id: str,
        outcome: str,
        attempt: int,
        http_status: int | None,
        error: str | None,
    ) -> None:
        if outcome not in {"recovered", "parse_failed", "fetch_failed"}:
            raise ValueError("invalid Common Crawl post capture outcome")
        self.connection.execute(
            """
            UPDATE common_crawl_post_captures SET
                outcome = ?, attempts = ?, http_status = ?, last_error = ?,
                updated_at = now()
            WHERE post_slug = ? AND crawl_id = ?
            """,
            [outcome, attempt, http_status, error, post_slug, crawl_id],
        )

    def common_crawl_post_recovery_counts(self) -> dict[str, int]:
        row = self.connection.execute(
            """
            SELECT (SELECT count(*) FROM common_crawl_post_recovery_scans),
                   count(*),
                   (
                       SELECT count(*)
                       FROM common_crawl_post_captures AS pending
                       JOIN archive_post_resolutions AS resolution
                         ON resolution.post_slug = pending.post_slug
                       WHERE pending.outcome = 'pending'
                         AND resolution.outcome = 'unavailable'
                   ),
                   count(*) FILTER (WHERE outcome = 'recovered'),
                   count(*) FILTER (WHERE outcome = 'parse_failed'),
                   count(*) FILTER (WHERE outcome = 'fetch_failed')
            FROM common_crawl_post_captures
            """
        ).fetchone()
        return {
            "scans_complete": int(row[0]),
            "captures": int(row[1]),
            "pending": int(row[2]),
            "recovered": int(row[3]),
            "parse_failed": int(row[4]),
            "fetch_failed": int(row[5]),
        }

    def apply_archive_recoveries(
        self, recoveries: Sequence[ArchiveRecoveryLike]
    ) -> None:
        if not recoveries:
            return
        self.connection.execute("BEGIN")
        try:
            for recovery in recoveries:
                self.connection.execute(
                    """
                    INSERT INTO archive_recoveries (slug, outcome, attempts)
                    VALUES (?, 'claimed', ?)
                    ON CONFLICT (slug) DO UPDATE SET
                        attempts = greatest(archive_recoveries.attempts, excluded.attempts),
                        updated_at = now()
                    """,
                    [recovery.slug, recovery.attempt],
                )
                if recovery.outcome == "recovered":
                    if recovery.data is None or recovery.content_hash is None:
                        raise ValueError("recovered archive row is missing parsed data")
                    canonical_url = recovery.data.producthunt_url
                    self._apply_result(
                        FetchResult(
                            requested_slug=recovery.slug,
                            canonical_slug=recovery.data.slug,
                            requested_url=(
                                f"https://www.producthunt.com/products/{recovery.slug}"
                            ),
                            canonical_url=canonical_url,
                            attempt=recovery.attempt,
                            status=ProductStatus.FETCHED,
                            http_status=recovery.http_status,
                            proxy_label="wayback",
                            elapsed_ms=0,
                            data=recovery.data,
                            content_hash=recovery.content_hash,
                        )
                    )
                self.connection.execute(
                    """
                    UPDATE archive_recoveries SET
                        outcome = ?, capture_timestamp = ?, http_status = ?,
                        last_error = ?, updated_at = now()
                    WHERE slug = ?
                    """,
                    [
                        recovery.outcome,
                        recovery.capture_timestamp,
                        recovery.http_status,
                        recovery.error,
                        recovery.slug,
                    ],
                )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def archive_recovery_counts(self) -> dict[str, int]:
        rows = {
            str(outcome): int(count)
            for outcome, count in self.connection.execute(
                "SELECT outcome, count(*) FROM archive_recoveries GROUP BY outcome"
            ).fetchall()
        }
        rows["eligible_unavailable"] = int(
            self.connection.execute(
                """
                SELECT count(*) FROM products
                WHERE products.status = 'unavailable'
                  AND regexp_matches(
                        products.slug, '^[a-z0-9]([a-z0-9._-]*[a-z0-9])?$'
                      )
                """
            ).fetchone()[0]
        )
        return rows

    def requeue_archive_recoveries(self, outcomes: Sequence[str]) -> int:
        allowed = {"no_capture", "parse_failed", "retry"}
        if not outcomes or not set(outcomes) <= allowed:
            raise ValueError("invalid archive recovery outcomes")
        placeholders = ",".join("?" for _ in outcomes)
        changed = int(
            self.connection.execute(
                f"SELECT count(*) FROM archive_recoveries "
                f"WHERE outcome IN ({placeholders})",  # noqa: S608
                list(outcomes),
            ).fetchone()[0]
        )
        self.connection.execute(
            f"""
            UPDATE archive_recoveries SET
                outcome = 'retry', attempts = 0, last_error = NULL, updated_at = now()
            WHERE outcome IN ({placeholders})
            """,  # noqa: S608
            list(outcomes),
        )
        return changed

    def claim_archive_post_recoveries(
        self, limit: int, max_attempts: int
    ) -> list[tuple[str, int]]:
        rows = self.connection.execute(
            """
            SELECT archive.slug, coalesce(recovery.attempts, 0) + 1
            FROM archive_candidates AS archive
            JOIN archive_post_resolutions AS resolution
              ON resolution.post_slug = archive.slug
            LEFT JOIN archive_post_recoveries AS recovery
              ON recovery.post_slug = archive.slug
            WHERE archive.path_kind = 'posts'
              AND resolution.outcome = 'unavailable'
              AND regexp_matches(archive.slug, '^[a-z0-9][a-z0-9_-]*$')
              AND NOT regexp_matches(archive.slug, '^[0-9]+$')
              AND coalesce(recovery.attempts, 0) < ?
              AND (
                recovery.post_slug IS NULL
                OR recovery.outcome IN ('claimed', 'retry')
              )
            ORDER BY archive.slug
            LIMIT ?
            """,
            [max_attempts, limit],
        ).fetchall()
        if not rows:
            return []
        self.connection.execute("BEGIN")
        try:
            self.connection.executemany(
                """
                INSERT INTO archive_post_recoveries (post_slug, outcome, attempts)
                VALUES (?, 'claimed', ?)
                ON CONFLICT (post_slug) DO UPDATE SET
                    outcome = 'claimed', attempts = excluded.attempts,
                    last_error = NULL, updated_at = now()
                """,
                rows,
            )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise
        return [(str(slug), int(attempt)) for slug, attempt in rows]

    def apply_archive_post_recoveries(
        self, recoveries: Sequence[ArchivePostRecoveryLike]
    ) -> int:
        if not recoveries:
            return 0
        before = int(
            self.connection.execute(
                "SELECT count(*) FROM products WHERE status = 'fetched'"
            ).fetchone()[0]
        )
        self.connection.execute("BEGIN")
        try:
            for recovery in recoveries:
                self.connection.execute(
                    """
                    INSERT INTO archive_post_recoveries (post_slug, outcome, attempts)
                    VALUES (?, 'claimed', ?)
                    ON CONFLICT (post_slug) DO UPDATE SET
                        attempts = greatest(
                            archive_post_recoveries.attempts, excluded.attempts
                        ),
                        updated_at = now()
                    """,
                    [recovery.post_slug, recovery.attempt],
                )
                product_slug = recovery.data.slug if recovery.data else None
                if recovery.outcome == "recovered":
                    if recovery.data is None or recovery.content_hash is None:
                        raise ValueError("recovered post archive row is missing parsed data")
                    alias = self.connection.execute(
                        "SELECT canonical_slug FROM product_aliases WHERE alias_slug = ?",
                        [recovery.data.slug],
                    ).fetchone()
                    if alias is not None:
                        product_slug = str(alias[0])
                    else:
                        self.connection.execute(
                            """
                            INSERT INTO products (slug, producthunt_url)
                            VALUES (?, ?)
                            ON CONFLICT (slug) DO NOTHING
                            """,
                            [recovery.data.slug, recovery.data.producthunt_url],
                        )
                        status = self.connection.execute(
                            "SELECT status FROM products WHERE slug = ?",
                            [recovery.data.slug],
                        ).fetchone()
                        if status is not None and str(status[0]) != "fetched":
                            self._apply_result(
                                FetchResult(
                                    requested_slug=recovery.data.slug,
                                    canonical_slug=recovery.data.slug,
                                    requested_url=recovery.data.producthunt_url,
                                    canonical_url=recovery.data.producthunt_url,
                                    attempt=recovery.attempt,
                                    status=ProductStatus.FETCHED,
                                    http_status=recovery.http_status,
                                    proxy_label="wayback-post",
                                    elapsed_ms=0,
                                    data=recovery.data,
                                    content_hash=recovery.content_hash,
                                )
                            )
                    self.connection.execute(
                        """
                        UPDATE archive_post_resolutions SET
                            product_slug = ?, outcome = 'resolved', resolved_at = now()
                        WHERE post_slug = ?
                        """,
                        [product_slug, recovery.post_slug],
                    )
                self.connection.execute(
                    """
                    UPDATE archive_post_recoveries SET
                        outcome = ?, capture_timestamp = ?, product_slug = ?,
                        http_status = ?, last_error = ?, updated_at = now()
                    WHERE post_slug = ?
                    """,
                    [
                        recovery.outcome,
                        recovery.capture_timestamp,
                        product_slug,
                        recovery.http_status,
                        recovery.error,
                        recovery.post_slug,
                    ],
                )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise
        after = int(
            self.connection.execute(
                "SELECT count(*) FROM products WHERE status = 'fetched'"
            ).fetchone()[0]
        )
        return after - before

    def archive_post_recovery_counts(self) -> dict[str, int]:
        return {
            str(outcome): int(count)
            for outcome, count in self.connection.execute(
                "SELECT outcome, count(*) FROM archive_post_recoveries GROUP BY outcome"
            ).fetchall()
        }

    def requeue_archive_post_recoveries(self, outcomes: Sequence[str]) -> int:
        allowed = {"no_capture", "parse_failed", "retry"}
        if not outcomes or not set(outcomes) <= allowed:
            raise ValueError("invalid archive post recovery outcomes")
        placeholders = ",".join("?" for _ in outcomes)
        changed = int(
            self.connection.execute(
                f"SELECT count(*) FROM archive_post_recoveries "
                f"WHERE outcome IN ({placeholders})",  # noqa: S608
                list(outcomes),
            ).fetchone()[0]
        )
        self.connection.execute(
            f"""
            UPDATE archive_post_recoveries SET
                outcome = 'retry', attempts = 0, last_error = NULL, updated_at = now()
            WHERE outcome IN ({placeholders})
            """,  # noqa: S608
            list(outcomes),
        )
        return changed

    def pending_archive_post_slugs(self, limit: int) -> list[str]:
        return [
            str(row[0])
            for row in self.connection.execute(
                """
                SELECT archive.slug
                FROM archive_candidates AS archive
                LEFT JOIN source_products AS source USING (slug)
                LEFT JOIN archive_post_resolutions AS resolution
                    ON resolution.post_slug = archive.slug
                WHERE archive.path_kind = 'posts'
                  AND (
                    source.slug IS NULL
                    OR regexp_matches(archive.slug, '^[0-9]+$')
                  )
                  AND resolution.post_slug IS NULL
                  AND regexp_matches(archive.slug, '^[a-z0-9][a-z0-9_-]*$')
                ORDER BY archive.slug
                LIMIT ?
                """,
                [limit],
            ).fetchall()
        ]

    def quarantine_invalid_archive_post_slugs(self) -> int:
        rows = self.connection.execute(
            """
            SELECT archive.slug
            FROM archive_candidates AS archive
            LEFT JOIN source_products AS source USING (slug)
            LEFT JOIN archive_post_resolutions AS resolution
                ON resolution.post_slug = archive.slug
            WHERE archive.path_kind = 'posts'
              AND source.slug IS NULL
              AND resolution.post_slug IS NULL
              AND NOT regexp_matches(archive.slug, '^[a-z0-9][a-z0-9_-]*$')
            """
        ).fetchall()
        if rows:
            self.connection.executemany(
                """
                INSERT INTO archive_post_resolutions (post_slug, outcome)
                VALUES (?, 'unavailable')
                ON CONFLICT (post_slug) DO NOTHING
                """,
                rows,
            )
        return len(rows)

    def pending_archive_product_slugs(self, limit: int) -> list[str]:
        return [
            str(row[0])
            for row in self.connection.execute(
                """
                SELECT products.slug
                FROM products
                JOIN archive_candidates AS archive
                  ON archive.path_kind = 'products' AND archive.slug = products.slug
                WHERE products.status IN ('pending', 'retry')
                ORDER BY products.slug
                LIMIT ?
                """,
                [limit],
            ).fetchall()
        ]

    def apply_archive_product_resolutions(
        self, resolutions: Sequence[tuple[str, CatalogRecord | None]]
    ) -> None:
        self.connection.execute("BEGIN")
        try:
            for requested_slug, record in resolutions:
                requested_url = f"https://www.producthunt.com/products/{requested_slug}"
                if record is None:
                    self.connection.execute(
                        """
                        UPDATE products SET status = 'unavailable', http_status = 404,
                            last_error = 'not returned by Product Hunt product lookup'
                        WHERE slug = ?
                        """,
                        [requested_slug],
                    )
                    continue
                self.connection.execute(
                    """
                    INSERT INTO source_products (product_id, slug)
                    VALUES (?, ?)
                    ON CONFLICT (product_id) DO UPDATE SET
                        slug = excluded.slug, last_seen_at = now()
                    """,
                    [record.product_id, record.data.slug],
                )
                self._apply_result(
                    FetchResult(
                        requested_slug=requested_slug,
                        canonical_slug=record.data.slug,
                        requested_url=requested_url,
                        canonical_url=record.data.producthunt_url,
                        attempt=0,
                        status=ProductStatus.FETCHED,
                        http_status=200,
                        proxy_label="direct-graphql",
                        elapsed_ms=0,
                        data=record.data,
                        content_hash=record.content_hash,
                    )
                )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def apply_post_resolutions(self, resolutions: Sequence[PostResolutionLike]) -> None:
        if not resolutions:
            return
        self.connection.execute("BEGIN")
        try:
            for item in resolutions:
                post_slug = item.post_slug
                outcome = item.outcome
                post_id = item.post_id
                product_id = item.product_id
                product_slug = item.product_slug
                if outcome == "resolved" and product_id is not None and product_slug:
                    self.connection.execute(
                        """
                        INSERT INTO source_products (product_id, slug)
                        VALUES (?, ?)
                        ON CONFLICT (product_id) DO UPDATE SET
                            slug = excluded.slug, last_seen_at = now()
                        """,
                        [product_id, product_slug],
                    )
                    self.connection.execute(
                        """
                        INSERT INTO products (slug, producthunt_url)
                        SELECT ?, 'https://www.producthunt.com/products/' || ?
                        WHERE NOT EXISTS (
                            SELECT 1 FROM product_aliases WHERE alias_slug = ?
                        )
                        ON CONFLICT (slug) DO NOTHING
                        """,
                        [product_slug, product_slug, product_slug],
                    )
                self.connection.execute(
                    """
                    INSERT INTO archive_post_resolutions (
                        post_slug, post_id, product_id, product_slug, outcome
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (post_slug) DO NOTHING
                    """,
                    [post_slug, post_id, product_id, product_slug, outcome],
                )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def apply_post_id_batch(
        self,
        scan_name: str,
        expected_start_id: int,
        next_id: int,
        resolutions: Sequence[PostResolutionLike],
    ) -> int:
        """Atomically save an exact numeric-post batch and advance its cursor."""
        self.connection.execute("BEGIN")
        try:
            state = self.connection.execute(
                "SELECT next_id FROM manifest_scans WHERE scan_name = ?", [scan_name]
            ).fetchone()
            if state is None or int(state[0]) != expected_start_id:
                raise ValueError("post ID scan cursor changed unexpectedly")

            before = int(
                self.connection.execute("SELECT count(*) FROM products").fetchone()[0]
            )
            for item in resolutions:
                if (
                    item.outcome == "resolved"
                    and item.product_id is not None
                    and item.product_slug
                ):
                    self.connection.execute(
                        """
                        INSERT INTO source_products (product_id, slug)
                        VALUES (?, ?)
                        ON CONFLICT (product_id) DO UPDATE SET
                            slug = excluded.slug, last_seen_at = now()
                        """,
                        [item.product_id, item.product_slug],
                    )
                    self.connection.execute(
                        """
                        INSERT INTO products (slug, producthunt_url)
                        SELECT ?, 'https://www.producthunt.com/products/' || ?
                        WHERE NOT EXISTS (
                            SELECT 1 FROM product_aliases WHERE alias_slug = ?
                        )
                        ON CONFLICT (slug) DO NOTHING
                        """,
                        [item.product_slug, item.product_slug, item.product_slug],
                    )
                self.connection.execute(
                    """
                    INSERT INTO archive_post_resolutions (
                        post_slug, post_id, product_id, product_slug, outcome
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (post_slug) DO UPDATE SET
                        post_id = coalesce(archive_post_resolutions.post_id,
                                           excluded.post_id),
                        product_id = CASE
                            WHEN excluded.outcome = 'resolved' THEN excluded.product_id
                            ELSE archive_post_resolutions.product_id END,
                        product_slug = CASE
                            WHEN excluded.outcome = 'resolved' THEN excluded.product_slug
                            ELSE archive_post_resolutions.product_slug END,
                        outcome = CASE
                            WHEN excluded.outcome = 'resolved' THEN 'resolved'
                            ELSE archive_post_resolutions.outcome END,
                        resolved_at = now()
                    """,
                    [
                        item.post_slug,
                        item.post_id,
                        item.product_id,
                        item.product_slug,
                        item.outcome,
                    ],
                )

            resolved = sum(item.outcome == "resolved" for item in resolutions)
            self.connection.execute(
                """
                UPDATE manifest_scans SET
                    next_id = ?, requests = requests + 1,
                    discovered = discovered + ?, unavailable = unavailable + ?,
                    updated_at = now()
                WHERE scan_name = ?
                """,
                [next_id, resolved, len(resolutions) - resolved, scan_name],
            )
            after = int(
                self.connection.execute("SELECT count(*) FROM products").fetchone()[0]
            )
            self.connection.execute("COMMIT")
            return after - before
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def pending_source_product_ids(
        self, after_id: int, limit: int
    ) -> list[int]:
        return [
            int(row[0])
            for row in self.connection.execute(
                """
                SELECT source.product_id
                FROM source_products AS source
                JOIN products USING (slug)
                WHERE products.status != 'fetched' AND source.product_id > ?
                ORDER BY source.product_id
                LIMIT ?
                """,
                [after_id, limit],
            ).fetchall()
        ]

    def prepare_search_scan(self, query: str, pages_count: int) -> None:
        self.connection.execute(
            """
            INSERT INTO search_scans (query, pages_count)
            VALUES (?, ?)
            ON CONFLICT (query) DO UPDATE SET
                pages_count = greatest(search_scans.pages_count, excluded.pages_count),
                updated_at = now()
            """,
            [query, pages_count],
        )

    def search_scan_state(self, query: str) -> dict[str, int] | None:
        row = self.connection.execute(
            "SELECT pages_count, next_page FROM search_scans WHERE query = ?", [query]
        ).fetchone()
        if row is None:
            return None
        return {"pages_count": int(row[0]), "next_page": int(row[1])}

    def apply_search_batch(
        self,
        query: str,
        expected_page: int,
        next_page: int,
        records: Sequence[SearchRecordLike],
        pages_scanned: int,
    ) -> None:
        self.connection.execute("BEGIN")
        try:
            state = self.search_scan_state(query)
            if state is None or state["next_page"] != expected_page:
                raise ValueError("search scan cursor changed unexpectedly")
            product_ids = [record.product_id for record in records]
            slugs = [record.slug for record in records]
            names = [record.name for record in records]
            taglines = [record.tagline for record in records]
            existing_ids = {
                int(row[0])
                for row in self.connection.execute(
                    """
                    SELECT product_id FROM source_products
                    WHERE product_id IN (SELECT unnest(?))
                    """,
                    [product_ids],
                ).fetchall()
            }
            new_flags = [product_id not in existing_ids for product_id in product_ids]
            if records:
                self.connection.execute(
                    """
                    INSERT INTO search_candidates (
                        product_id, slug, name, tagline, new_product_id
                    )
                    SELECT unnest(?), unnest(?), unnest(?), unnest(?), unnest(?)
                    ON CONFLICT (product_id) DO UPDATE SET
                        slug = excluded.slug,
                        name = coalesce(excluded.name, search_candidates.name),
                        tagline = coalesce(excluded.tagline, search_candidates.tagline),
                        last_seen_at = now()
                    """,
                    [product_ids, slugs, names, taglines, new_flags],
                )
            new_rows = [
                (product_ids[index], slugs[index], names[index], taglines[index])
                for index, is_new in enumerate(new_flags)
                if is_new
            ]
            if new_rows:
                new_ids = [row[0] for row in new_rows]
                new_slugs = [row[1] for row in new_rows]
                new_names = [row[2] for row in new_rows]
                new_taglines = [row[3] for row in new_rows]
                self.connection.execute(
                    """
                    INSERT INTO source_products (product_id, slug)
                    SELECT unnest(?), unnest(?)
                    ON CONFLICT (product_id) DO NOTHING
                    """,
                    [new_ids, new_slugs],
                )
                self.connection.execute(
                    """
                    INSERT INTO products (slug, producthunt_url, name, tagline)
                    SELECT candidate.slug,
                           'https://www.producthunt.com/products/' || candidate.slug,
                           candidate.name, candidate.tagline
                    FROM (
                        SELECT unnest(?) AS slug, unnest(?) AS name, unnest(?) AS tagline
                    ) AS candidate
                    WHERE NOT EXISTS (
                        SELECT 1 FROM product_aliases
                        WHERE alias_slug = candidate.slug
                    )
                    ON CONFLICT (slug) DO NOTHING
                    """,
                    [new_slugs, new_names, new_taglines],
                )
            self.connection.execute(
                """
                UPDATE search_scans SET next_page = ?, requests = requests + 1,
                    pages_scanned = pages_scanned + ?, updated_at = now()
                WHERE query = ?
                """,
                [next_page, pages_scanned, query],
            )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def search_counts(self) -> dict[str, int]:
        row = self.connection.execute(
            """
            SELECT count(*), count(*) FILTER (WHERE new_product_id)
            FROM search_candidates
            """
        ).fetchone()
        scans = self.connection.execute(
            """
            SELECT count(*), count(*) FILTER (WHERE next_page > pages_count),
                   coalesce(sum(pages_scanned), 0)
            FROM search_scans
            """
        ).fetchone()
        return {
            "unique_candidates": int(row[0]),
            "new_product_ids": int(row[1]),
            "queries": int(scans[0]),
            "queries_complete": int(scans[1]),
            "pages_scanned": int(scans[2]),
        }

    def prepare_category_scan(
        self, category_slug: str, total_count: int, pages_count: int
    ) -> None:
        if not category_slug or total_count < 0 or pages_count < 0:
            raise ValueError("invalid category scan state")
        self.connection.execute(
            """
            INSERT INTO category_scans (category_slug, total_count, pages_count)
            VALUES (?, ?, ?)
            ON CONFLICT (category_slug) DO UPDATE SET
                total_count = excluded.total_count,
                pages_count = greatest(category_scans.pages_count, excluded.pages_count),
                updated_at = now()
            """,
            [category_slug, total_count, pages_count],
        )

    def pending_category_pages(self, limit: int) -> list[tuple[str, int]]:
        return [
            (str(row[0]), int(row[1]))
            for row in self.connection.execute(
                """
                SELECT category_slug, next_page
                FROM category_scans
                WHERE next_page <= pages_count
                ORDER BY next_page, category_slug
                LIMIT ?
                """,
                [limit],
            ).fetchall()
        ]

    def apply_category_page_batch(
        self,
        pages: Sequence[tuple[str, int, Sequence[CategoryRecordLike]]],
    ) -> int:
        if not pages:
            return 0
        unique_records: dict[int, CategoryRecordLike] = {}
        for _, _, records in pages:
            for record in records:
                unique_records[record.product_id] = record
        product_ids = list(unique_records)
        existing_ids: set[int] = set()
        if product_ids:
            existing_ids = {
                int(row[0])
                for row in self.connection.execute(
                    """
                    SELECT product_id FROM source_products
                    WHERE product_id IN (SELECT unnest(?))
                    """,
                    [product_ids],
                ).fetchall()
            }
        new_ids = set(product_ids) - existing_ids
        page_slugs = [category_slug for category_slug, _, _ in pages]
        expected_pages = [page for _, page, _ in pages]
        self.connection.execute("BEGIN")
        try:
            mismatches = int(
                self.connection.execute(
                    """
                    SELECT count(*)
                    FROM (
                        SELECT unnest(?::VARCHAR[]) AS category_slug,
                               unnest(?::INTEGER[]) AS expected_page
                    ) AS incoming
                    LEFT JOIN category_scans USING (category_slug)
                    WHERE category_scans.category_slug IS NULL
                       OR category_scans.next_page != incoming.expected_page
                    """,
                    [page_slugs, expected_pages],
                ).fetchone()[0]
            )
            if mismatches:
                raise ValueError("category scan cursor changed unexpectedly")

            membership_rows = [
                (category_slug, record.product_id, record.slug, record.product_id in new_ids)
                for category_slug, _, records in pages
                for record in records
            ]
            if membership_rows:
                self.connection.execute(
                    """
                    INSERT INTO category_candidates (
                        category_slug, product_id, slug, new_product_id
                    )
                    SELECT unnest(?::VARCHAR[]), unnest(?::BIGINT[]),
                           unnest(?::VARCHAR[]), unnest(?::BOOLEAN[])
                    ON CONFLICT (category_slug, product_id) DO UPDATE SET
                        slug = excluded.slug,
                        new_product_id = category_candidates.new_product_id
                            OR excluded.new_product_id,
                        last_seen_at = now()
                    """,
                    [
                        [row[0] for row in membership_rows],
                        [row[1] for row in membership_rows],
                        [row[2] for row in membership_rows],
                        [row[3] for row in membership_rows],
                    ],
                )
            if unique_records:
                records = list(unique_records.values())
                record_ids = [record.product_id for record in records]
                record_slugs = [record.slug for record in records]
                self.connection.execute(
                    """
                    INSERT INTO source_products (product_id, slug)
                    SELECT unnest(?::BIGINT[]), unnest(?::VARCHAR[])
                    ON CONFLICT (product_id) DO UPDATE SET
                        slug = excluded.slug, last_seen_at = now()
                    """,
                    [record_ids, record_slugs],
                )
                self.connection.execute(
                    """
                    INSERT INTO products (slug, producthunt_url)
                    SELECT incoming.slug,
                           'https://www.producthunt.com/products/' || incoming.slug
                    FROM (
                        SELECT unnest(?::VARCHAR[]) AS slug
                    ) AS incoming
                    WHERE NOT EXISTS (
                        SELECT 1 FROM product_aliases
                        WHERE alias_slug = incoming.slug
                    )
                    ON CONFLICT (slug) DO NOTHING
                    """,
                    [record_slugs],
                )
            self.connection.execute(
                """
                UPDATE category_scans SET
                    next_page = incoming.next_page,
                    pages_scanned = pages_scanned + 1,
                    updated_at = now()
                FROM (
                    SELECT unnest(?::VARCHAR[]) AS category_slug,
                           unnest(?::INTEGER[]) + 1 AS next_page
                ) AS incoming
                WHERE category_scans.category_slug = incoming.category_slug
                """,
                [page_slugs, expected_pages],
            )
            self.connection.execute("COMMIT")
            return len(new_ids)
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def category_counts(self) -> dict[str, int]:
        candidates = self.connection.execute(
            """
            SELECT count(*), count(DISTINCT product_id),
                   count(DISTINCT product_id) FILTER (WHERE new_product_id)
            FROM category_candidates
            """
        ).fetchone()
        scans = self.connection.execute(
            """
            SELECT count(*), count(*) FILTER (WHERE next_page > pages_count),
                   coalesce(sum(pages_scanned), 0), coalesce(sum(total_count), 0)
            FROM category_scans
            """
        ).fetchone()
        return {
            "memberships": int(candidates[0]),
            "unique_candidates": int(candidates[1]),
            "new_product_ids": int(candidates[2]),
            "categories": int(scans[0]),
            "categories_complete": int(scans[1]),
            "pages_scanned": int(scans[2]),
            "reported_memberships": int(scans[3]),
        }

    def prepare_collection_scan(
        self, scan_name: str, total_count: int, pages_count: int
    ) -> None:
        if not scan_name or total_count < 0 or pages_count < 0:
            raise ValueError("invalid collection scan state")
        self.connection.execute(
            """
            INSERT INTO collection_scans (scan_name, total_count, pages_count)
            VALUES (?, ?, ?)
            ON CONFLICT (scan_name) DO UPDATE SET
                total_count = excluded.total_count,
                pages_count = greatest(collection_scans.pages_count, excluded.pages_count),
                updated_at = now()
            """,
            [scan_name, total_count, pages_count],
        )

    def pending_collection_pages(self, scan_name: str, limit: int) -> list[int]:
        row = self.connection.execute(
            """
            SELECT next_page, pages_count FROM collection_scans WHERE scan_name = ?
            """,
            [scan_name],
        ).fetchone()
        if row is None:
            return []
        next_page, pages_count = int(row[0]), int(row[1])
        return list(range(next_page, min(pages_count, next_page + limit)))

    def apply_collection_page_batch(
        self,
        scan_name: str,
        pages: Sequence[tuple[int, Sequence[CollectionSummaryLike]]],
    ) -> int:
        if not pages:
            return 0
        ordered = sorted(pages, key=lambda item: item[0])
        current = self.connection.execute(
            "SELECT next_page FROM collection_scans WHERE scan_name = ?", [scan_name]
        ).fetchone()
        if current is None:
            raise ValueError("collection scan is not initialized")
        expected = list(range(int(current[0]), int(current[0]) + len(ordered)))
        if [page for page, _ in ordered] != expected:
            raise ValueError("collection scan cursor changed unexpectedly")
        records = [
            record
            for _, summaries in ordered
            for summary in summaries
            for record in summary.products
        ]
        candidate_records, source_records = self._collection_record_state(records)
        self.connection.execute("BEGIN")
        try:
            self._upsert_collection_records(candidate_records, source_records)
            overflow = [
                summary
                for _, summaries in ordered
                for summary in summaries
                if summary.products_count > 100
            ]
            if overflow:
                self.connection.execute(
                    """
                    INSERT INTO collection_overflow_scans (
                        collection_id, products_count, next_offset
                    )
                    SELECT unnest(?::BIGINT[]), unnest(?::INTEGER[]), 100
                    ON CONFLICT (collection_id) DO UPDATE SET
                        products_count = greatest(
                            collection_overflow_scans.products_count,
                            excluded.products_count
                        ),
                        updated_at = now()
                    """,
                    [
                        [summary.collection_id for summary in overflow],
                        [summary.products_count for summary in overflow],
                    ],
                )
            self.connection.execute(
                """
                UPDATE collection_scans SET
                    next_page = next_page + ?,
                    pages_scanned = pages_scanned + ?,
                    collections_seen = collections_seen + ?,
                    memberships_seen = memberships_seen + ?,
                    reported_memberships = reported_memberships + ?,
                    updated_at = now()
                WHERE scan_name = ?
                """,
                [
                    len(ordered),
                    len(ordered),
                    sum(len(summaries) for _, summaries in ordered),
                    len(records),
                    sum(
                        summary.products_count
                        for _, summaries in ordered
                        for summary in summaries
                    ),
                    scan_name,
                ],
            )
            self.connection.execute("COMMIT")
            return len(source_records)
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def pending_collection_overflow(self, limit: int) -> list[tuple[int, int, int]]:
        return [
            (int(row[0]), int(row[1]), int(row[2]))
            for row in self.connection.execute(
                """
                SELECT collection_id, next_offset, products_count
                FROM collection_overflow_scans
                WHERE next_offset < products_count
                ORDER BY next_offset, collection_id
                LIMIT ?
                """,
                [limit],
            ).fetchall()
        ]

    def apply_collection_overflow_batch(
        self,
        pages: Sequence[tuple[int, int, Sequence[CategoryRecordLike]]],
    ) -> int:
        if not pages:
            return 0
        mismatches = int(
            self.connection.execute(
                """
                SELECT count(*)
                FROM (
                    SELECT unnest(?::BIGINT[]) AS collection_id,
                           unnest(?::INTEGER[]) AS expected_offset
                ) AS incoming
                LEFT JOIN collection_overflow_scans USING (collection_id)
                WHERE collection_overflow_scans.collection_id IS NULL
                   OR collection_overflow_scans.next_offset != incoming.expected_offset
                """,
                [
                    [collection_id for collection_id, _, _ in pages],
                    [offset for _, offset, _ in pages],
                ],
            ).fetchone()[0]
        )
        if mismatches:
            raise ValueError("collection overflow cursor changed unexpectedly")
        records = [record for _, _, page_records in pages for record in page_records]
        candidate_records, source_records = self._collection_record_state(records)
        self.connection.execute("BEGIN")
        try:
            self._upsert_collection_records(candidate_records, source_records)
            self.connection.execute(
                """
                UPDATE collection_overflow_scans SET
                    next_offset = least(
                        collection_overflow_scans.products_count,
                        incoming.expected_offset + 100
                    ),
                    pages_scanned = pages_scanned + 1,
                    updated_at = now()
                FROM (
                    SELECT unnest(?::BIGINT[]) AS collection_id,
                           unnest(?::INTEGER[]) AS expected_offset
                ) AS incoming
                WHERE collection_overflow_scans.collection_id = incoming.collection_id
                """,
                [
                    [collection_id for collection_id, _, _ in pages],
                    [offset for _, offset, _ in pages],
                ],
            )
            self.connection.execute(
                """
                UPDATE collection_scans SET
                    memberships_seen = memberships_seen + ?, updated_at = now()
                WHERE scan_name = 'public-collections-v1'
                """,
                [len(records)],
            )
            self.connection.execute("COMMIT")
            return len(source_records)
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def _collection_record_state(
        self, records: Sequence[CategoryRecordLike]
    ) -> tuple[dict[int, CategoryRecordLike], dict[int, CategoryRecordLike]]:
        unique_records = {record.product_id: record for record in records}
        if not unique_records:
            return unique_records, unique_records
        product_ids = list(unique_records)
        existing_candidates = {
            int(row[0])
            for row in self.connection.execute(
                """
                SELECT product_id FROM collection_candidates
                WHERE product_id IN (SELECT unnest(?))
                """,
                [product_ids],
            ).fetchall()
        }
        existing_sources = {
            int(row[0])
            for row in self.connection.execute(
                """
                SELECT product_id FROM source_products
                WHERE product_id IN (SELECT unnest(?))
                """,
                [product_ids],
            ).fetchall()
        }
        return (
            {
                product_id: record
                for product_id, record in unique_records.items()
                if product_id not in existing_candidates
            },
            {
                product_id: record
                for product_id, record in unique_records.items()
                if product_id not in existing_sources
            },
        )

    def _upsert_collection_records(
        self,
        candidate_records: dict[int, CategoryRecordLike],
        source_records: dict[int, CategoryRecordLike],
    ) -> None:
        if candidate_records:
            candidate_values = list(candidate_records.values())
            candidate_ids = [record.product_id for record in candidate_values]
            candidate_slugs = [record.slug for record in candidate_values]
            self.connection.execute(
                """
                INSERT INTO collection_candidates (product_id, slug, new_product_id)
                SELECT unnest(?::BIGINT[]), unnest(?::VARCHAR[]), unnest(?::BOOLEAN[])
                ON CONFLICT (product_id) DO NOTHING
                """,
                [
                    candidate_ids,
                    candidate_slugs,
                    [product_id in source_records for product_id in candidate_ids],
                ],
            )
        if not source_records:
            return
        source_values = list(source_records.values())
        product_ids = [record.product_id for record in source_values]
        slugs = [record.slug for record in source_values]
        self.connection.execute(
            """
            INSERT INTO source_products (product_id, slug)
            SELECT unnest(?::BIGINT[]), unnest(?::VARCHAR[])
            ON CONFLICT (product_id) DO NOTHING
            """,
            [product_ids, slugs],
        )
        self.connection.execute(
            """
            INSERT INTO products (slug, producthunt_url)
            SELECT incoming.slug, 'https://www.producthunt.com/products/' || incoming.slug
            FROM (SELECT unnest(?::VARCHAR[]) AS slug) AS incoming
            WHERE NOT EXISTS (
                SELECT 1 FROM product_aliases WHERE alias_slug = incoming.slug
            )
            ON CONFLICT (slug) DO NOTHING
            """,
            [slugs],
        )

    def collection_counts(self) -> dict[str, int]:
        candidates = self.connection.execute(
            """
            SELECT count(*), count(*) FILTER (WHERE new_product_id)
            FROM collection_candidates
            """
        ).fetchone()
        scan = self.connection.execute(
            """
            SELECT coalesce(sum(total_count), 0), coalesce(sum(pages_scanned), 0),
                   coalesce(sum(collections_seen), 0),
                   coalesce(sum(memberships_seen), 0),
                   coalesce(sum(reported_memberships), 0),
                   count(*) FILTER (WHERE next_page >= pages_count)
            FROM collection_scans
            """
        ).fetchone()
        overflow = self.connection.execute(
            """
            SELECT count(*), count(*) FILTER (WHERE next_offset >= products_count),
                   coalesce(sum(pages_scanned), 0)
            FROM collection_overflow_scans
            """
        ).fetchone()
        return {
            "unique_candidates": int(candidates[0]),
            "new_product_ids": int(candidates[1]),
            "collections_reported": int(scan[0]),
            "root_pages_scanned": int(scan[1]),
            "collections_scanned": int(scan[2]),
            "memberships_seen": int(scan[3]),
            "reported_memberships": int(scan[4]),
            "scans_complete": int(scan[5]),
            "overflow_collections": int(overflow[0]),
            "overflow_complete": int(overflow[1]),
            "overflow_pages_scanned": int(overflow[2]),
        }

    def stage_external_products(
        self,
        source: str,
        rows: Sequence[
            tuple[
                str,
                str | None,
                str | None,
                str | None,
                str | None,
                list[str],
                str,
                int | None,
            ]
        ],
    ) -> None:
        self.connection.execute("BEGIN")
        try:
            if rows:
                self.connection.executemany(
                    """
                    INSERT INTO external_products (
                        slug, source, name, tagline, description, website_url,
                        categories, producthunt_url, product_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (slug) DO UPDATE SET
                        name = coalesce(excluded.name, external_products.name),
                        tagline = coalesce(excluded.tagline, external_products.tagline),
                        description = coalesce(excluded.description, external_products.description),
                        website_url = coalesce(excluded.website_url, external_products.website_url),
                        categories = CASE WHEN length(excluded.categories) > 0
                            THEN excluded.categories ELSE external_products.categories END,
                        product_id = coalesce(excluded.product_id, external_products.product_id)
                    """,
                    [
                        (
                            slug,
                            source,
                            name,
                            tagline,
                            description,
                            website_url,
                            categories,
                            producthunt_url,
                            product_id,
                        )
                        for (
                            slug,
                            name,
                            tagline,
                            description,
                            website_url,
                            categories,
                            producthunt_url,
                            product_id,
                        ) in rows
                    ],
                )
                self.connection.executemany(
                    """
                    INSERT INTO archive_candidates (path_kind, slug)
                    VALUES ('products', ?)
                    ON CONFLICT (path_kind, slug) DO UPDATE SET last_seen_at = now()
                    """,
                    [(row[0],) for row in rows],
                )
                self.connection.executemany(
                    """
                    INSERT INTO products (slug, producthunt_url, name, tagline)
                    SELECT ?, ?, ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM product_aliases WHERE alias_slug = ?
                    )
                    ON CONFLICT (slug) DO NOTHING
                    """,
                    [(row[0], row[6], row[1], row[2], row[0]) for row in rows],
                )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def record_external_product_relations(
        self,
        source_products: Sequence[tuple[int, str]],
        aliases: Sequence[tuple[str, str]],
    ) -> None:
        """Record Product Hunt ID and rename evidence from a public product dump."""
        self.connection.execute("BEGIN")
        try:
            if source_products:
                self.connection.executemany(
                    """
                    INSERT INTO source_products (product_id, slug)
                    VALUES (?, ?)
                    ON CONFLICT (product_id) DO UPDATE SET last_seen_at = now()
                    """,
                    source_products,
                )
            if aliases:
                self.connection.executemany(
                    """
                    INSERT INTO product_aliases (
                        alias_slug, canonical_slug, alias_url, canonical_url
                    )
                    SELECT ?, ?,
                           'https://www.producthunt.com/products/' || ?,
                           'https://www.producthunt.com/products/' || ?
                    WHERE EXISTS (SELECT 1 FROM products WHERE slug = ?)
                    ON CONFLICT (alias_slug) DO UPDATE SET
                        canonical_slug = excluded.canonical_slug,
                        canonical_url = excluded.canonical_url
                    """,
                    [
                        (alias, canonical, alias, canonical, canonical)
                        for alias, canonical in aliases
                    ],
                )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def promote_external_products(self) -> int:
        eligible = int(
            self.connection.execute(
                """
                SELECT count(*)
                FROM products JOIN external_products USING (slug)
                WHERE (products.status = 'unavailable'
                       OR (products.status = 'fetched' AND products.description = ''))
                  AND external_products.name IS NOT NULL
                  AND external_products.tagline IS NOT NULL
                  AND external_products.website_url IS NOT NULL
                """
            ).fetchone()[0]
        )
        self.connection.execute(
            """
            UPDATE products SET
                name = external.name,
                tagline = external.tagline,
                -- Some historical Product Hunt records explicitly contain no
                -- description. Fall back to Product Hunt's own tagline.
                description = coalesce(external.description, external.tagline),
                website_url = external.website_url,
                categories = external.categories,
                status = 'fetched', http_status = NULL, fetched_at = now(),
                content_hash = sha256(concat_ws(chr(31), external.slug, external.name,
                    external.tagline, external.description, external.website_url,
                    array_to_string(external.categories, chr(30)))),
                next_retry_at = NULL,
                last_error = NULL
            FROM external_products AS external
            WHERE products.slug = external.slug
              AND (products.status = 'unavailable'
                   OR (products.status = 'fetched' AND products.description = ''))
              AND external.name IS NOT NULL
              AND external.tagline IS NOT NULL
              AND external.website_url IS NOT NULL
            """
        )
        return eligible

    def external_counts(self) -> dict[str, int]:
        row = self.connection.execute(
            """
            SELECT count(*), count(*) FILTER (
                WHERE name IS NOT NULL AND tagline IS NOT NULL
                  AND website_url IS NOT NULL
            ) FROM external_products
            """
        ).fetchone()
        return {"staged": int(row[0]), "complete": int(row[1])}

    def claim_website_enrichments(
        self, limit: int, max_attempts: int
    ) -> list[tuple[str, str, str, int, bool, bool]]:
        """Claim fetched rows whose public website may fill missing text fields."""
        rows = self.connection.execute(
            """
            SELECT products.slug, products.name, products.website_url,
                   coalesce(enrichment.attempts, 0) + 1 AS attempt,
                   coalesce(trim(products.tagline), '') = '' AS needs_tagline,
                   coalesce(trim(products.description), '') = '' AS needs_description
            FROM products
            LEFT JOIN website_enrichments AS enrichment USING (slug)
            WHERE products.status = 'fetched'
              AND (
                  coalesce(trim(products.tagline), '') = ''
                  OR coalesce(trim(products.description), '') = ''
              )
              AND regexp_matches(products.website_url, '^https?://[^ ]+$', 'i')
              AND lower(products.website_url) NOT LIKE '%producthunt.com/%'
              AND (
                  enrichment.slug IS NULL
                  OR (
                      enrichment.outcome = 'retry'
                      AND enrichment.attempts < ?
                  )
              )
            ORDER BY products.slug
            LIMIT ?
            """,
            [max_attempts, limit],
        ).fetchall()
        if not rows:
            return []
        self.connection.execute("BEGIN")
        try:
            self.connection.executemany(
                """
                INSERT INTO website_enrichments (
                    slug, requested_url, outcome, attempts, updated_at
                ) VALUES (?, ?, 'claimed', ?, now())
                ON CONFLICT (slug) DO UPDATE SET
                    requested_url = excluded.requested_url,
                    outcome = 'claimed',
                    attempts = excluded.attempts,
                    last_error = NULL,
                    updated_at = now()
                """,
                [(slug, website_url, attempt) for slug, _, website_url, attempt, _, _ in rows],
            )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise
        return [
            (str(slug), str(name), str(url), int(attempt), bool(tagline), bool(description))
            for slug, name, url, attempt, tagline, description in rows
        ]

    def apply_website_enrichments(
        self,
        results: Sequence[WebsiteEnrichmentLike],
        *,
        apply_fields: bool = True,
    ) -> dict[str, int]:
        """Persist fetch provenance and fill only fields that are still empty."""
        if not results:
            return {"processed": 0, "taglines_applied": 0, "descriptions_applied": 0}
        from ph_catalog.parsing import content_hash

        taglines_applied = 0
        descriptions_applied = 0
        self.connection.execute("BEGIN")
        try:
            for result in results:
                current = self.connection.execute(
                    """
                    SELECT coalesce(trim(tagline), '') = '',
                           coalesce(trim(description), '') = ''
                    FROM products WHERE slug = ? AND status = 'fetched'
                    """,
                    [result.slug],
                ).fetchone()
                can_apply_tagline = bool(
                    apply_fields
                    and current
                    and current[0]
                    and result.outcome == "enriched"
                    and result.tagline
                    and result.tagline_source
                )
                can_apply_description = bool(
                    apply_fields
                    and current
                    and current[1]
                    and result.outcome == "enriched"
                    and result.description
                    and result.description_source
                )
                self.connection.execute(
                    """
                    INSERT INTO website_enrichments (
                        slug, requested_url, final_url, outcome, attempts, http_status,
                        tagline_candidate, tagline_source, description_candidate,
                        description_source, applied_tagline, applied_description,
                        response_hash, last_error, fetched_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), now())
                    ON CONFLICT (slug) DO UPDATE SET
                        requested_url = excluded.requested_url,
                        final_url = excluded.final_url,
                        outcome = excluded.outcome,
                        attempts = greatest(website_enrichments.attempts, excluded.attempts),
                        http_status = excluded.http_status,
                        tagline_candidate = excluded.tagline_candidate,
                        tagline_source = excluded.tagline_source,
                        description_candidate = excluded.description_candidate,
                        description_source = excluded.description_source,
                        applied_tagline = excluded.applied_tagline,
                        applied_description = excluded.applied_description,
                        response_hash = excluded.response_hash,
                        last_error = excluded.last_error,
                        fetched_at = now(),
                        updated_at = now()
                    """,
                    [
                        result.slug,
                        result.requested_url,
                        result.final_url,
                        result.outcome,
                        result.attempt,
                        result.http_status,
                        result.tagline,
                        result.tagline_source,
                        result.description,
                        result.description_source,
                        can_apply_tagline,
                        can_apply_description,
                        result.response_hash,
                        result.error,
                    ],
                )
                if not current or not (can_apply_tagline or can_apply_description):
                    continue
                self.connection.execute(
                    """
                    UPDATE products SET
                        tagline = CASE WHEN ? THEN ? ELSE tagline END,
                        tagline_source = CASE WHEN ? THEN ? ELSE tagline_source END,
                        description = CASE WHEN ? THEN ? ELSE description END,
                        description_source = CASE WHEN ? THEN ? ELSE description_source END
                    WHERE slug = ? AND status = 'fetched'
                    """,
                    [
                        can_apply_tagline,
                        result.tagline,
                        can_apply_tagline,
                        f"website:{result.tagline_source}",
                        can_apply_description,
                        result.description,
                        can_apply_description,
                        f"website:{result.description_source}",
                        result.slug,
                    ],
                )
                product = self.connection.execute(
                    """
                    SELECT slug, producthunt_url, name, tagline, description,
                           website_url, coalesce(categories, []::VARCHAR[])
                    FROM products WHERE slug = ?
                    """,
                    [result.slug],
                ).fetchone()
                assert product is not None
                data = ProductData(
                    slug=str(product[0]),
                    producthunt_url=str(product[1]),
                    name=product[2],
                    tagline=product[3],
                    description=product[4],
                    website_url=product[5],
                    categories=list(product[6]),
                )
                self.connection.execute(
                    "UPDATE products SET content_hash = ? WHERE slug = ?",
                    [content_hash(data), result.slug],
                )
                taglines_applied += int(can_apply_tagline)
                descriptions_applied += int(can_apply_description)
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise
        return {
            "processed": len(results),
            "taglines_applied": taglines_applied,
            "descriptions_applied": descriptions_applied,
        }

    def website_enrichment_counts(self) -> dict[str, int]:
        outcomes = {
            str(outcome): int(count)
            for outcome, count in self.connection.execute(
                "SELECT outcome, count(*) FROM website_enrichments GROUP BY outcome"
            ).fetchall()
        }
        applied = self.connection.execute(
            """
            SELECT count(*) FILTER (WHERE applied_tagline),
                   count(*) FILTER (WHERE applied_description)
            FROM website_enrichments
            """
        ).fetchone()
        outcomes["taglines_applied"] = int(applied[0])
        outcomes["descriptions_applied"] = int(applied[1])
        return outcomes

    def apply_stored_website_enrichments(self, slugs: Sequence[str]) -> dict[str, int]:
        """Apply explicitly reviewed website candidates to fields that remain empty."""
        if not slugs:
            return {"processed": 0, "taglines_applied": 0, "descriptions_applied": 0}
        rows = self.connection.execute(
            """
            SELECT slug, requested_url, final_url, attempts, outcome, http_status,
                   tagline_candidate, tagline_source, description_candidate,
                   description_source, response_hash, last_error
            FROM website_enrichments
            WHERE outcome = 'enriched' AND slug IN (SELECT unnest(?))
            ORDER BY slug
            """,
            [list(slugs)],
        ).fetchall()
        results = [
            SimpleNamespace(
                slug=row[0],
                requested_url=row[1],
                final_url=row[2],
                attempt=int(row[3]),
                outcome=row[4],
                http_status=row[5],
                tagline=row[6],
                tagline_source=row[7],
                description=row[8],
                description_source=row[9],
                response_hash=row[10],
                error=row[11],
            )
            for row in rows
        ]
        return self.apply_website_enrichments(results, apply_fields=True)

    def exclude_incomplete_copy_products(self) -> dict[str, int]:
        """Quarantine fetched rows still missing tagline or description, then remove them."""
        reason = "missing tagline or description after website enrichment"
        targets = [
            str(row[0])
            for row in self.connection.execute(
                """
                SELECT slug FROM products
                WHERE status = 'fetched'
                  AND (
                      coalesce(trim(tagline), '') = ''
                      OR coalesce(trim(description), '') = ''
                  )
                ORDER BY slug
                """
            ).fetchall()
        ]
        if not targets:
            return {"excluded_products": 0, "excluded_aliases": 0}
        aliases = int(
            self.connection.execute(
                """
                SELECT count(*) FROM product_aliases
                WHERE canonical_slug IN (SELECT unnest(?))
                """,
                [targets],
            ).fetchone()[0]
        )
        self.connection.execute("BEGIN")
        try:
            self.connection.execute(
                """
                INSERT INTO excluded_product_aliases
                SELECT aliases.alias_slug, aliases.canonical_slug, aliases.alias_url,
                       aliases.canonical_url, aliases.discovered_at, aliases.http_status,
                       ?, now()
                FROM product_aliases AS aliases
                WHERE aliases.canonical_slug IN (SELECT unnest(?))
                ON CONFLICT (alias_slug) DO NOTHING
                """,
                [reason, targets],
            )
            self.connection.execute(
                """
                INSERT INTO excluded_products
                SELECT products.slug, products.producthunt_url, products.name,
                       products.tagline, products.description, products.website_url,
                       products.categories, products.status, products.attempts,
                       products.http_status, products.first_seen_at, products.fetched_at,
                       products.content_hash, products.source_lastmod,
                       products.next_retry_at, products.last_error,
                       products.tagline_source, products.description_source, ?, now()
                FROM products
                WHERE products.slug IN (SELECT unnest(?))
                ON CONFLICT (slug) DO NOTHING
                """,
                [reason, targets],
            )
            self.connection.execute(
                "DELETE FROM product_aliases WHERE canonical_slug IN (SELECT unnest(?))",
                [targets],
            )
            self.connection.execute(
                "DELETE FROM products WHERE slug IN (SELECT unnest(?))", [targets]
            )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise
        return {"excluded_products": len(targets), "excluded_aliases": aliases}

    def exclusion_counts(self) -> dict[str, int]:
        row = self.connection.execute(
            """
            SELECT (SELECT count(*) FROM excluded_products),
                   (SELECT count(*) FROM excluded_product_aliases)
            """
        ).fetchone()
        return {"products": int(row[0]), "aliases": int(row[1])}

    def apply_catalog_enrichment(self, records: Sequence[CatalogRecord]) -> int:
        complete = [record for record in records if record.data.is_complete_enough]
        if not complete:
            return 0
        slugs = [record.data.slug for record in complete]
        placeholders = ",".join("?" for _ in slugs)
        before = int(
            self.connection.execute(
                f"SELECT count(*) FROM products "
                f"WHERE status = 'fetched' AND slug IN ({placeholders})",  # noqa: S608
                slugs,
            ).fetchone()[0]
        )
        self.connection.execute("BEGIN")
        try:
            self.connection.executemany(
                """
                UPDATE products SET
                    name = coalesce(?, name),
                    tagline = coalesce(?, tagline),
                    description = coalesce(?, description),
                    website_url = coalesce(?, website_url),
                    categories = CASE WHEN ? IS NOT NULL AND length(?) > 0
                        THEN ? ELSE categories END,
                    status = 'fetched', http_status = 200, fetched_at = now(),
                    content_hash = ?, next_retry_at = NULL, last_error = NULL
                WHERE slug = ?
                """,
                [
                    (
                        record.data.name,
                        record.data.tagline,
                        record.data.description,
                        record.data.website_url,
                        record.data.categories,
                        record.data.categories,
                        record.data.categories,
                        record.content_hash,
                        record.data.slug,
                    )
                    for record in complete
                ],
            )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise
        return len(complete) - before

    def manifest_scan_state(self, name: str) -> dict[str, int] | None:
        row = self.connection.execute(
            """
            SELECT next_id, stop_id, requests, discovered, unavailable
            FROM manifest_scans WHERE scan_name = ?
            """,
            [name],
        ).fetchone()
        if row is None:
            return None
        return {
            "next_id": int(row[0]),
            "stop_id": int(row[1]),
            "requests": int(row[2]),
            "discovered": int(row[3]),
            "unavailable": int(row[4]),
        }

    def claim_products(self, limit: int, max_attempts: int) -> list[PendingProduct]:
        """Increment attempts before I/O so an interrupted fetch remains resumable."""
        rows = self.connection.execute(
            """
            SELECT slug, producthunt_url, attempts + 1 AS attempt
            FROM products
            WHERE status IN ('pending', 'retry', 'parse_failed')
              AND attempts < ?
              AND (next_retry_at IS NULL OR next_retry_at <= current_timestamp)
            ORDER BY
              CASE status WHEN 'pending' THEN 0 WHEN 'retry' THEN 1 ELSE 2 END,
              source_lastmod DESC NULLS LAST,
              first_seen_at,
              slug
            LIMIT ?
            """,
            [max_attempts, limit],
        ).fetchall()
        if not rows:
            return []
        self.connection.execute("BEGIN")
        try:
            self.connection.executemany(
                """
                UPDATE products
                SET attempts = ?, status = 'retry', next_retry_at = NULL
                WHERE slug = ?
                """,
                [(attempt, slug) for slug, _, attempt in rows],
            )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise
        return [PendingProduct(slug, url, attempt) for slug, url, attempt in rows]

    def apply_results(self, results: Sequence[FetchResult]) -> None:
        if not results:
            return
        self.connection.execute("BEGIN")
        try:
            for result in results:
                self._apply_result(result)
            errors = [
                (
                    result.requested_slug,
                    result.attempt,
                    result.http_status,
                    result.error,
                    result.proxy_label,
                )
                for result in results
                if result.error
            ]
            if errors:
                self.connection.executemany(
                    """
                    INSERT INTO crawl_errors
                        (slug, attempt, http_status, error, proxy_label)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    errors,
                )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def _apply_result(self, result: FetchResult) -> None:
        if result.status is ProductStatus.FETCHED and result.data is not None:
            data = result.data
            if result.is_alias:
                self.connection.execute(
                    """
                    INSERT INTO products (
                        slug, producthunt_url, name, tagline, description, website_url,
                        categories, status, attempts, http_status, fetched_at, content_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'fetched', ?, ?, current_timestamp, ?)
                    ON CONFLICT (slug) DO UPDATE SET
                        producthunt_url = excluded.producthunt_url,
                        name = excluded.name,
                        tagline = excluded.tagline,
                        description = excluded.description,
                        website_url = excluded.website_url,
                        categories = excluded.categories,
                        status = 'fetched',
                        attempts = greatest(products.attempts, excluded.attempts),
                        http_status = excluded.http_status,
                        fetched_at = now(),
                        content_hash = excluded.content_hash,
                        next_retry_at = NULL,
                        last_error = NULL
                    """,
                    [
                        data.slug,
                        result.canonical_url,
                        data.name,
                        data.tagline,
                        data.description,
                        data.website_url,
                        data.categories,
                        result.attempt,
                        result.http_status,
                        result.content_hash,
                    ],
                )
                self.connection.execute(
                    "DELETE FROM products WHERE slug = ?", [result.requested_slug]
                )
                self.connection.execute(
                    """
                    INSERT INTO product_aliases (
                        alias_slug, canonical_slug, alias_url, canonical_url, http_status
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (alias_slug) DO UPDATE SET
                        canonical_slug = excluded.canonical_slug,
                        canonical_url = excluded.canonical_url,
                        http_status = excluded.http_status
                    """,
                    [
                        result.requested_slug,
                        result.canonical_slug,
                        result.requested_url,
                        result.canonical_url,
                        result.http_status,
                    ],
                )
                return

            self.connection.execute(
                """
                UPDATE products SET
                    name = ?, tagline = ?, description = ?, website_url = ?, categories = ?,
                    status = 'fetched', http_status = ?, fetched_at = now(),
                    content_hash = ?, next_retry_at = NULL, last_error = NULL
                WHERE slug = ?
                """,
                [
                    data.name,
                    data.tagline,
                    data.description,
                    data.website_url,
                    data.categories,
                    result.http_status,
                    result.content_hash,
                    result.requested_slug,
                ],
            )
            return

        self.connection.execute(
            """
            UPDATE products SET
                status = ?, http_status = ?, next_retry_at = ?, last_error = ?
            WHERE slug = ?
            """,
            [
                result.status.value,
                result.http_status,
                result.retry_at,
                result.error,
                result.requested_slug,
            ],
        )

    def counts(self) -> dict[str, int]:
        result = {
            str(status): int(count)
            for status, count in self.connection.execute(
                "SELECT status, count(*) FROM products GROUP BY status ORDER BY status"
            ).fetchall()
        }
        result["aliases"] = int(
            self.connection.execute("SELECT count(*) FROM product_aliases").fetchone()[0]
        )
        result["ready_retry"] = int(
            self.connection.execute(
                "SELECT count(*) FROM products WHERE status = 'retry' AND next_retry_at IS NULL"
            ).fetchone()[0]
        )
        return result

    def duplicate_counts(self) -> dict[str, int]:
        slug_duplicates = self.connection.execute(
            """
            SELECT count(*) FROM (
                SELECT slug FROM products GROUP BY slug HAVING count(*) > 1
            )
            """
        ).fetchone()[0]
        url_duplicates = self.connection.execute(
            """
            SELECT count(*) FROM (
                SELECT producthunt_url FROM products
                GROUP BY producthunt_url HAVING count(*) > 1
            )
            """
        ).fetchone()[0]
        return {"slug": int(slug_duplicates), "producthunt_url": int(url_duplicates)}

    def requeue(self, statuses: Sequence[str], *, reset_attempts: bool = False) -> int:
        if not statuses:
            return 0
        placeholders = ",".join("?" for _ in statuses)
        attempts_sql = ", attempts = 0" if reset_attempts else ""
        changed = int(
            self.connection.execute(
                f"SELECT count(*) FROM products WHERE status IN ({placeholders})",
                list(statuses),
            ).fetchone()[0]
        )
        self.connection.execute(
            f"""
            UPDATE products
            SET status = 'retry', next_retry_at = NULL{attempts_sql}
            WHERE status IN ({placeholders})
            """,  # noqa: S608 - placeholders are generated, not user supplied
            list(statuses),
        )
        return changed

    def snapshot(self, output: Path) -> int:
        output.parent.mkdir(parents=True, exist_ok=True)
        escaped_path = str(output.resolve()).replace("'", "''")
        self.connection.execute(
            f"""
            COPY (
                SELECT slug, name, tagline, description, website_url,
                       coalesce(categories, []::VARCHAR[]) AS categories,
                       producthunt_url
                FROM products
                WHERE status = 'fetched'
                ORDER BY slug
            ) TO '{escaped_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """  # noqa: S608 - escaped local path, not SQL data
        )
        return int(
            self.connection.execute(
                "SELECT count(*) FROM products WHERE status = 'fetched'"
            ).fetchone()[0]
        )

    def restore_snapshot(self, snapshot: Path) -> int:
        """Seed missing fetched rows from a seven-field catalogue snapshot."""
        if not snapshot.is_file():
            raise ValueError(f"snapshot does not exist: {snapshot}")
        escaped_path = str(snapshot.resolve()).replace("'", "''")
        before = int(self.connection.execute("SELECT count(*) FROM products").fetchone()[0])
        self.connection.execute(
            f"""
            INSERT INTO products (
                slug, name, tagline, description, website_url, categories,
                producthunt_url, status, attempts, http_status, fetched_at,
                content_hash, tagline_source, description_source
            )
            SELECT slug, name, tagline, description, website_url,
                   coalesce(categories, []::VARCHAR[]), producthunt_url,
                   'fetched', 0, 200, current_timestamp,
                   sha256(concat_ws(chr(31), slug, name, tagline, description,
                       website_url, array_to_string(categories, chr(30)))),
                   'snapshot', 'snapshot'
            FROM read_parquet('{escaped_path}')
            ON CONFLICT (slug) DO NOTHING
            """  # noqa: S608 - escaped local path, not SQL data
        )
        after = int(self.connection.execute("SELECT count(*) FROM products").fetchone()[0])
        return after - before

    def begin_run(self, kind: str) -> str:
        run_id = str(self.connection.execute("SELECT uuid()").fetchone()[0])
        self.connection.execute(
            "INSERT INTO crawl_runs (run_id, kind) VALUES (?, ?)", [run_id, kind]
        )
        return run_id

    def finish_run(self, run_id: str, outcome: str, metrics: dict[str, object]) -> None:
        self.connection.execute(
            """
            UPDATE crawl_runs
            SET finished_at = current_timestamp, outcome = ?, metrics = ?::JSON
            WHERE run_id = ?
            """,
            [outcome, orjson.dumps(metrics).decode(), run_id],
        )

    def verify(self) -> dict[str, object]:
        counts = self.counts()
        duplicates = self.duplicate_counts()
        fetched = counts.get("fetched", 0)
        parse_failed = counts.get("parse_failed", 0)
        parsed_with_required_fields = int(
            self.connection.execute(
                """
                SELECT count(*) FROM products
                WHERE status = 'fetched'
                  AND name IS NOT NULL AND name != ''
                  AND description IS NOT NULL AND description != ''
                  AND (tagline IS NOT NULL OR website_url IS NOT NULL)
                """
            ).fetchone()[0]
        )
        return {
            "checked_at": datetime.now(UTC).isoformat(),
            "counts": counts,
            "duplicates": duplicates,
            "complete_fetched": parsed_with_required_fields,
            "fetched_completeness": (parsed_with_required_fields / fetched if fetched else None),
            "reachable_parse_success": (
                fetched / (fetched + parse_failed) if fetched + parse_failed else None
            ),
        }
