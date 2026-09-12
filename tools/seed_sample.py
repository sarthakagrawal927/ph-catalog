#!/usr/bin/env python3
"""Offline sample-data demo for ph-catalog.

Parses the synthetic HTML fixtures in ``samples/`` with the real product-page
parser and loads them into the local DuckDB through the same public API the
network crawler uses (``import_products`` -> ``claim_products`` ->
``apply_results``). No network access is required and no real Product Hunt data
is used; every fixture is a fictional product. Run the existing CLI commands
afterwards to inspect, verify, and export the seeded catalogue:

    uv run ph-catalog --database data/sample.duckdb init
    uv run python tools/seed_sample.py
    uv run ph-catalog --database data/sample.duckdb status
    uv run ph-catalog --database data/sample.duckdb verify
    uv run ph-catalog --database data/sample.duckdb snapshot --output snapshots/sample.parquet
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

from ph_catalog.database import CatalogDatabase
from ph_catalog.models import FetchResult, ProductStatus, SitemapProduct
from ph_catalog.parsing import content_hash, parse_product_page


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _sample_files(samples_dir: Path) -> list[Path]:
    return sorted(samples_dir.glob("*.html"))


def seed(database_path: Path, samples_dir: Path) -> dict[str, object]:
    fixtures = _sample_files(samples_dir)
    if not fixtures:
        raise ValueError(f"no sample HTML fixtures found in {samples_dir}")

    # Inspect before CatalogDatabase opens a writer or recovers interrupted jobs.
    # A demonstration must never claim records from someone's real catalogue.
    if database_path.exists():
        with duckdb.connect(str(database_path), read_only=True) as existing:
            count = existing.execute("SELECT count(*) FROM products").fetchone()[0]
            marker = existing.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = 'sample_demo'"
            ).fetchone()[0]
            slugs = {row[0] for row in existing.execute("SELECT slug FROM products").fetchall()}
            if (count and not marker) or not slugs.issubset({path.stem for path in fixtures}):
                raise ValueError(
                    "Refusing to seed a non-sample catalogue; use a fresh sample database"
                )

    sitemap_products = [
        SitemapProduct(
            path.stem,
            f"https://www.producthunt.com/products/{path.stem}",
        )
        for path in fixtures
    ]
    with CatalogDatabase(database_path) as database:
        database.connection.execute("CREATE TABLE IF NOT EXISTS sample_demo (version INTEGER)")
        inserted, discovered = database.import_products(sitemap_products)
        claimed = database.claim_products(limit=len(sitemap_products), max_attempts=8)
        results: list[FetchResult] = []
        for pending in claimed:
            html = (samples_dir / f"{pending.slug}.html").read_bytes()
            final_url = f"https://www.producthunt.com/products/{pending.slug}"
            data = parse_product_page(html, pending.slug, final_url)
            results.append(
                FetchResult(
                    requested_slug=pending.slug,
                    canonical_slug=data.slug,
                    requested_url=pending.producthunt_url,
                    canonical_url=data.producthunt_url,
                    attempt=pending.attempt,
                    status=ProductStatus.FETCHED,
                    http_status=200,
                    proxy_label="sample",
                    elapsed_ms=0,
                    data=data,
                    content_hash=content_hash(data),
                )
            )
        database.apply_results(results)

        rows = database.connection.execute(
            """
            SELECT slug, name, tagline, website_url
            FROM products
            WHERE status = 'fetched'
            ORDER BY slug
            """
        ).fetchall()
        products = [
            {"slug": slug, "name": name, "tagline": tagline, "website_url": website_url}
            for slug, name, tagline, website_url in rows
        ]
    return {
        "fixtures": len(fixtures),
        "inserted": inserted,
        "discovered": discovered,
        "parsed": len(results),
        "products": products,
    }


def main(argv: list[str] | None = None) -> None:
    root = _project_root()
    parser = argparse.ArgumentParser(description="Load synthetic sample products offline.")
    parser.add_argument("--database", type=Path, default=root / "data/sample.duckdb")
    parser.add_argument("--samples-dir", type=Path, default=root / "samples")
    args = parser.parse_args(argv)
    print(json.dumps(seed(args.database, args.samples_dir), indent=2))


if __name__ == "__main__":
    main()
