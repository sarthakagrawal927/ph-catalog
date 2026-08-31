from __future__ import annotations

from pathlib import Path

import duckdb
import orjson

from ph_catalog.analytics import build_analytics
from ph_catalog.database import CatalogDatabase


def test_build_analytics_materializes_dimensions_and_relative_trends(tmp_path: Path) -> None:
    catalog_path = tmp_path / "catalog.duckdb"
    labels_path = tmp_path / "labels.parquet"
    output_path = tmp_path / "analytics.duckdb"
    summary_path = tmp_path / "summary.json"

    with CatalogDatabase(catalog_path) as database:
        database.connection.execute(
            """
            INSERT INTO products (
                slug, producthunt_url, name, tagline, description, website_url,
                categories, status, tagline_source, description_source
            ) VALUES
                ('alpha', 'https://www.producthunt.com/products/alpha', 'Alpha',
                 'Alpha tagline', 'Alpha description', 'https://example.com/path',
                 ['Developer Tools', 'AI'], 'fetched', 'producthunt', 'producthunt'),
                ('beta', 'https://www.producthunt.com/products/beta', 'Beta',
                 'Beta tagline', 'Beta description', 'https://www.example.com/other',
                 [], 'fetched', 'producthunt', 'producthunt'),
                ('gone', 'https://www.producthunt.com/products/gone', NULL,
                 NULL, NULL, NULL, [], 'unavailable', NULL, NULL)
            """
        )
        database.connection.execute(
            "INSERT INTO source_products (product_id, slug) VALUES (10, 'alpha'), (20, 'beta')"
        )
        database.connection.execute(
            """
            INSERT INTO archive_post_resolutions (
                post_slug, post_id, product_id, product_slug, outcome
            ) VALUES
                ('alpha-launch', 100, 10, 'alpha', 'resolved'),
                ('alpha-relaunch', 300, 10, 'alpha', 'resolved'),
                ('beta-launch', 200, 20, 'beta', 'resolved')
            """
        )

    label_connection = duckdb.connect()
    label_connection.execute(
        """
        CREATE TABLE labels (
            slug VARCHAR, tag VARCHAR, score FLOAT, rank UTINYINT,
            taxonomy_version VARCHAR, method VARCHAR
        )
        """
    )
    label_connection.execute(
        """
        INSERT INTO labels VALUES
            ('alpha', 'developer_tools', 0.95, 1, 'v1', 'test'),
            ('alpha', 'ai_ml', 0.90, 2, 'v1', 'test'),
            ('beta', 'developer_tools', 0.85, 1, 'v1', 'test')
        """
    )
    label_connection.execute(f"COPY labels TO '{labels_path}' (FORMAT PARQUET)")
    label_connection.close()

    result = build_analytics(
        catalog_path,
        labels_path,
        output_path,
        summary_path,
    )

    assert result["table_rows"]["product_facts"] == 2
    connection = duckdb.connect(str(output_path), read_only=True)
    alpha = connection.execute(
        """
        SELECT website_host, source_category_count, launch_count,
               trusted_label_count, top_trusted_label
        FROM product_facts WHERE slug = 'alpha'
        """
    ).fetchone()
    assert alpha == ("example.com", 2, 2, 2, "developer_tools")
    assert connection.execute("SELECT count(*) FROM product_categories").fetchone()[0] == 2
    assert connection.execute("SELECT count(*) FROM product_launches").fetchone()[0] == 3
    assert connection.execute("SELECT count(*) FROM label_cooccurrence").fetchone()[0] == 1
    connection.close()

    summary = orjson.loads(summary_path.read_bytes())
    assert summary["core_metrics"]["products"] == 2
    assert summary["core_metrics"]["products_with_launches"] == 2
    assert summary["important_limitations"][1].startswith("post IDs provide relative")
