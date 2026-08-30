from __future__ import annotations

import shutil
import subprocess
import tarfile
from pathlib import Path

import orjson
import pytest

from ph_catalog.compact_export import export_compact_archive
from ph_catalog.database import CatalogDatabase


@pytest.mark.skipif(shutil.which("zstd") is None, reason="zstd is not installed")
def test_compact_archive_contains_useful_tables_and_schema(tmp_path: Path) -> None:
    database_path = tmp_path / "catalog.duckdb"
    output = tmp_path / "catalog.tar.zst"
    with CatalogDatabase(database_path) as database:
        database.connection.execute(
            """
            INSERT INTO products (
                slug, producthunt_url, name, tagline, description, website_url,
                categories, status, tagline_source, description_source
            ) VALUES (
                'demo', 'https://www.producthunt.com/products/demo', 'Demo',
                'A tagline', 'A description', 'https://example.com', ['Developer Tools'],
                'fetched', 'producthunt', 'producthunt'
            )
            """
        )
        database.connection.execute(
            "INSERT INTO source_products (product_id, slug) VALUES (42, 'demo')"
        )
        database.connection.execute(
            """
            INSERT INTO archive_post_resolutions (
                post_slug, post_id, product_id, product_slug, outcome
            ) VALUES ('demo-launch', 7, 42, 'demo', 'resolved')
            """
        )
        database.connection.execute(
            """
            INSERT INTO product_aliases (
                alias_slug, canonical_slug, alias_url, canonical_url
            ) VALUES (
                'old-demo', 'demo', 'https://www.producthunt.com/products/old-demo',
                'https://www.producthunt.com/products/demo'
            )
            """
        )
        result = export_compact_archive(database, output)

    assert result["tables"]["products.csv"] == 1
    unpacked = tmp_path / "unpacked"
    unpacked.mkdir()
    decompressed = subprocess.run(  # noqa: S603
        [shutil.which("zstd") or "zstd", "-q", "-d", "-c", str(output)],
        check=True,
        capture_output=True,
    ).stdout
    tar_path = tmp_path / "catalog.tar"
    tar_path.write_bytes(decompressed)
    with tarfile.open(tar_path) as archive:
        archive.extractall(unpacked, filter="data")

    schema = orjson.loads((unpacked / "schema.json").read_bytes())
    assert schema["tables"]["launches.csv"]["rows"] == 1
    assert "demo-launch" in (unpacked / "launches.csv").read_text()
