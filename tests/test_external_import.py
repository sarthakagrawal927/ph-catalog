import zipfile
from pathlib import Path

from ph_catalog.database import CatalogDatabase
from ph_catalog.external_import import (
    import_daily_csv_archive,
    import_kaggle_launches,
    import_markdown_archive,
    import_product_dump,
    import_product_metadata,
)


def test_external_import_stages_canonical_slug_and_promotes_complete_row(
    tmp_path: Path,
) -> None:
    launches = tmp_path / "launches.csv"
    launches.write_text(
        "slug,name,tagline,description,website,ph_url,launch_date,topic_slugs\n"
        "launch-one,Archived,Old tagline,Old description,https://old.example,"
        "https://www.producthunt.com/products/archived/launches/launch-one,"
        "2020-01-01T00:00:00,developer-tools|saas\n",
        encoding="utf-8",
    )
    with CatalogDatabase(tmp_path / "catalog.duckdb") as database:
        result = import_kaggle_launches(database, launches)
        assert result.staged_candidates == 1
        database.connection.execute(
            "UPDATE products SET status = 'unavailable' WHERE slug = 'archived'"
        )
        assert database.promote_external_products() == 1
        row = database.connection.execute(
            "SELECT slug, status, categories FROM products"
        ).fetchone()
        assert row == ("archived", "fetched", ["developer-tools", "saas"])


def test_external_promotion_falls_back_to_archival_tagline_for_description(tmp_path):
    database = CatalogDatabase(tmp_path / "catalog.duckdb")
    database.stage_external_products(
        "archive",
        [
            (
                "old-product",
                "Old Product",
                "An archival tagline",
                None,
                "https://example.com",
                ["tech"],
                "https://www.producthunt.com/products/old-product",
                123,
            )
        ],
    )
    database.connection.execute(
        "UPDATE products SET status = 'unavailable' WHERE slug = 'old-product'"
    )

    assert database.promote_external_products() == 1
    assert database.connection.execute(
        "SELECT description, status, last_error FROM products WHERE slug = 'old-product'"
    ).fetchone() == ("An archival tagline", "fetched", None)
    database.close()


def test_product_dump_imports_new_products_ids_and_id_backed_aliases(tmp_path):
    dump = tmp_path / "dump.json"
    dump.write_text(
        '{\n  "users": [\n  ],\n  "products": [\n'
        '  {"id": 10, "slug": "current", "name": "Current", '
        '"tagline": "Now", "description": "Current description", '
        '"website": "https://current.example"},\n'
        '  {"id": 11, "slug": "old-name", "name": "Old", '
        '"tagline": "Then", "description": "Old description", '
        '"website": "https://old.example"},\n'
        '  {"id": 12, "slug": "archive-only", "name": "Archive", '
        '"tagline": "Found", "description": "Archived description", '
        '"website": "https://archive.example"}\n'
        '  "posts": [\n  ]\n}\n',
        encoding="utf-8",
    )
    with CatalogDatabase(tmp_path / "catalog.duckdb") as database:
        database.connection.execute(
            """
            INSERT INTO products (slug, producthunt_url, status)
            VALUES ('current', 'https://www.producthunt.com/products/current', 'fetched')
            """
        )
        database.connection.execute(
            "INSERT INTO source_products (product_id, slug) VALUES (11, 'current')"
        )

        result = import_product_dump(database, dump)

        assert result.source_rows == 3
        assert result.source_ids_added == 2
        assert result.aliases_added == 1
        assert result.staged_candidates == 1
        assert database.connection.execute(
            "SELECT canonical_slug FROM product_aliases WHERE alias_slug = 'old-name'"
        ).fetchone() == ("current",)
        assert database.connection.execute(
            "SELECT product_id, slug FROM source_products ORDER BY product_id"
        ).fetchall() == [(10, "current"), (11, "current"), (12, "archive-only")]
        assert database.connection.execute(
            "SELECT status FROM products WHERE slug = 'archive-only'"
        ).fetchone() == ("pending",)

        repeated = import_product_dump(database, dump)
        assert repeated.source_ids_added == 0
        assert repeated.aliases_added == 0
        assert repeated.staged_candidates == 0


def test_product_metadata_stages_only_complete_unknown_canonical_rows(tmp_path):
    metadata = tmp_path / "metadata.csv"
    metadata.write_text(
        "homepage,url,name,description,title\n"
        "https://archive.example,https://www.producthunt.com/products/archive-only,"
        "Archive Only,Archived description,Archive Only: Historical tagline\n"
        ",https://www.producthunt.com/products/incomplete,Incomplete,,Incomplete\n",
        encoding="utf-8",
    )
    with CatalogDatabase(tmp_path / "catalog.duckdb") as database:
        result = import_product_metadata(database, metadata)

        assert result.source_rows == 2
        assert result.source_product_slugs == 2
        assert result.staged_candidates == 1
        assert database.connection.execute(
            "SELECT slug, tagline, status FROM products ORDER BY slug"
        ).fetchall() == [("archive-only", "Historical tagline", "pending")]


def test_daily_csv_archive_stages_complete_canonical_rows(tmp_path):
    archive = tmp_path / "daily.zip"
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr(
            "output/products_2026-01-01.csv",
            "Name,Tagline,URL,Website,Topics,Description\n"
            "Archive,Found,https://www.producthunt.com/products/archive-only,"
            "https://www.producthunt.com/r/TOKEN,AI,Archived description\n",
        )
    with CatalogDatabase(tmp_path / "catalog.duckdb") as database:
        result = import_daily_csv_archive(database, archive)
        assert result.source_rows == 1
        assert result.staged_candidates == 1
        assert database.connection.execute(
            "SELECT slug, categories FROM external_products"
        ).fetchall() == [("archive-only", ["AI"])]


def test_markdown_archive_stages_complete_canonical_rows(tmp_path):
    archive = tmp_path / "reports.zip"
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr(
            "2026/01/01.md",
            "| 1 | [Archive](<https://www.producthunt.com/products/archive-only>) | "
            "Found<br><details><summary><strong>Full description</strong></summary><br>"
            "Archived &amp; complete<br></details> | 1 | 2 | "
            "[link](<https://www.producthunt.com/r/TOKEN>) |\n",
        )
    with CatalogDatabase(tmp_path / "catalog.duckdb") as database:
        result = import_markdown_archive(database, archive)
        assert result.source_rows == 1
        assert result.staged_candidates == 1
        assert database.connection.execute(
            "SELECT name, tagline, description FROM external_products"
        ).fetchone() == ("Archive", "Found", "Archived & complete")
