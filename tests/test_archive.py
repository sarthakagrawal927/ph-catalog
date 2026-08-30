from pathlib import Path

from ph_catalog.archive import (
    ArquivoPtImporter,
    CommonCrawlDirectImporter,
    IndependentCdxImporter,
    InternetArchiveDirectImporter,
    archive_slug,
    internet_archive_legacy_slug,
)
from ph_catalog.database import CatalogDatabase


def test_archive_slug_extracts_only_the_entity_segment() -> None:
    assert archive_slug("https://www.producthunt.com/products/demo/reviews", "products") == "demo"
    assert archive_slug("https://www.producthunt.com/posts/Old-Name?ref=x", "posts") == "old-name"
    assert archive_slug("https://producthubx.com/products/demo", "products") is None
    assert archive_slug("https://www.producthunt.com/topics/demo", "products") is None
    assert archive_slug("https://www.producthunt.com/products/logo.png", "products") is None
    assert archive_slug("https://www.producthunt.com/products/source.tsx", "products") is None


def test_independent_cdx_parses_plain_and_cdxj_urls() -> None:
    body = b"\n".join(
        [
            b"com,producthunt)/products/demo 20200101 "
            b"https://www.producthunt.com/products/demo text/html 200",
            b'com,producthunt)/products/other 20200101 {"url":'
            b'"https://www.producthunt.com/products/other?launch=x","status":"200"}',
            b"com,example)/products/nope 20200101 https://example.com/products/nope text/html 200",
        ]
    )
    candidates, captures = IndependentCdxImporter._parse_cdx(body, "products")
    assert candidates == {"demo", "other"}
    assert captures == 2


def test_arquivo_pt_jsonl_parsing_deduplicates_entity_slugs() -> None:
    body = b"\n".join(
        [
            b'{"url":"https://www.producthunt.com/products/demo","status":"200"}',
            b'{"url":"https://www.producthunt.com/products/demo/reviews","status":"200"}',
            b'{"url":"https://www.producthunt.com/products/app.js","status":"200"}',
            b'{"timestamp":"20240101000000"}',
            b"malformed",
        ]
    )
    candidates, captures, malformed = ArquivoPtImporter._parse_jsonl(body, "products")
    assert candidates == {"demo"}
    assert captures == 3
    assert malformed == 2


def test_archive_scan_is_deduplicated_and_queues_product_paths(tmp_path: Path) -> None:
    with CatalogDatabase(tmp_path / "catalog.duckdb") as database:
        database.apply_archive_scan("CC-ONE", "products", {"one", "two"}, 7)
        database.apply_archive_scan("CC-ONE", "products", {"one", "two"}, 7)
        database.apply_archive_scan("CC-ONE", "posts", {"launch-one"}, 3)

        assert database.archive_scan_complete("CC-ONE", "products")
        assert database.archive_counts() == {
            "unique_candidates": 3,
            "products": 2,
            "posts": 1,
            "product_candidates_absent_from_live_ids": 2,
            "scans_complete": 2,
            "malformed_index_lines": 0,
            "wayback_paths_complete": 0,
            "wayback_requests": 0,
            "wayback_captures": 0,
            "post_resolved": 0,
            "post_no_product": 0,
            "post_unavailable": 0,
        }
        assert database.total_products() == 2


def test_numeric_post_id_is_not_hidden_by_same_text_product_slug(tmp_path: Path) -> None:
    with CatalogDatabase(tmp_path / "catalog.duckdb") as database:
        database.connection.execute(
            "INSERT INTO source_products (product_id, slug) VALUES (99, '13212')"
        )
        database.apply_archive_scan("numeric-posts", "posts", {"13212"}, 1)

        assert database.pending_archive_post_slugs(10) == ["13212"]


def test_direct_archive_parses_cluster_and_successful_cdx_rows() -> None:
    cluster = CommonCrawlDirectImporter._parse_cluster_line(
        100,
        b"com,producthunt)/products/a 20200101000000\tcdx-00001.gz\t123\t456\t7",
    )
    assert (cluster.key, cluster.filename, cluster.offset, cluster.length) == (
        "com,producthunt)/products/a",
        "cdx-00001.gz",
        123,
        456,
    )

    block = b"\n".join(
        [
            b'com,producthunt)/products/a 20200101000000 {"url":"https://www.producthunt.com/products/a","status":"200"}',
            b'com,producthunt)/products/b 20200101000000 {"url":"https://www.producthunt.com/products/b","status":"404"}',
            b"malformed",
        ]
    )
    urls, malformed = CommonCrawlDirectImporter._parse_cdx_block(block)
    assert urls == {"https://www.producthunt.com/products/a"}
    assert malformed == 1

    all_status_urls, malformed = CommonCrawlDirectImporter._parse_cdx_block(
        block, include_non_200=True
    )
    assert all_status_urls == {
        "https://www.producthunt.com/products/a",
        "https://www.producthunt.com/products/b",
    }
    assert malformed == 1


def test_internet_archive_legacy_routes_map_to_launch_slugs() -> None:
    assert internet_archive_legacy_slug(
        "https://www.producthunt.com/books/Old-Book?ref=home"
    ) == ("books", "old-book")
    assert internet_archive_legacy_slug("https://producthunt.com/tech/demo/flagging") == (
        "tech",
        "demo",
    )
    assert internet_archive_legacy_slug("https://example.com/tech/demo") is None
    assert internet_archive_legacy_slug("https://producthunt.com/topics/demo") is None


def test_internet_archive_index_and_cdx_parsing() -> None:
    chunks = InternetArchiveDirectImporter._parse_item_index(
        b"com,other)/\titem.cdx.gz\t10\t20\n"
        b"com,producthunt)/posts/a 20160101\titem.cdx.gz\t30\t40\n"
    )
    assert [(chunk.key, chunk.offset, chunk.length) for chunk in chunks] == [
        ("com,other)/", 10, 20),
        ("com,producthunt)/posts/a", 30, 40),
    ]

    block = b"\n".join(
        [
            b"com,producthunt)/books/a 20160101 "
            b"https://www.producthunt.com/books/a text/html 200 "
            b"DIGEST - - 123 456 item.warc.gz",
            b"com,producthunt)/tech/b 20160101 "
            b"https://www.producthunt.com/tech/b text/html 404 "
            b"DIGEST - - 123 456 item.warc.gz",
            b"malformed",
        ]
    )
    candidates, captures, malformed = InternetArchiveDirectImporter._parse_item_cdx(block)
    assert candidates == {"a"}
    assert captures == 1
    assert malformed == 1

    candidates, captures, malformed = InternetArchiveDirectImporter._parse_item_cdx(
        block, include_non_200=True
    )
    assert candidates == {"a", "b"}
    assert captures == 2
    assert malformed == 1
