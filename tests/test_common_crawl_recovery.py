import gzip

import orjson

from ph_catalog.common_crawl_recovery import CommonCrawlRecovery


def test_recovery_cdx_requires_exact_product_hunt_product_page() -> None:
    exact = {
        "url": "https://www.producthunt.com/products/demo?launch=demo",
        "status": "200",
        "mime": "text/html",
        "filename": "crawl-data/demo.warc.gz",
        "offset": "100",
        "length": "200",
    }
    neighboring_host = {**exact, "url": "https://producthubx.com/products/neighbor"}
    nested_page = {**exact, "url": "https://www.producthunt.com/products/demo/reviews"}
    block = b"\n".join(
        [
            b"com,producthunt)/products/demo 20250101000000 " + orjson.dumps(exact),
            b"com,producthubx)/products/neighbor 20250101000001 "
            + orjson.dumps(neighboring_host),
            b"com,producthunt)/products/demo/reviews 20250101000002 "
            + orjson.dumps(nested_page),
        ]
    )

    captures, rows = CommonCrawlRecovery._parse_cdx_block(
        block, "CC-MAIN-TEST", {"demo", "neighbor"}
    )

    assert rows == 3
    assert [(item.slug, item.byte_offset, item.byte_length) for item in captures] == [
        ("demo", 100, 200)
    ]


def test_recovery_cdx_can_select_exact_launch_pages() -> None:
    record = {
        "url": "https://www.producthunt.com/posts/demo-launch?ref=archive",
        "status": "200",
        "mime": "text/html",
        "filename": "crawl-data/demo.warc.gz",
        "offset": "300",
        "length": "400",
    }
    block = b"com,producthunt)/posts/demo-launch 20250101000000 " + orjson.dumps(record)

    captures, rows = CommonCrawlRecovery._parse_cdx_block(
        block, "CC-MAIN-TEST", {"demo-launch"}, "posts"
    )

    assert rows == 1
    assert [(item.slug, item.byte_offset, item.byte_length) for item in captures] == [
        ("demo-launch", 300, 400)
    ]


def test_recovery_extracts_gzipped_warc_http_body() -> None:
    html = b"<html><title>Archived product</title></html>"
    record = (
        b"WARC/1.0\r\nContent-Type: application/http; msgtype=response\r\n\r\n"
        b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n"
        + html
    )

    assert CommonCrawlRecovery._extract_http_body(gzip.compress(record)) == html


def test_recovery_decodes_chunked_http_body() -> None:
    body = b"4\r\nWiki\r\n5\r\npedia\r\n0\r\n\r\n"
    assert CommonCrawlRecovery._decode_chunked(body) == b"Wikipedia"
