import gzip

import httpx

from ph_catalog.config import DEFAULT_SITEMAP_URLS
from ph_catalog.sitemap import discover_products, parse_sitemap


def test_default_manifest_includes_unlinked_numbered_product_shards() -> None:
    assert "https://www.producthunt.com/sitemaps_v3/product_about_sitemap16.xml.gz" in (
        DEFAULT_SITEMAP_URLS
    )
    assert "https://www.producthunt.com/sitemaps_v3/product_alternatives_sitemap6.xml.gz" in (
        DEFAULT_SITEMAP_URLS
    )


def test_parse_gzipped_urlset_deduplicates_at_discovery_layer() -> None:
    xml = b"""<?xml version="1.0"?>
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <url><loc>https://www.producthunt.com/products/alpha</loc><lastmod>2025-01-01</lastmod></url>
      <url><loc>https://www.producthunt.com/products/alpha</loc><lastmod>2025-02-01</lastmod></url>
      <url><loc>https://www.producthunt.com/posts/not-a-product</loc></url>
    </urlset>"""
    children, products = parse_sitemap(gzip.compress(xml))
    assert children == []
    assert [product.slug for product in products] == ["alpha", "alpha"]


def test_parse_product_subpage_as_canonical_product() -> None:
    xml = b"""<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <url><loc>https://www.producthunt.com/products/Alpha/alternatives</loc></url>
      <url><loc>https://www.producthunt.com/products/beta/addons</loc></url>
    </urlset>"""
    _, products = parse_sitemap(xml)
    assert [(item.slug, item.producthunt_url) for item in products] == [
        ("alpha", "https://www.producthunt.com/products/alpha"),
        ("beta", "https://www.producthunt.com/products/beta"),
    ]


async def test_recursive_sitemap_discovery() -> None:
    index = b"""<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <sitemap><loc>https://example.test/one.xml.gz</loc></sitemap>
      <sitemap><loc>https://example.test/two.xml</loc></sitemap>
    </sitemapindex>"""
    one = gzip.compress(
        b"""<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
        <url><loc>https://www.producthunt.com/products/alpha</loc><lastmod>2025-01-01</lastmod></url>
        </urlset>"""
    )
    two = b"""<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <url><loc>https://www.producthunt.com/products/alpha</loc><lastmod>2025-03-01</lastmod></url>
      <url><loc>https://www.producthunt.com/products/beta</loc></url>
    </urlset>"""
    bodies = {"/root.xml": index, "/one.xml.gz": one, "/two.xml": two}

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=bodies[request.url.path])

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await discover_products("https://example.test/root.xml", client=client)

    assert result.sitemap_count == 3
    assert {product.slug for product in result.products} == {"alpha", "beta"}
    alpha = next(product for product in result.products if product.slug == "alpha")
    assert alpha.lastmod.month == 3


async def test_multiple_root_sitemaps_are_unioned() -> None:
    bodies = {
        "/about.xml": b"""<urlset><url><loc>https://www.producthunt.com/products/alpha</loc></url></urlset>""",
        "/imported.xml": b"""<urlset><url><loc>https://www.producthunt.com/products/beta</loc></url></urlset>""",
    }

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=bodies[request.url.path])

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await discover_products(
            ["https://example.test/about.xml", "https://example.test/imported.xml"],
            client=client,
        )

    assert result.sitemap_count == 2
    assert {product.slug for product in result.products} == {"alpha", "beta"}
