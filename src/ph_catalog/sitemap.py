from __future__ import annotations

import asyncio
import gzip
import io
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import unquote, urlparse

import httpx
from lxml import etree

from ph_catalog.config import DEFAULT_USER_AGENT
from ph_catalog.models import SitemapProduct

MAX_SITEMAP_BYTES = 128 * 1024 * 1024
MAX_SITEMAPS = 10_000


@dataclass(slots=True, frozen=True)
class SitemapDiscovery:
    products: list[SitemapProduct]
    sitemap_count: int


def _decode_sitemap(body: bytes) -> bytes:
    if body.startswith(b"\x1f\x8b"):
        with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:
            decoded = stream.read(MAX_SITEMAP_BYTES + 1)
        if len(decoded) > MAX_SITEMAP_BYTES:
            raise ValueError("decompressed sitemap exceeds safety limit")
        return decoded
    if len(body) > MAX_SITEMAP_BYTES:
        raise ValueError("sitemap exceeds safety limit")
    return body


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _slug_from_product_url(url: str) -> str | None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return None
    if parsed.hostname not in {"producthunt.com", "www.producthunt.com"}:
        return None
    parts = [unquote(part) for part in parsed.path.split("/") if part]
    # Supplemental Product Hunt manifests contain URLs such as
    # /products/{slug}/alternatives. They identify the same canonical product.
    if len(parts) < 2 or parts[0] != "products":
        return None
    slug = parts[1].strip().lower()
    if not slug or "/" in slug or len(slug) > 255:
        return None
    return slug


def parse_sitemap(body: bytes) -> tuple[list[str], list[SitemapProduct]]:
    """Return child sitemap URLs and canonical product rows from XML bytes."""
    parser = etree.XMLParser(resolve_entities=False, no_network=True, recover=False)
    root = etree.fromstring(_decode_sitemap(body), parser=parser)
    root_name = etree.QName(root).localname

    if root_name == "sitemapindex":
        children = [
            str(value).strip()
            for value in root.xpath("//*[local-name()='sitemap']/*[local-name()='loc']/text()")
            if str(value).strip()
        ]
        return children, []
    if root_name != "urlset":
        raise ValueError(f"unsupported sitemap root: {root_name}")

    products: list[SitemapProduct] = []
    for node in root.xpath("//*[local-name()='url']"):
        locations = node.xpath("./*[local-name()='loc']/text()")
        if not locations:
            continue
        source_url = str(locations[0]).strip()
        slug = _slug_from_product_url(source_url)
        if not slug:
            continue
        lastmods = node.xpath("./*[local-name()='lastmod']/text()")
        products.append(
            SitemapProduct(
                slug=slug,
                producthunt_url=f"https://www.producthunt.com/products/{slug}",
                lastmod=_parse_datetime(str(lastmods[0])) if lastmods else None,
            )
        )
    return [], products


async def _fetch_sitemap(client: httpx.AsyncClient, url: str) -> bytes:
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            response = await client.get(url)
            response.raise_for_status()
            return response.content
        except (httpx.HTTPError, ValueError) as exc:
            last_error = exc
            if attempt < 2:
                await asyncio.sleep(2**attempt)
    assert last_error is not None
    raise last_error


async def discover_products(
    root_url: str | Sequence[str],
    *,
    client: httpx.AsyncClient | None = None,
    user_agent: str = DEFAULT_USER_AGENT,
) -> SitemapDiscovery:
    """Recursively enumerate product sitemap indexes with in-run URL deduplication."""
    owns_client = client is None
    if client is None:
        client = httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(60.0),
            headers={"User-Agent": user_agent, "Accept": "application/xml,text/xml,*/*"},
        )

    queue = [root_url] if isinstance(root_url, str) else list(root_url)
    if not queue:
        raise ValueError("at least one sitemap URL is required")
    visited: set[str] = set()
    products: dict[str, SitemapProduct] = {}
    try:
        while queue:
            url = queue.pop(0)
            if url in visited:
                continue
            if len(visited) >= MAX_SITEMAPS:
                raise ValueError(f"sitemap recursion exceeded {MAX_SITEMAPS} documents")
            parsed_url = urlparse(url)
            if parsed_url.scheme not in {"http", "https"}:
                raise ValueError(f"unsupported sitemap URL: {url}")
            visited.add(url)
            children, discovered = parse_sitemap(await _fetch_sitemap(client, url))
            queue.extend(child for child in children if child not in visited)
            for product in discovered:
                previous = products.get(product.slug)
                if previous is None or (
                    product.lastmod is not None
                    and (previous.lastmod is None or product.lastmod > previous.lastmod)
                ):
                    products[product.slug] = product
    finally:
        if owns_client:
            await client.aclose()

    return SitemapDiscovery(products=list(products.values()), sitemap_count=len(visited))
