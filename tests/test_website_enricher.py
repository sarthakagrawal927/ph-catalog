from pathlib import Path

import httpx
import pytest

from ph_catalog.database import CatalogDatabase
from ph_catalog.website_enricher import (
    UnsafeWebsiteUrl,
    WebsiteEnricher,
    WebsiteEnrichmentConfig,
    _validate_public_url,
)


def test_website_enrichment_never_exceeds_two_requests_per_second() -> None:
    with pytest.raises(ValueError, match="at most 2 RPS"):
        WebsiteEnrichmentConfig(requests_per_second=2.1)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/secret",
        "http://169.254.169.254/latest/meta-data",
        "http://localhost/admin",
        "file:///etc/passwd",
        "https://user:password@example.com/",
    ],
)
async def test_public_url_validation_rejects_local_and_credentialed_urls(url: str) -> None:
    with pytest.raises(UnsafeWebsiteUrl):
        await _validate_public_url(url)


@pytest.mark.asyncio
async def test_enricher_returns_selected_values_and_sources(tmp_path: Path) -> None:
    class StubWebsiteEnricher(WebsiteEnricher):
        async def _fetch(
            self, client: httpx.AsyncClient, requested_url: str
        ) -> tuple[httpx.Response, bytes]:
            del client
            body = b"""
                <meta property="og:description"
                      content="A clear project workspace for distributed product teams.">
                <title>Alpha | Plan work without busywork</title>
            """
            request = httpx.Request("GET", requested_url)
            return (
                httpx.Response(
                    200,
                    headers={"content-type": "text/html"},
                    request=request,
                ),
                body,
            )

    with CatalogDatabase(tmp_path / "catalog.duckdb") as database:
        enricher = StubWebsiteEnricher(
            database,
            WebsiteEnrichmentConfig(workers=1, requests_per_second=1),
            tmp_path / "crawl.jsonl",
        )
        async with httpx.AsyncClient() as client:
            result = await enricher._enrich(
                client,
                "alpha",
                "Alpha",
                "https://alpha.example",
                1,
                True,
                True,
            )

    assert result.outcome == "enriched"
    assert result.tagline == "A clear project workspace for distributed product teams."
    assert result.tagline_source == "og_description"
    assert result.description == "A clear project workspace for distributed product teams."
    assert result.description_source == "og_description"
    assert result.response_hash
