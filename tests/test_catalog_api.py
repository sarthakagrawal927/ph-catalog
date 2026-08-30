from pathlib import Path

import httpx
import pytest

from ph_catalog.catalog_api import (
    CatalogBatchFetcher,
    CatalogScanConfig,
    CatalogScanMetrics,
)


def test_catalog_batch_size_is_capped_at_verified_graphql_limit() -> None:
    with pytest.raises(ValueError, match="between 1 and 200"):
        CatalogScanConfig(batch_size=201)


def test_catalog_scan_name_must_not_be_empty() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        CatalogScanConfig(scan_name="  ")


async def test_catalog_batch_parses_complete_and_missing_products(tmp_path: Path) -> None:
    config = CatalogScanConfig(
        start_id=100,
        stop_id=102,
        batch_size=2,
        workers=1,
        requests_per_second=100,
    )
    metrics = CatalogScanMetrics(tmp_path / "scan.jsonl", 10_000, 0, 100)
    fetcher = CatalogBatchFetcher(config, metrics)
    await fetcher.close()

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/frontend/graphql"
        return httpx.Response(
            200,
            json={
                "data": {
                    "p100": {
                        "id": "100",
                        "slug": "alpha",
                        "name": "Alpha",
                        "tagline": "The first",
                        "description": "Description",
                        "websiteUrl": "https://alpha.example",
                        "categories": [{"name": "Developer Tools"}],
                    },
                    "p101": None,
                }
            },
        )

    fetcher.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        records = await fetcher.fetch(100, 102)
    finally:
        await fetcher.close()

    assert len(records) == 1
    assert records[0].product_id == 100
    assert records[0].data.slug == "alpha"
    assert records[0].data.categories == ["Developer Tools"]
