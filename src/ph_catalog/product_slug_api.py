from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass
from pathlib import Path

import httpx
import orjson

from ph_catalog.catalog_api import (
    FRONTEND_GRAPHQL_URL,
    CatalogBatchFetcher,
)
from ph_catalog.config import DEFAULT_USER_AGENT
from ph_catalog.database import CatalogDatabase
from ph_catalog.models import CatalogRecord
from ph_catalog.network import BlockController, GlobalRateLimiter

MAX_PRODUCT_SLUG_BATCH = 100
MAX_QUERY_BYTES = 24_000


@dataclass(slots=True, frozen=True)
class ProductSlugResolveConfig:
    batch_size: int = 100
    requests_per_second: float = 0.5
    timeout: float = 60.0
    block_threshold: int = 5
    max_block_backoff: float = 900.0
    limit: int | None = None
    user_agent: str = DEFAULT_USER_AGENT

    def __post_init__(self) -> None:
        if not 1 <= self.batch_size <= MAX_PRODUCT_SLUG_BATCH:
            raise ValueError(
                f"product slug batch size must be between 1 and {MAX_PRODUCT_SLUG_BATCH}"
            )
        if self.requests_per_second <= 0:
            raise ValueError("product slug resolver requests per second must be positive")


class ProductSlugResolver:
    def __init__(
        self, database: CatalogDatabase, config: ProductSlugResolveConfig, log_path: Path
    ) -> None:
        self.database = database
        self.config = config
        self.log_path = log_path
        self.rate = GlobalRateLimiter(config.requests_per_second, 0.05, 0.2)
        self.blocks = BlockController(config.block_threshold, config.max_block_backoff)

    async def run(self) -> dict[str, int | float]:
        started = time.monotonic()
        processed = fetched = unavailable = requests = 0
        headers = {
            "User-Agent": self.config.user_agent,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Origin": "https://www.producthunt.com",
            "Referer": "https://www.producthunt.com/products",
            "X-Requested-With": "XMLHttpRequest",
        }
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(self.config.timeout),
            headers=headers,
        ) as client:
            while self.config.limit is None or processed < self.config.limit:
                remaining = (
                    self.config.batch_size
                    if self.config.limit is None
                    else min(self.config.batch_size, self.config.limit - processed)
                )
                slugs = self.database.pending_archive_product_slugs(remaining)
                if not slugs:
                    break
                resolutions, request_count = await self._fetch(client, slugs)
                self.database.apply_archive_product_resolutions(resolutions)
                requests += request_count
                processed += len(resolutions)
                fetched += sum(record is not None for _, record in resolutions)
                unavailable += sum(record is None for _, record in resolutions)
        return {
            "processed": processed,
            "requests": requests,
            "fetched_or_aliased": fetched,
            "unavailable": unavailable,
            "elapsed_seconds": round(time.monotonic() - started, 2),
        }

    async def _fetch(
        self, client: httpx.AsyncClient, slugs: list[str]
    ) -> tuple[list[tuple[str, CatalogRecord | None]], int]:
        query = self._query(slugs)
        if len(query.encode()) > MAX_QUERY_BYTES:
            midpoint = len(slugs) // 2
            if midpoint == 0:
                raise ValueError("one product slug exceeds the GraphQL query limit")
            first, first_requests = await self._fetch(client, slugs[:midpoint])
            second, second_requests = await self._fetch(client, slugs[midpoint:])
            return first + second, first_requests + second_requests
        payload = {
            "operationName": "ProductSlugBatch",
            "variables": {},
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": hashlib.sha256(query.encode()).hexdigest(),
                }
            },
            "query": query,
        }
        for attempt in range(4):
            if not await self.blocks.wait():
                raise RuntimeError("product slug resolver paused after sustained blocking")
            await self.rate.wait()
            try:
                response = await client.post(FRONTEND_GRAPHQL_URL, json=payload)
            except httpx.TransportError:
                if attempt == 3:
                    raise
                await asyncio.sleep(2**attempt)
                continue
            if response.status_code in {403, 429}:
                _, stopped = await self.blocks.blocked(response.headers.get("Retry-After"))
                await self.rate.reduce()
                if stopped:
                    raise RuntimeError("product slug resolver paused after sustained blocking")
                continue
            response.raise_for_status()
            body = response.json()
            if body.get("errors"):
                messages = "; ".join(str(item.get("message")) for item in body["errors"])
                raise ValueError(f"product slug query failed: {messages}")
            data = body.get("data")
            if not isinstance(data, dict) or len(data) != len(slugs):
                raise ValueError("incomplete product slug batch")
            await self.blocks.success()
            await self.rate.success()
            resolutions: list[tuple[str, CatalogRecord | None]] = []
            for offset, requested_slug in enumerate(slugs):
                records = CatalogBatchFetcher._records({f"p{offset}": data.get(f"p{offset}")})
                resolutions.append((requested_slug, records[0] if records else None))
            return resolutions, 1
        raise RuntimeError("product slug batch retries exhausted")

    @staticmethod
    def _query(slugs: list[str]) -> str:
        fields = " ".join(
            f"p{offset}:product(slug:{orjson.dumps(slug).decode()})"
            "{id slug name tagline description websiteUrl categories{name} meta{description}}"
            for offset, slug in enumerate(slugs)
        )
        return f"query ProductSlugBatch{{{fields}}}"
