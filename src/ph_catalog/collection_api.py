from __future__ import annotations

import asyncio
import base64
import hashlib
import math
import time
from dataclasses import dataclass
from pathlib import Path

import httpx
import orjson

from ph_catalog.catalog_api import FRONTEND_GRAPHQL_URL
from ph_catalog.config import DEFAULT_USER_AGENT
from ph_catalog.database import CatalogDatabase
from ph_catalog.network import BlockController, GlobalRateLimiter

COLLECTION_SCAN_NAME = "public-collections-v1"
COLLECTION_PAGE_SIZE = 100
MAX_OVERFLOW_BATCH = 20


@dataclass(slots=True, frozen=True)
class CollectionRecord:
    product_id: int
    slug: str


@dataclass(slots=True, frozen=True)
class CollectionSummary:
    collection_id: int
    products_count: int
    products: tuple[CollectionRecord, ...]


@dataclass(slots=True, frozen=True)
class CollectionScanConfig:
    workers: int = 4
    requests_per_second: float = 2.0
    timeout: float = 60.0
    block_threshold: int = 5
    max_block_backoff: float = 900.0
    limit_pages: int | None = None
    user_agent: str = DEFAULT_USER_AGENT

    def __post_init__(self) -> None:
        if not 1 <= self.workers <= 8:
            raise ValueError("collection workers must be between 1 and 8")
        if self.requests_per_second <= 0 or self.timeout <= 0:
            raise ValueError("invalid collection scan network configuration")
        if self.limit_pages is not None and self.limit_pages < 1:
            raise ValueError("collection page limit must be positive")


class CollectionScanner:
    """Exhaust public collections and their product memberships."""

    def __init__(
        self, database: CatalogDatabase, config: CollectionScanConfig, log_path: Path
    ) -> None:
        self.database = database
        self.config = config
        self.log_path = log_path
        self.rate = GlobalRateLimiter(config.requests_per_second, 0.05, 0.2)
        self.blocks = BlockController(config.block_threshold, config.max_block_backoff)

    async def run(self) -> dict[str, int | float]:
        started = time.monotonic()
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        headers = {
            "User-Agent": self.config.user_agent,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Origin": "https://www.producthunt.com",
            "Referer": "https://www.producthunt.com/",
            "X-Requested-With": "XMLHttpRequest",
        }
        requests = root_pages_this_run = overflow_pages_this_run = 0
        initial = self.database.collection_counts()
        next_report = ((initial["unique_candidates"] // 10_000) + 1) * 10_000
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(self.config.timeout),
            headers=headers,
        ) as client:
            total = await self._total(client)
            requests += 1
            self.database.prepare_collection_scan(
                COLLECTION_SCAN_NAME,
                total,
                math.ceil(total / COLLECTION_PAGE_SIZE),
            )

            while True:
                limit = self.config.workers
                if self.config.limit_pages is not None:
                    limit = min(limit, self.config.limit_pages - root_pages_this_run)
                    if limit <= 0:
                        break
                pages = self.database.pending_collection_pages(
                    COLLECTION_SCAN_NAME, limit
                )
                if not pages:
                    break
                results = await asyncio.gather(
                    *(self._collection_page(client, page) for page in pages)
                )
                self.database.apply_collection_page_batch(
                    COLLECTION_SCAN_NAME,
                    list(zip(pages, results, strict=True)),
                )
                requests += len(pages)
                root_pages_this_run += len(pages)
                next_report = self._report_progress(next_report, requests)

            while True:
                pending = self.database.pending_collection_overflow(
                    MAX_OVERFLOW_BATCH * self.config.workers
                )
                if not pending:
                    break
                chunks = [
                    pending[start : start + MAX_OVERFLOW_BATCH]
                    for start in range(0, len(pending), MAX_OVERFLOW_BATCH)
                ]
                results = await asyncio.gather(
                    *(self._overflow_pages(client, chunk) for chunk in chunks)
                )
                flattened = [item for group in results for item in group]
                self.database.apply_collection_overflow_batch(flattened)
                requests += len(chunks)
                overflow_pages_this_run += len(flattened)
                next_report = self._report_progress(next_report, requests)

        result = self.database.collection_counts()
        result.update(
            requests_this_run=requests,
            root_pages_this_run=root_pages_this_run,
            overflow_pages_this_run=overflow_pages_this_run,
            elapsed_seconds=round(time.monotonic() - started, 2),
        )
        return result

    def _report_progress(self, next_report: int, requests: int) -> int:
        counts = self.database.collection_counts()
        while counts["unique_candidates"] >= next_report:
            self._emit(
                "collection_scan_progress",
                unique_candidates=counts["unique_candidates"],
                new_product_ids=counts["new_product_ids"],
                collections_scanned=counts["collections_scanned"],
                root_pages_scanned=counts["root_pages_scanned"],
                overflow_complete=counts["overflow_complete"],
                requests_this_run=requests,
            )
            next_report += 10_000
        return next_report

    async def _total(self, client: httpx.AsyncClient) -> int:
        data = await self._graphql(
            client, "query CollectionTotal{collections(first:1){totalCount}}"
        )
        connection = data.get("collections")
        total = connection.get("totalCount") if isinstance(connection, dict) else None
        if not isinstance(total, int) or total < 0:
            raise ValueError("Product Hunt collection total is missing")
        return total

    async def _collection_page(
        self, client: httpx.AsyncClient, page: int
    ) -> list[CollectionSummary]:
        after = "" if page == 0 else f",after:{orjson.dumps(self._cursor(page)).decode()}"
        query = (
            f"query CollectionPage{{collections(first:{COLLECTION_PAGE_SIZE}{after})"
            "{edges{node{id productsCount products(first:100)"
            "{edges{node{id slug}}}}}}}"
        )
        data = await self._graphql(client, query)
        connection = data.get("collections")
        if not isinstance(connection, dict):
            raise ValueError(f"Product Hunt collection page {page} is missing")
        return self._summaries(connection.get("edges"))

    async def _overflow_pages(
        self,
        client: httpx.AsyncClient,
        pending: list[tuple[int, int, int]],
    ) -> list[tuple[int, int, list[CollectionRecord]]]:
        fields = " ".join(
            f"c{offset}:collection(id:{orjson.dumps(str(collection_id)).decode()})"
            "{products(first:100,after:"
            f"{orjson.dumps(self._offset_cursor(next_offset)).decode()})"
            "{edges{node{id slug}}}}"
            for offset, (collection_id, next_offset, _) in enumerate(pending)
        )
        data = await self._graphql(client, f"query CollectionOverflow{{{fields}}}")
        results = []
        for offset, (collection_id, next_offset, _) in enumerate(pending):
            collection = data.get(f"c{offset}")
            products = collection.get("products") if isinstance(collection, dict) else None
            if not isinstance(products, dict):
                raise ValueError(f"collection overflow missing for {collection_id}")
            results.append(
                (collection_id, next_offset, self._records(products.get("edges")))
            )
        return results

    @staticmethod
    def _summaries(edges: object) -> list[CollectionSummary]:
        summaries = []
        for edge in edges if isinstance(edges, list) else []:
            node = edge.get("node") if isinstance(edge, dict) else None
            if not isinstance(node, dict) or not str(node.get("id", "")).isdigit():
                continue
            products = node.get("products")
            count = node.get("productsCount")
            if not isinstance(products, dict) or not isinstance(count, int):
                continue
            summaries.append(
                CollectionSummary(
                    int(str(node["id"])),
                    count,
                    tuple(CollectionScanner._records(products.get("edges"))),
                )
            )
        return summaries

    @staticmethod
    def _records(edges: object) -> list[CollectionRecord]:
        records: dict[int, CollectionRecord] = {}
        for edge in edges if isinstance(edges, list) else []:
            node = edge.get("node") if isinstance(edge, dict) else None
            if not isinstance(node, dict):
                continue
            product_id = node.get("id")
            slug = node.get("slug")
            if not str(product_id).isdigit() or not isinstance(slug, str) or not slug:
                continue
            numeric_id = int(str(product_id))
            records[numeric_id] = CollectionRecord(numeric_id, slug.strip().lower())
        return list(records.values())

    @staticmethod
    def _cursor(page: int) -> str:
        return CollectionScanner._offset_cursor(page * COLLECTION_PAGE_SIZE)

    @staticmethod
    def _offset_cursor(offset: int) -> str:
        return base64.b64encode(str(offset).encode()).decode()

    async def _graphql(
        self, client: httpx.AsyncClient, query: str
    ) -> dict[str, object]:
        operation = query.split("{", 1)[0].removeprefix("query ")
        payload = {
            "operationName": operation,
            "variables": {},
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": hashlib.sha256(query.encode()).hexdigest(),
                }
            },
            "query": query,
        }
        response = await self._request(client, payload)
        body = response.json()
        if body.get("errors"):
            messages = "; ".join(str(item.get("message")) for item in body["errors"])
            raise ValueError(f"Product Hunt collection query failed: {messages}")
        data = body.get("data")
        if not isinstance(data, dict):
            raise ValueError("Product Hunt collection query returned no data")
        return data

    async def _request(
        self, client: httpx.AsyncClient, payload: dict[str, object]
    ) -> httpx.Response:
        for attempt in range(4):
            if not await self.blocks.wait():
                raise RuntimeError("collection scan paused after sustained blocking")
            await self.rate.wait()
            try:
                response = await client.post(FRONTEND_GRAPHQL_URL, json=payload)
            except httpx.TransportError:
                if attempt == 3:
                    raise
                await asyncio.sleep(2**attempt)
                continue
            if response.status_code in {403, 429}:
                delay, stopped = await self.blocks.blocked(
                    response.headers.get("Retry-After")
                )
                requests_per_second = await self.rate.reduce()
                self._emit(
                    "collection_scan_backoff",
                    http_status=response.status_code,
                    delay_seconds=round(delay, 2),
                    requests_per_second=requests_per_second,
                    sustained_blocking=stopped,
                )
                if stopped:
                    raise RuntimeError("collection scan paused after sustained blocking")
                continue
            if response.status_code >= 500:
                if attempt == 3:
                    response.raise_for_status()
                await asyncio.sleep(2**attempt)
                continue
            response.raise_for_status()
            await self.blocks.success()
            await self.rate.success()
            return response
        raise RuntimeError("collection scan retries exhausted")

    def _emit(self, event: str, **fields: object) -> None:
        line = orjson.dumps({"event": event, **fields}).decode()
        with self.log_path.open("a", encoding="utf-8") as stream:
            stream.write(line + "\n")
        print(line, flush=True)
