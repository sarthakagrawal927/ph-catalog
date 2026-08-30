from __future__ import annotations

import asyncio
import hashlib
import math
import re
import time
from dataclasses import dataclass
from pathlib import Path

import httpx
import orjson
from selectolax.parser import HTMLParser

from ph_catalog.catalog_api import FRONTEND_GRAPHQL_URL
from ph_catalog.config import DEFAULT_USER_AGENT
from ph_catalog.database import CatalogDatabase
from ph_catalog.network import BlockController, GlobalRateLimiter

CATEGORIES_URL = "https://www.producthunt.com/categories"
CATEGORY_PAGE_SIZE = 20
MAX_CATEGORY_PAGE_BATCH = 25
MAX_CATEGORY_TOTAL_BATCH = 40


@dataclass(slots=True, frozen=True)
class CategoryRecord:
    product_id: int
    slug: str


@dataclass(slots=True, frozen=True)
class CategoryScanConfig:
    workers: int = 4
    page_batch_size: int = 20
    requests_per_second: float = 2.0
    timeout: float = 60.0
    block_threshold: int = 5
    max_block_backoff: float = 900.0
    limit_pages: int | None = None
    user_agent: str = DEFAULT_USER_AGENT

    def __post_init__(self) -> None:
        if not 1 <= self.workers <= 8:
            raise ValueError("category workers must be between 1 and 8")
        if not 1 <= self.page_batch_size <= MAX_CATEGORY_PAGE_BATCH:
            raise ValueError(
                f"category page batch size must be between 1 and "
                f"{MAX_CATEGORY_PAGE_BATCH}"
            )
        if self.requests_per_second <= 0 or self.timeout <= 0:
            raise ValueError("invalid category scan network configuration")
        if self.limit_pages is not None and self.limit_pages < 1:
            raise ValueError("category page limit must be positive")


class CategoryScanner:
    """Exhaust public category memberships, including retired products."""

    def __init__(
        self, database: CatalogDatabase, config: CategoryScanConfig, log_path: Path
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
            "Referer": CATEGORIES_URL,
            "X-Requested-With": "XMLHttpRequest",
        }
        requests = pages_this_run = 0
        initial = self.database.category_counts()
        next_report = ((initial["unique_candidates"] // 10_000) + 1) * 10_000
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(self.config.timeout),
            headers=headers,
        ) as client:
            slugs = await self._category_slugs(client)
            requests += 1
            totals = await self._category_totals(client, slugs)
            requests += math.ceil(len(slugs) / MAX_CATEGORY_TOTAL_BATCH)
            for slug in slugs:
                total = totals[slug]
                self.database.prepare_category_scan(
                    slug, total, math.ceil(total / CATEGORY_PAGE_SIZE)
                )

            while True:
                pending_limit = self.config.page_batch_size * self.config.workers
                if self.config.limit_pages is not None:
                    pending_limit = min(
                        pending_limit, self.config.limit_pages - pages_this_run
                    )
                    if pending_limit <= 0:
                        break
                pending = self.database.pending_category_pages(pending_limit)
                if not pending:
                    break
                chunks = [
                    pending[start : start + self.config.page_batch_size]
                    for start in range(0, len(pending), self.config.page_batch_size)
                ]
                chunk_results = await asyncio.gather(
                    *(self._category_pages(client, chunk) for chunk in chunks)
                )
                records = {
                    key: value
                    for chunk_result in chunk_results
                    for key, value in chunk_result.items()
                }
                self.database.apply_category_page_batch(
                    [
                        (slug, page, records.get((slug, page), []))
                        for slug, page in pending
                    ]
                )
                requests += len(chunks)
                pages_this_run += len(pending)
                counts = self.database.category_counts()
                while counts["unique_candidates"] >= next_report:
                    self._emit(
                        "category_scan_progress",
                        unique_candidates=counts["unique_candidates"],
                        memberships=counts["memberships"],
                        new_product_ids=counts["new_product_ids"],
                        categories_complete=counts["categories_complete"],
                        pages_scanned=counts["pages_scanned"],
                        requests_this_run=requests,
                    )
                    next_report += 10_000

        result = self.database.category_counts()
        result["requests_this_run"] = requests
        result["pages_this_run"] = pages_this_run
        result["elapsed_seconds"] = round(time.monotonic() - started, 2)
        return result

    async def _category_slugs(self, client: httpx.AsyncClient) -> list[str]:
        response = await self._request(client, CATEGORIES_URL, graphql=False)
        return self._parse_category_slugs(response.content)

    @staticmethod
    def _parse_category_slugs(html: bytes) -> list[str]:
        slugs: list[str] = []
        tree = HTMLParser(html)
        for node in tree.css("a[href]"):
            match = re.match(
                r"^/categories/([a-z0-9]+(?:-[a-z0-9]+)*)",
                node.attributes.get("href", ""),
            )
            if match and match.group(1) not in slugs:
                slugs.append(match.group(1))
        if not slugs:
            raise ValueError("Product Hunt categories page returned no categories")
        return slugs

    async def _category_totals(
        self, client: httpx.AsyncClient, slugs: list[str]
    ) -> dict[str, int]:
        totals: dict[str, int] = {}
        for start in range(0, len(slugs), MAX_CATEGORY_TOTAL_BATCH):
            batch = slugs[start : start + MAX_CATEGORY_TOTAL_BATCH]
            fields = " ".join(
                f"c{offset}:productCategory(slug:{orjson.dumps(slug).decode()})"
                "{products(page:1,first:1,order:most_recent,"
                "onlyHasFeaturedPosts:false,liveOnly:false){totalCount}}"
                for offset, slug in enumerate(batch)
            )
            data = await self._graphql(client, f"query CategoryTotals{{{fields}}}")
            for offset, slug in enumerate(batch):
                category = data.get(f"c{offset}")
                products = category.get("products") if isinstance(category, dict) else None
                total = products.get("totalCount") if isinstance(products, dict) else None
                if not isinstance(total, int) or total < 0:
                    raise ValueError(f"category total missing for {slug}")
                totals[slug] = total
        return totals

    async def _category_pages(
        self, client: httpx.AsyncClient, pages: list[tuple[str, int]]
    ) -> dict[tuple[str, int], list[CategoryRecord]]:
        fields = " ".join(
            f"c{offset}:productCategory(slug:{orjson.dumps(slug).decode()})"
            f"{{products(page:{page},first:{CATEGORY_PAGE_SIZE},"
            "order:most_recent,onlyHasFeaturedPosts:false,liveOnly:false)"
            "{ edges { node { id slug } } } }"
            for offset, (slug, page) in enumerate(pages)
        )
        data = await self._graphql(client, f"query CategoryPages{{{fields}}}")
        result: dict[tuple[str, int], list[CategoryRecord]] = {}
        for offset, key in enumerate(pages):
            category = data.get(f"c{offset}")
            connection = category.get("products") if isinstance(category, dict) else None
            if not isinstance(connection, dict):
                raise ValueError(f"category page missing for {key[0]} page {key[1]}")
            records: dict[int, CategoryRecord] = {}
            for edge in connection.get("edges") or []:
                node = edge.get("node") if isinstance(edge, dict) else None
                if not isinstance(node, dict):
                    continue
                product_id = node.get("id")
                slug = node.get("slug")
                if not str(product_id).isdigit() or not isinstance(slug, str) or not slug:
                    continue
                numeric_id = int(str(product_id))
                records[numeric_id] = CategoryRecord(numeric_id, slug.strip().lower())
            result[key] = list(records.values())
        return result

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
        response = await self._request(
            client, FRONTEND_GRAPHQL_URL, graphql=True, payload=payload
        )
        body = response.json()
        if body.get("errors"):
            messages = "; ".join(str(item.get("message")) for item in body["errors"])
            raise ValueError(f"Product Hunt category query failed: {messages}")
        data = body.get("data")
        if not isinstance(data, dict):
            raise ValueError("Product Hunt category query returned no data")
        return data

    async def _request(
        self,
        client: httpx.AsyncClient,
        url: str,
        *,
        graphql: bool,
        payload: dict[str, object] | None = None,
    ) -> httpx.Response:
        for attempt in range(4):
            if not await self.blocks.wait():
                raise RuntimeError("category scan paused after sustained blocking")
            await self.rate.wait()
            try:
                response = (
                    await client.post(url, json=payload)
                    if graphql
                    else await client.get(url)
                )
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
                    "category_scan_backoff",
                    http_status=response.status_code,
                    delay_seconds=round(delay, 2),
                    requests_per_second=requests_per_second,
                    sustained_blocking=stopped,
                )
                if stopped:
                    raise RuntimeError("category scan paused after sustained blocking")
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
        raise RuntimeError("category scan retries exhausted")

    def _emit(self, event: str, **fields: object) -> None:
        record = {"event": event, **fields}
        line = orjson.dumps(record).decode()
        with self.log_path.open("a", encoding="utf-8") as stream:
            stream.write(line + "\n")
        print(line, flush=True)
