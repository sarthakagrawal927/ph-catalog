from __future__ import annotations

import asyncio
import hashlib
import string
import time
from dataclasses import dataclass
from pathlib import Path

import httpx
import orjson

from ph_catalog.catalog_api import FRONTEND_GRAPHQL_URL
from ph_catalog.config import DEFAULT_USER_AGENT
from ph_catalog.database import CatalogDatabase
from ph_catalog.network import BlockController, GlobalRateLimiter

DEFAULT_SEARCH_QUERIES = ("", *string.digits, *string.ascii_lowercase)
TWO_CHARACTER_SEARCH_QUERIES = tuple(
    left + right for left in string.ascii_lowercase for right in string.ascii_lowercase
)
MAX_SEARCH_PAGE_BATCH = 100
MAX_SEARCH_COUNT_BATCH = 100


@dataclass(slots=True, frozen=True)
class SearchRecord:
    product_id: int
    slug: str
    name: str | None
    tagline: str | None


@dataclass(slots=True, frozen=True)
class SearchScanConfig:
    queries: tuple[str, ...] = DEFAULT_SEARCH_QUERIES
    workers: int = 4
    page_batch_size: int = 100
    requests_per_second: float = 1.0
    timeout: float = 60.0
    block_threshold: int = 5
    max_block_backoff: float = 900.0
    user_agent: str = DEFAULT_USER_AGENT

    def __post_init__(self) -> None:
        if not 1 <= self.workers <= 8:
            raise ValueError("search workers must be between 1 and 8")
        if not 1 <= self.page_batch_size <= MAX_SEARCH_PAGE_BATCH:
            raise ValueError(
                f"search page batch size must be between 1 and {MAX_SEARCH_PAGE_BATCH}"
            )
        if self.requests_per_second <= 0:
            raise ValueError("search scan requests per second must be positive")


class SearchScanner:
    def __init__(
        self, database: CatalogDatabase, config: SearchScanConfig, log_path: Path
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
            "Referer": "https://www.producthunt.com/search",
            "X-Requested-With": "XMLHttpRequest",
        }
        requests = 0
        pages = 0
        initial_total = self.database.search_counts()["unique_candidates"]
        next_report = ((initial_total // 10_000) + 1) * 10_000
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(self.config.timeout),
            headers=headers,
        ) as client:
            missing = [
                query
                for query in self.config.queries
                if self.database.search_scan_state(query) is None
            ]
            if missing:
                page_counts = await self._fetch_page_counts(client, missing)
                requests += (len(missing) + MAX_SEARCH_COUNT_BATCH - 1) // MAX_SEARCH_COUNT_BATCH
                for query in missing:
                    self.database.prepare_search_scan(query, page_counts[query])

            queue: asyncio.Queue[str] = asyncio.Queue()
            for query in self.config.queries:
                queue.put_nowait(query)

            async def scan_worker() -> None:
                nonlocal requests, pages, next_report
                while not queue.empty():
                    try:
                        query = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    try:
                        await scan_query(query)
                    finally:
                        queue.task_done()

            async def scan_query(query: str) -> None:
                nonlocal requests, pages, next_report
                while True:
                    state = self.database.search_scan_state(query)
                    assert state is not None
                    next_page = state["next_page"]
                    pages_count = state["pages_count"]
                    if next_page > pages_count:
                        break
                    stop_page = min(next_page + self.config.page_batch_size, pages_count + 1)
                    records = await self._fetch_pages(client, query, next_page, stop_page)
                    self.database.apply_search_batch(
                        query, next_page, stop_page, records, stop_page - next_page
                    )
                    requests += 1
                    pages += stop_page - next_page
                    counts = self.database.search_counts()
                    while counts["unique_candidates"] >= next_report:
                        self._emit(
                            "search_scan_progress",
                            unique_candidates=counts["unique_candidates"],
                            new_product_ids=counts["new_product_ids"],
                            pages_scanned=pages,
                            requests=requests,
                        )
                        next_report += 10_000

            await asyncio.gather(
                *(scan_worker() for _ in range(min(self.config.workers, len(self.config.queries))))
            )

        result = self.database.search_counts()
        result["requests_this_run"] = requests
        result["pages_this_run"] = pages
        result["elapsed_seconds"] = round(time.monotonic() - started, 2)
        return result

    async def _fetch_page_counts(
        self, client: httpx.AsyncClient, queries: list[str]
    ) -> dict[str, int]:
        counts: dict[str, int] = {}
        for start in range(0, len(queries), MAX_SEARCH_COUNT_BATCH):
            batch = queries[start : start + MAX_SEARCH_COUNT_BATCH]
            fields = " ".join(
                f"q{offset}:productSearch(query:{orjson.dumps(query).decode()},first:10,page:1)"
                "{pagesCount}"
                for offset, query in enumerate(batch)
            )
            data = await self._request(client, f"query SearchPageCounts{{{fields}}}")
            counts.update(
                {
                    query: int(data[f"q{offset}"]["pagesCount"])
                    for offset, query in enumerate(batch)
                }
            )
        return counts

    async def _fetch_pages(
        self, client: httpx.AsyncClient, query_text: str, start_page: int, stop_page: int
    ) -> list[SearchRecord]:
        pages = list(range(start_page, stop_page))
        fields = " ".join(
            f"p{offset}:productSearch(query:{orjson.dumps(query_text).decode()},"
            f"first:10,page:{page})"
            "{edges{node{id slug name tagline}}}"
            for offset, page in enumerate(pages)
        )
        data = await self._request(client, f"query SearchPageBatch{{{fields}}}")
        return self._records(pages, data)

    @staticmethod
    def _records(pages: list[int], data: dict[str, object]) -> list[SearchRecord]:
        records: dict[int, SearchRecord] = {}
        for offset in range(len(pages)):
            connection = data.get(f"p{offset}")
            if not isinstance(connection, dict):
                continue
            for edge in connection.get("edges") or []:
                node = edge.get("node") if isinstance(edge, dict) else None
                if not isinstance(node, dict):
                    continue
                product_id = node.get("id")
                slug = node.get("slug")
                if not str(product_id).isdigit() or not isinstance(slug, str) or not slug:
                    continue
                numeric_id = int(str(product_id))
                records[numeric_id] = SearchRecord(
                    product_id=numeric_id,
                    slug=slug.strip().lower(),
                    name=node.get("name") if isinstance(node.get("name"), str) else None,
                    tagline=(
                        node.get("tagline") if isinstance(node.get("tagline"), str) else None
                    ),
                )
        return list(records.values())

    async def _request(
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
        for attempt in range(4):
            if not await self.blocks.wait():
                raise RuntimeError("search scan paused after sustained blocking")
            await self.rate.wait()
            try:
                response = await client.post(FRONTEND_GRAPHQL_URL, json=payload)
            except httpx.TransportError:
                if attempt == 3:
                    raise
                await asyncio.sleep(2**attempt)
                continue
            if response.status_code in {403, 429}:
                delay, stopped = await self.blocks.blocked(response.headers.get("Retry-After"))
                requests_per_second = await self.rate.reduce()
                self._emit(
                    "search_scan_backoff",
                    http_status=response.status_code,
                    delay_seconds=round(delay, 2),
                    requests_per_second=requests_per_second,
                    sustained_blocking=stopped,
                )
                if stopped:
                    raise RuntimeError("search scan paused after sustained blocking")
                continue
            response.raise_for_status()
            body = response.json()
            if body.get("errors"):
                messages = "; ".join(str(item.get("message")) for item in body["errors"])
                raise ValueError(f"Product Hunt search query failed: {messages}")
            data = body.get("data")
            if not isinstance(data, dict):
                raise ValueError("Product Hunt search query returned no data")
            await self.blocks.success()
            await self.rate.success()
            return data
        raise RuntimeError("search query retries exhausted")

    def _emit(self, event: str, **fields: object) -> None:
        record = {"event": event, **fields}
        line = orjson.dumps(record).decode()
        with self.log_path.open("a", encoding="utf-8") as stream:
            stream.write(line + "\n")
        print(line, flush=True)
