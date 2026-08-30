from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx
import orjson

from ph_catalog.catalog_api import FRONTEND_GRAPHQL_URL
from ph_catalog.config import DEFAULT_USER_AGENT
from ph_catalog.database import CatalogDatabase
from ph_catalog.network import AdaptiveConcurrency, BlockController, GlobalRateLimiter

MAX_POST_BATCH = 200
MAX_QUERY_BYTES = 24_000
POST_ID_SCAN_NAME = "producthunt-frontend-post-ids-v1"


@dataclass(slots=True, frozen=True)
class PostResolution:
    post_slug: str
    outcome: str
    post_id: int | None = None
    product_id: int | None = None
    product_slug: str | None = None


@dataclass(slots=True, frozen=True)
class PostResolveConfig:
    batch_size: int = 100
    workers: int = 1
    requests_per_second: float = 0.5
    timeout: float = 60.0
    block_threshold: int = 5
    max_block_backoff: float = 900.0
    limit: int | None = None
    user_agent: str = DEFAULT_USER_AGENT

    def __post_init__(self) -> None:
        if not 1 <= self.batch_size <= MAX_POST_BATCH:
            raise ValueError(f"post batch size must be between 1 and {MAX_POST_BATCH}")
        if not 1 <= self.workers <= 8:
            raise ValueError("post resolver workers must be between 1 and 8")
        if self.requests_per_second <= 0:
            raise ValueError("post resolver requests per second must be positive")


@dataclass(slots=True, frozen=True)
class PostIdScanConfig:
    start_id: int = 1
    stop_id: int = 1_240_000
    batch_size: int = 200
    workers: int = 4
    requests_per_second: float = 1.0
    timeout: float = 60.0
    block_threshold: int = 5
    max_block_backoff: float = 900.0
    progress_interval: int = 10_000
    user_agent: str = DEFAULT_USER_AGENT

    def __post_init__(self) -> None:
        if self.start_id < 1 or self.stop_id <= self.start_id:
            raise ValueError("post ID scan requires 1 <= start-id < stop-id")
        if not 1 <= self.batch_size <= MAX_POST_BATCH:
            raise ValueError(f"post batch size must be between 1 and {MAX_POST_BATCH}")
        if not 1 <= self.workers <= 8:
            raise ValueError("post ID scan workers must be between 1 and 8")
        if self.requests_per_second <= 0:
            raise ValueError("post ID scan requests per second must be positive")
        if self.progress_interval <= 0:
            raise ValueError("post ID scan progress interval must be positive")


class PostResolver:
    def __init__(
        self, database: CatalogDatabase, config: PostResolveConfig, log_path: Path
    ) -> None:
        self.database = database
        self.config = config
        self.log_path = log_path
        self.rate = GlobalRateLimiter(config.requests_per_second, 0.05, 0.2)
        self.concurrency = AdaptiveConcurrency(config.workers)
        self.blocks = BlockController(config.block_threshold, config.max_block_backoff)

    async def run(self) -> dict[str, int | float]:
        started = time.monotonic()
        processed = 0
        requests = 0
        resolved = 0
        no_product = 0
        unavailable = 0
        quarantined = self.database.quarantine_invalid_archive_post_slugs()
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
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
                slugs = self.database.pending_archive_post_slugs(remaining)
                if not slugs:
                    break
                resolutions = await self._fetch(client, slugs)
                self.database.apply_post_resolutions(resolutions)
                requests += 1
                processed += len(resolutions)
                resolved += sum(item.outcome == "resolved" for item in resolutions)
                no_product += sum(item.outcome == "no_product" for item in resolutions)
                unavailable += sum(item.outcome == "unavailable" for item in resolutions)
                if processed // 10_000 > (processed - len(resolutions)) // 10_000:
                    self._emit(
                        "post_resolution_progress",
                        processed=processed,
                        resolved=resolved,
                        no_product=no_product,
                        unavailable=unavailable,
                        requests=requests,
                    )
        return {
            "processed": processed,
            "requests": requests,
            "resolved": resolved,
            "no_product": no_product,
            "unavailable": unavailable,
            "quarantined_invalid_slugs": quarantined,
            "elapsed_seconds": round(time.monotonic() - started, 2),
        }

    async def _fetch(
        self, client: httpx.AsyncClient, slugs: list[str]
    ) -> list[PostResolution]:
        query = self._query(slugs)
        if len(query.encode()) > MAX_QUERY_BYTES:
            midpoint = len(slugs) // 2
            if midpoint == 0:
                raise ValueError("one legacy post slug exceeds the GraphQL query limit")
            first = await self._fetch(client, slugs[:midpoint])
            second = await self._fetch(client, slugs[midpoint:])
            return first + second
        payload = {
            "operationName": "LegacyPostBatch",
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
            await self.concurrency.acquire()
            try:
                if not await self.blocks.wait():
                    raise RuntimeError("post resolver paused after sustained blocking")
                await self.rate.wait()
                response = await client.post(FRONTEND_GRAPHQL_URL, json=payload)
            except httpx.TransportError:
                if attempt == 3:
                    raise
                await asyncio.sleep(2**attempt)
                continue
            finally:
                await self.concurrency.release()
            if response.status_code in {403, 429}:
                delay, stopped = await self.blocks.blocked(response.headers.get("Retry-After"))
                concurrency = await self.concurrency.reduce()
                requests_per_second = await self.rate.reduce()
                self._emit(
                    "post_resolution_backoff",
                    http_status=response.status_code,
                    delay_seconds=round(delay, 2),
                    concurrency=concurrency,
                    requests_per_second=requests_per_second,
                    sustained_blocking=stopped,
                )
                if stopped:
                    raise RuntimeError("post resolver paused after sustained blocking")
                continue
            response.raise_for_status()
            body = response.json()
            if body.get("errors"):
                messages = "; ".join(str(item.get("message")) for item in body["errors"])
                raise ValueError(f"legacy post query failed: {messages}")
            data = body.get("data")
            if not isinstance(data, dict) or len(data) != len(slugs):
                raise ValueError("incomplete legacy post batch")
            await self.blocks.success()
            await self.concurrency.success()
            await self.rate.success()
            return self._resolutions(slugs, data)
        raise RuntimeError("legacy post batch retries exhausted")

    @staticmethod
    def _query(slugs: list[str]) -> str:
        fields = " ".join(
            f"p{offset}:post(slug:{orjson.dumps(slug).decode()})"
            "{id slug product{id slug}}"
            for offset, slug in enumerate(slugs)
        )
        return f"query LegacyPostBatch{{{fields}}}"

    @staticmethod
    def _resolutions(slugs: list[str], data: dict[str, object]) -> list[PostResolution]:
        results: list[PostResolution] = []
        for offset, requested_slug in enumerate(slugs):
            post = data.get(f"p{offset}")
            if not isinstance(post, dict):
                results.append(PostResolution(requested_slug, "unavailable"))
                continue
            post_id = int(str(post["id"])) if str(post.get("id", "")).isdigit() else None
            product = post.get("product")
            if not isinstance(product, dict):
                results.append(PostResolution(requested_slug, "no_product", post_id=post_id))
                continue
            product_id = product.get("id")
            product_slug = product.get("slug")
            if not str(product_id).isdigit() or not isinstance(product_slug, str):
                results.append(PostResolution(requested_slug, "no_product", post_id=post_id))
                continue
            results.append(
                PostResolution(
                    requested_slug,
                    "resolved",
                    post_id=post_id,
                    product_id=int(str(product_id)),
                    product_slug=product_slug.strip().lower(),
                )
            )
        return results

    def _emit(self, event: str, **fields: object) -> None:
        record = {"timestamp": datetime.now(UTC).isoformat(), "event": event, **fields}
        line = orjson.dumps(record).decode()
        with self.log_path.open("a", encoding="utf-8") as stream:
            stream.write(line + "\n")
        print(line, flush=True)


class PostIdScanner:
    """Enumerate numeric launch IDs independently of product IDs and sitemaps."""

    def __init__(
        self, database: CatalogDatabase, config: PostIdScanConfig, log_path: Path
    ) -> None:
        self.database = database
        self.config = config
        self.log_path = log_path
        self.state = database.prepare_manifest_scan(
            POST_ID_SCAN_NAME, config.start_id, config.stop_id
        )
        resolver_config = PostResolveConfig(
            batch_size=config.batch_size,
            workers=config.workers,
            requests_per_second=config.requests_per_second,
            timeout=config.timeout,
            block_threshold=config.block_threshold,
            max_block_backoff=config.max_block_backoff,
            user_agent=config.user_agent,
        )
        self.resolver = PostResolver(database, resolver_config, log_path)
        self._start_total = database.total_products()
        self._next_id_report = (
            (self.state["next_id"] // config.progress_interval) + 1
        ) * config.progress_interval
        self._next_product_report = (
            (database.total_products() // config.progress_interval) + 1
        ) * config.progress_interval

    async def run(self) -> dict[str, object]:
        started = time.monotonic()
        start_total = self.database.total_products()
        outcome = "complete"
        self._emit(
            "post_id_scan_started",
            next_id=self.state["next_id"],
            stop_id=self.state["stop_id"],
            batch_size=self.config.batch_size,
            workers=self.config.workers,
            requests_per_second=self.config.requests_per_second,
        )
        headers = {
            "User-Agent": self.config.user_agent,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Origin": "https://www.producthunt.com",
            "Referer": "https://www.producthunt.com/products",
            "X-Requested-With": "XMLHttpRequest",
        }
        try:
            async with httpx.AsyncClient(
                follow_redirects=True,
                timeout=httpx.Timeout(self.config.timeout),
                headers=headers,
            ) as client:
                while self.state["next_id"] < self.state["stop_id"]:
                    ranges: list[tuple[int, int]] = []
                    cursor = self.state["next_id"]
                    for _ in range(self.resolver.concurrency.limit):
                        if cursor >= self.state["stop_id"]:
                            break
                        batch_stop = min(
                            cursor + self.config.batch_size, self.state["stop_id"]
                        )
                        ranges.append((cursor, batch_stop))
                        cursor = batch_stop
                    fetched_groups = await asyncio.gather(
                        *(
                            self.resolver._fetch(
                                client,
                                [str(post_id) for post_id in range(start, stop)],
                            )
                            for start, stop in ranges
                        )
                    )
                    for (batch_start, batch_stop), fetched in zip(
                        ranges, fetched_groups, strict=True
                    ):
                        resolutions = self._exact_resolutions(batch_start, fetched)
                        self.database.apply_post_id_batch(
                            POST_ID_SCAN_NAME,
                            batch_start,
                            batch_stop,
                            resolutions,
                        )
                        state = self.database.manifest_scan_state(POST_ID_SCAN_NAME)
                        assert state is not None
                        self.state = state
                        self._report_progress(batch_stop)
        except BaseException:
            outcome = "interrupted"
            raise
        finally:
            state = self.database.manifest_scan_state(POST_ID_SCAN_NAME)
            assert state is not None
            self.state = state
            final = {
                **state,
                "outcome": outcome,
                "inserted": self.database.total_products() - start_total,
                "total_products": self.database.total_products(),
                "elapsed_seconds": round(time.monotonic() - started, 2),
            }
            self._emit("post_id_scan_finished", **final)
        return final

    @staticmethod
    def _exact_resolutions(
        batch_start: int, resolutions: list[PostResolution]
    ) -> list[PostResolution]:
        exact: list[PostResolution] = []
        for expected_id, item in enumerate(resolutions, batch_start):
            if item.post_id == expected_id:
                exact.append(item)
            else:
                exact.append(PostResolution(str(expected_id), "unavailable"))
        return exact

    def _report_progress(self, next_id: int) -> None:
        total = self.database.total_products()
        if total >= self._next_product_report:
            self._emit(
                "post_id_product_progress",
                total_products=total,
                next_id=next_id,
                inserted_this_run=total - self._start_total,
            )
            while self._next_product_report <= total:
                self._next_product_report += self.config.progress_interval
        if next_id >= self._next_id_report:
            self._emit(
                "post_id_scan_checkpoint",
                total_products=total,
                next_id=next_id,
                requests=self.state["requests"],
                resolved_posts=self.state["discovered"],
                unavailable=self.state["unavailable"],
            )
            while self._next_id_report <= next_id:
                self._next_id_report += self.config.progress_interval

    def _emit(self, event: str, **fields: object) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        record = {"timestamp": datetime.now(UTC).isoformat(), "event": event, **fields}
        line = orjson.dumps(record).decode()
        with self.log_path.open("a", encoding="utf-8") as stream:
            stream.write(line + "\n")
        print(line, flush=True)
