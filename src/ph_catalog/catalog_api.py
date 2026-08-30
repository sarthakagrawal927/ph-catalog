from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx
import orjson

from ph_catalog.config import DEFAULT_USER_AGENT
from ph_catalog.database import CatalogDatabase
from ph_catalog.models import CatalogRecord, ProductData
from ph_catalog.network import (
    AdaptiveConcurrency,
    BlockController,
    GlobalRateLimiter,
)
from ph_catalog.parsing import content_hash

FRONTEND_GRAPHQL_URL = "https://www.producthunt.com/frontend/graphql"
CATALOG_SCAN_NAME = "producthunt-frontend-product-ids-v1"
MAX_CATALOG_BATCH = 200


@dataclass(slots=True, frozen=True)
class CatalogScanConfig:
    scan_name: str = CATALOG_SCAN_NAME
    start_id: int = 1
    stop_id: int = 1_400_000
    batch_size: int = 200
    workers: int = 4
    requests_per_second: float = 2.0
    timeout: float = 60.0
    block_threshold: int = 5
    max_block_backoff: float = 900.0
    progress_interval: int = 10_000
    user_agent: str = DEFAULT_USER_AGENT

    def __post_init__(self) -> None:
        if not self.scan_name.strip():
            raise ValueError("catalog scan name must not be empty")
        if self.start_id < 1 or self.stop_id <= self.start_id:
            raise ValueError("catalog scan requires 1 <= start-id < stop-id")
        if not 1 <= self.batch_size <= MAX_CATALOG_BATCH:
            raise ValueError(f"catalog batch size must be between 1 and {MAX_CATALOG_BATCH}")
        if not 1 <= self.workers <= 8:
            raise ValueError("workers must be between 1 and 8")
        if self.requests_per_second <= 0:
            raise ValueError("requests per second must be positive")


class CatalogScanMetrics:
    def __init__(
        self, log_path: Path, progress_interval: int, product_total: int, next_id: int
    ) -> None:
        self.log_path = log_path
        self.progress_interval = progress_interval
        self.started = time.monotonic()
        self.session_requests = 0
        self.session_ids = 0
        self.session_discovered = 0
        self._next_progress = ((product_total // progress_interval) + 1) * progress_interval
        self._next_id_progress = ((next_id // progress_interval) + 1) * progress_interval
        log_path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, event: str, **fields: object) -> None:
        record = {"timestamp": datetime.now(UTC).isoformat(), "event": event, **fields}
        line = orjson.dumps(record).decode()
        with self.log_path.open("a", encoding="utf-8") as stream:
            stream.write(line + "\n")
        print(line, flush=True)

    def record(self, ids: int, product_total: int, found: int, next_id: int) -> None:
        self.session_requests += 1
        self.session_ids += ids
        self.session_discovered += found
        if product_total >= self._next_progress:
            self.emit(
                "catalog_progress",
                total_products=product_total,
                next_id=next_id,
                session_requests=self.session_requests,
                session_ids=self.session_ids,
                session_discovered=self.session_discovered,
                elapsed_seconds=round(time.monotonic() - self.started, 2),
            )
            while self._next_progress <= product_total:
                self._next_progress += self.progress_interval
        if next_id >= self._next_id_progress:
            self.emit(
                "catalog_scan_checkpoint",
                total_products=product_total,
                next_id=next_id,
                session_requests=self.session_requests,
                session_ids=self.session_ids,
                session_discovered=self.session_discovered,
                elapsed_seconds=round(time.monotonic() - self.started, 2),
            )
            while self._next_id_progress <= next_id:
                self._next_id_progress += self.progress_interval


class CatalogBatchFetcher:
    def __init__(self, config: CatalogScanConfig, metrics: CatalogScanMetrics) -> None:
        self.config = config
        self.metrics = metrics
        self.rate = GlobalRateLimiter(config.requests_per_second, 0.05, 0.2)
        self.concurrency = AdaptiveConcurrency(config.workers)
        self.blocks = BlockController(config.block_threshold, config.max_block_backoff)
        self.client = httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(config.timeout),
            headers={
                "User-Agent": config.user_agent,
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Origin": "https://www.producthunt.com",
                "Referer": "https://www.producthunt.com/products",
                "X-Requested-With": "XMLHttpRequest",
            },
            limits=httpx.Limits(
                max_connections=config.workers,
                max_keepalive_connections=config.workers,
            ),
        )

    async def close(self) -> None:
        await self.client.aclose()

    @staticmethod
    def _query(product_ids: list[int]) -> str:
        fields = " ".join(
            f'p{product_id}:product(id:"{product_id}")'
            "{id slug name tagline description websiteUrl categories{name} meta{description}}"
            for product_id in product_ids
        )
        return f"query ProductCatalogBatch{{{fields}}}"

    async def fetch(self, start_id: int, stop_id: int) -> list[CatalogRecord]:
        return await self.fetch_ids(list(range(start_id, stop_id)))

    async def fetch_ids(self, product_ids: list[int]) -> list[CatalogRecord]:
        if not product_ids or len(product_ids) > MAX_CATALOG_BATCH:
            raise ValueError(f"catalog query requires 1-{MAX_CATALOG_BATCH} product IDs")
        start_id = min(product_ids)
        stop_id = max(product_ids) + 1
        query = self._query(product_ids)
        payload = {
            "operationName": "ProductCatalogBatch",
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
                    raise RuntimeError("catalog scan paused after sustained blocking")
                await self.rate.wait()
                response = await self.client.post(FRONTEND_GRAPHQL_URL, json=payload)
            except httpx.TransportError as exc:
                self.metrics.emit(
                    "catalog_transport_retry",
                    start_id=start_id,
                    stop_id=stop_id,
                    attempt=attempt + 1,
                    error=f"{type(exc).__name__}: {exc}",
                )
                if attempt >= 3:
                    raise
                await asyncio.sleep(2**attempt)
                continue
            finally:
                await self.concurrency.release()

            if response.status_code in {403, 429}:
                delay, stopped = await self.blocks.blocked(response.headers.get("Retry-After"))
                concurrency = await self.concurrency.reduce()
                requests_per_second = await self.rate.reduce()
                self.metrics.emit(
                    "catalog_global_backoff",
                    http_status=response.status_code,
                    delay_seconds=round(delay, 2),
                    concurrency=concurrency,
                    requests_per_second=requests_per_second,
                    sustained_blocking=stopped,
                )
                if stopped:
                    raise RuntimeError("catalog scan paused after sustained blocking")
                continue
            if response.status_code >= 500:
                self.metrics.emit(
                    "catalog_server_retry",
                    start_id=start_id,
                    stop_id=stop_id,
                    attempt=attempt + 1,
                    http_status=response.status_code,
                )
                if attempt >= 3:
                    response.raise_for_status()
                await asyncio.sleep(2**attempt)
                continue
            response.raise_for_status()

            body = response.json()
            if body.get("errors"):
                messages = "; ".join(str(item.get("message")) for item in body["errors"])
                raise ValueError(f"Product Hunt catalogue query failed: {messages}")
            data = body.get("data")
            expected = len(product_ids)
            if not isinstance(data, dict) or len(data) != expected:
                raise ValueError(
                    f"incomplete Product Hunt catalogue batch: expected {expected}, got "
                    f"{len(data) if isinstance(data, dict) else 0}"
                )
            await self.blocks.success()
            await self.concurrency.success()
            await self.rate.success()
            return self._records(data)
        raise RuntimeError("catalogue batch retries exhausted")

    @staticmethod
    def _records(data: dict[str, object]) -> list[CatalogRecord]:
        records: dict[int, CatalogRecord] = {}
        for value in data.values():
            if not isinstance(value, dict):
                continue
            slug = value.get("slug")
            product_id = value.get("id")
            if not isinstance(slug, str) or not slug or not str(product_id).isdigit():
                continue
            categories = value.get("categories")
            category_names = []
            if isinstance(categories, list):
                category_names = [
                    item["name"]
                    for item in categories
                    if isinstance(item, dict)
                    and isinstance(item.get("name"), str)
                    and item["name"]
                ]
            canonical_slug = slug.strip().lower()
            product = ProductData(
                slug=canonical_slug,
                producthunt_url=f"https://www.producthunt.com/products/{canonical_slug}",
                name=value.get("name") if isinstance(value.get("name"), str) else None,
                tagline=value.get("tagline") if isinstance(value.get("tagline"), str) else None,
                description=CatalogBatchFetcher._description(value),
                website_url=(
                    value.get("websiteUrl")
                    if isinstance(value.get("websiteUrl"), str)
                    else None
                ),
                categories=list(dict.fromkeys(category_names)),
            )
            numeric_id = int(str(product_id))
            records[numeric_id] = CatalogRecord(numeric_id, product, content_hash(product))
        return list(records.values())

    @staticmethod
    def _description(value: dict[str, object]) -> str | None:
        description = value.get("description")
        if isinstance(description, str) and description:
            return description
        meta = value.get("meta")
        if isinstance(meta, dict):
            meta_description = meta.get("description")
            if isinstance(meta_description, str) and meta_description:
                return meta_description
        return None


class CatalogScanner:
    def __init__(
        self,
        database: CatalogDatabase,
        config: CatalogScanConfig,
        log_path: Path,
    ) -> None:
        self.database = database
        self.config = config
        state = database.prepare_manifest_scan(
            config.scan_name, config.start_id, config.stop_id
        )
        self.metrics = CatalogScanMetrics(
            log_path, config.progress_interval, database.total_products(), state["next_id"]
        )
        self.fetcher = CatalogBatchFetcher(config, self.metrics)

    async def run(self) -> dict[str, object]:
        state = self.database.manifest_scan_state(self.config.scan_name)
        assert state is not None
        start_total = self.database.total_products()
        outcome = "complete"
        self.metrics.emit(
            "catalog_scan_started",
            next_id=state["next_id"],
            stop_id=state["stop_id"],
            batch_size=self.config.batch_size,
            workers=self.config.workers,
            requests_per_second=self.config.requests_per_second,
        )
        try:
            while state["next_id"] < state["stop_id"]:
                ranges: list[tuple[int, int]] = []
                cursor = state["next_id"]
                for _ in range(self.fetcher.concurrency.limit):
                    if cursor >= state["stop_id"]:
                        break
                    batch_stop = min(cursor + self.config.batch_size, state["stop_id"])
                    ranges.append((cursor, batch_stop))
                    cursor = batch_stop
                results = await asyncio.gather(
                    *(self.fetcher.fetch(start, stop) for start, stop in ranges)
                )
                for (batch_start, batch_stop), records in zip(ranges, results, strict=True):
                    unavailable = (batch_stop - batch_start) - len(records)
                    self.database.apply_catalog_batch(
                        self.config.scan_name,
                        batch_start,
                        batch_stop,
                        records,
                        unavailable,
                    )
                    state = self.database.manifest_scan_state(self.config.scan_name)
                    assert state is not None
                    self.metrics.record(
                        batch_stop - batch_start,
                        self.database.total_products(),
                        len(records),
                        batch_stop,
                    )
        except BaseException:
            outcome = "interrupted"
            raise
        finally:
            await self.fetcher.close()
            state = self.database.manifest_scan_state(self.config.scan_name)
            assert state is not None
            final = {
                **state,
                "outcome": outcome,
                "inserted": self.database.total_products() - start_total,
                "total_products": self.database.total_products(),
                "elapsed_seconds": round(time.monotonic() - self.metrics.started, 2),
            }
            self.metrics.emit("catalog_scan_finished", **final)
        return final


async def enrich_pending_catalog(
    database: CatalogDatabase,
    config: CatalogScanConfig,
    log_path: Path,
    limit: int | None = None,
) -> dict[str, object]:
    """Enrich only incomplete API records; completed batches disappear from the queue."""
    metrics = CatalogScanMetrics(log_path, config.progress_interval, 0, 0)
    fetcher = CatalogBatchFetcher(config, metrics)
    processed = 0
    enriched = 0
    after_id = 0
    outcome = "complete"
    metrics.emit(
        "catalog_enrichment_started",
        workers=config.workers,
        requests_per_second=config.requests_per_second,
        batch_size=config.batch_size,
        limit=limit,
    )
    try:
        while limit is None or processed < limit:
            groups: list[list[int]] = []
            cursor = after_id
            for _ in range(fetcher.concurrency.limit):
                batch_limit = config.batch_size
                if limit is not None:
                    batch_limit = min(batch_limit, limit - processed - sum(map(len, groups)))
                if batch_limit <= 0:
                    break
                ids = database.pending_source_product_ids(cursor, batch_limit)
                if not ids:
                    break
                groups.append(ids)
                cursor = ids[-1]
            if not groups:
                break
            results = await asyncio.gather(*(fetcher.fetch_ids(ids) for ids in groups))
            for ids, records in zip(groups, results, strict=True):
                enriched += database.apply_catalog_enrichment(records)
                processed += len(ids)
                after_id = ids[-1]
                if processed % config.progress_interval < len(ids):
                    metrics.emit(
                        "catalog_enrichment_progress",
                        processed=processed,
                        enriched=enriched,
                        last_product_id=after_id,
                    )
    except BaseException:
        outcome = "interrupted"
        raise
    finally:
        await fetcher.close()
        final = {
            "outcome": outcome,
            "processed": processed,
            "enriched": enriched,
            "last_product_id": after_id,
            "elapsed_seconds": round(time.monotonic() - metrics.started, 2),
        }
        metrics.emit("catalog_enrichment_finished", **final)
    return final
