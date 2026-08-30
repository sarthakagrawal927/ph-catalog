from __future__ import annotations

import asyncio
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import orjson

from ph_catalog.config import CrawlConfig
from ph_catalog.database import CatalogDatabase
from ph_catalog.models import FetchResult, PendingProduct, ProductStatus
from ph_catalog.network import (
    AdaptiveConcurrency,
    BlockController,
    GlobalRateLimiter,
    ProxyPool,
)
from ph_catalog.parsing import content_hash, parse_product_page


class CrawlMetrics:
    def __init__(self, log_path: Path, progress_interval: int) -> None:
        self.log_path = log_path
        self.progress_interval = progress_interval
        self.started = time.monotonic()
        self.processed = 0
        self.statuses: Counter[str] = Counter()
        self.http_statuses: Counter[str] = Counter()
        self.proxies: Counter[str] = Counter()
        self._next_progress = progress_interval
        log_path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, event: str, **fields: object) -> None:
        record = {
            "timestamp": datetime.now(UTC).isoformat(),
            "event": event,
            **fields,
        }
        line = orjson.dumps(record).decode()
        with self.log_path.open("a", encoding="utf-8") as stream:
            stream.write(line + "\n")
        print(line, flush=True)

    def record(self, result: FetchResult, concurrency: int) -> None:
        self.processed += 1
        self.statuses[result.status.value] += 1
        self.http_statuses[str(result.http_status or "transport")] += 1
        self.proxies[result.proxy_label] += 1
        if result.error:
            self.emit(
                "fetch_error",
                slug=result.requested_slug,
                attempt=result.attempt,
                status=result.status.value,
                http_status=result.http_status,
                proxy=result.proxy_label,
                error=result.error,
            )
        if self.processed >= self._next_progress:
            self.emit("progress", concurrency=concurrency, **self.as_dict())
            while self._next_progress <= self.processed:
                self._next_progress += self.progress_interval

    def as_dict(self) -> dict[str, object]:
        elapsed = time.monotonic() - self.started
        return {
            "processed": self.processed,
            "elapsed_seconds": round(elapsed, 2),
            "average_rps": round(self.processed / elapsed, 3) if elapsed else 0.0,
            "statuses": dict(self.statuses),
            "http_statuses": dict(self.http_statuses),
            "proxies": dict(self.proxies),
        }


class ProductFetcher:
    def __init__(self, config: CrawlConfig, proxy_urls: list[str], metrics: CrawlMetrics) -> None:
        self.config = config
        self.metrics = metrics
        self.rate = GlobalRateLimiter(
            config.requests_per_second, config.jitter_min, config.jitter_max
        )
        self.concurrency = AdaptiveConcurrency(config.workers)
        self.blocks = BlockController(config.block_threshold, config.max_block_backoff)
        self.proxies = ProxyPool(proxy_urls, config)

    async def close(self) -> None:
        await self.proxies.close()

    async def fetch(self, pending: PendingProduct) -> FetchResult | None:
        await self.concurrency.acquire()
        started = time.monotonic()
        try:
            while True:
                if not await self.blocks.wait():
                    return None
                await self.rate.wait()
                if await self.blocks.ready():
                    break
            selected = await self.proxies.next()
            try:
                response = await selected.client.get(pending.producthunt_url)
            except httpx.TransportError as exc:
                await self.proxies.transport_failure(selected.label)
                retry_at = self._retry_at(pending.attempt)
                return FetchResult(
                    requested_slug=pending.slug,
                    canonical_slug=pending.slug,
                    requested_url=pending.producthunt_url,
                    canonical_url=pending.producthunt_url,
                    attempt=pending.attempt,
                    status=ProductStatus.RETRY,
                    http_status=None,
                    proxy_label=selected.label,
                    elapsed_ms=int((time.monotonic() - started) * 1000),
                    error=f"transport: {type(exc).__name__}: {exc}",
                    retry_at=retry_at,
                )
        finally:
            await self.concurrency.release()

        elapsed_ms = int((time.monotonic() - started) * 1000)
        canonical_url = str(response.url).split("?", 1)[0].rstrip("/")
        canonical_slug = self._slug_from_final_url(canonical_url, pending.slug)

        if response.status_code in {403, 429}:
            delay, stopped = await self.blocks.blocked(response.headers.get("Retry-After"))
            concurrency = await self.concurrency.reduce()
            requests_per_second = await self.rate.reduce()
            self.metrics.emit(
                "global_backoff",
                http_status=response.status_code,
                delay_seconds=round(delay, 2),
                concurrency=concurrency,
                requests_per_second=requests_per_second,
                sustained_blocking=stopped,
            )
            return FetchResult(
                requested_slug=pending.slug,
                canonical_slug=pending.slug,
                requested_url=pending.producthunt_url,
                canonical_url=pending.producthunt_url,
                attempt=pending.attempt,
                status=ProductStatus.RETRY,
                http_status=response.status_code,
                proxy_label=selected.label,
                elapsed_ms=elapsed_ms,
                error=f"blocked with HTTP {response.status_code}",
                retry_at=datetime.now(UTC) + timedelta(seconds=delay),
                fixture_html=response.content,
            )

        if response.status_code in {404, 410}:
            return self._http_error(
                pending, selected.label, response.status_code, elapsed_ms, ProductStatus.UNAVAILABLE
            )
        if response.status_code >= 500:
            return self._http_error(
                pending,
                selected.label,
                response.status_code,
                elapsed_ms,
                ProductStatus.RETRY,
                retry_at=self._retry_at(pending.attempt),
            )
        if response.status_code >= 400:
            return self._http_error(
                pending, selected.label, response.status_code, elapsed_ms, ProductStatus.UNAVAILABLE
            )

        await self.blocks.success()
        await self.concurrency.success()
        await self.rate.success()
        data = parse_product_page(response.content, pending.slug, canonical_url)
        if not data.is_complete_enough:
            missing = [
                field
                for field, value in (
                    ("name", data.name),
                    ("description", data.description),
                    ("tagline_or_website", data.tagline or data.website_url),
                )
                if not value
            ]
            return FetchResult(
                requested_slug=pending.slug,
                canonical_slug=canonical_slug,
                requested_url=pending.producthunt_url,
                canonical_url=f"https://www.producthunt.com/products/{canonical_slug}",
                attempt=pending.attempt,
                status=ProductStatus.PARSE_FAILED,
                http_status=response.status_code,
                proxy_label=selected.label,
                elapsed_ms=elapsed_ms,
                data=data,
                error=f"missing required fields: {', '.join(missing)}",
                retry_at=self._retry_at(pending.attempt),
                fixture_html=response.content,
            )

        return FetchResult(
            requested_slug=pending.slug,
            canonical_slug=canonical_slug,
            requested_url=pending.producthunt_url,
            canonical_url=f"https://www.producthunt.com/products/{canonical_slug}",
            attempt=pending.attempt,
            status=ProductStatus.FETCHED,
            http_status=response.status_code,
            proxy_label=selected.label,
            elapsed_ms=elapsed_ms,
            data=data,
            content_hash=content_hash(data),
        )

    def _http_error(
        self,
        pending: PendingProduct,
        proxy_label: str,
        status_code: int,
        elapsed_ms: int,
        status: ProductStatus,
        retry_at: datetime | None = None,
    ) -> FetchResult:
        return FetchResult(
            requested_slug=pending.slug,
            canonical_slug=pending.slug,
            requested_url=pending.producthunt_url,
            canonical_url=pending.producthunt_url,
            attempt=pending.attempt,
            status=status,
            http_status=status_code,
            proxy_label=proxy_label,
            elapsed_ms=elapsed_ms,
            error=f"HTTP {status_code}",
            retry_at=retry_at,
        )

    @staticmethod
    def _slug_from_final_url(url: str, fallback: str) -> str:
        parts = [part for part in httpx.URL(url).path.split("/") if part]
        return parts[1].lower() if len(parts) == 2 and parts[0] == "products" else fallback

    @staticmethod
    def _retry_at(attempt: int) -> datetime:
        seconds = min(3600, 15 * (2 ** max(0, attempt - 1)))
        return datetime.now(UTC) + timedelta(seconds=seconds)


class Crawler:
    def __init__(
        self,
        database: CatalogDatabase,
        config: CrawlConfig,
        proxy_urls: list[str],
    ) -> None:
        self.database = database
        self.config = config
        self.metrics = CrawlMetrics(config.log_path, config.progress_interval)
        self.fetcher = ProductFetcher(config, proxy_urls, self.metrics)

    async def run(self, limit: int | None = None) -> dict[str, object]:
        run_id = self.database.begin_run("crawl")
        outcome = "complete"
        buffer: list[FetchResult] = []
        remaining = limit
        self.metrics.emit(
            "crawl_started",
            run_id=run_id,
            workers=self.config.workers,
            requests_per_second=self.config.requests_per_second,
            proxy_count=self.fetcher.proxies.size,
            progress_interval=self.config.progress_interval,
            limit=limit,
        )
        try:
            while remaining is None or remaining > 0:
                claim_size = self.config.batch_size * 2
                if remaining is not None:
                    claim_size = min(claim_size, remaining)
                pending = self.database.claim_products(claim_size, self.config.max_attempts)
                if not pending:
                    break

                tasks = [asyncio.create_task(self.fetcher.fetch(item)) for item in pending]
                for task in asyncio.as_completed(tasks):
                    result = await task
                    if result is None:
                        continue
                    buffer.append(result)
                    self.metrics.record(result, self.fetcher.concurrency.limit)
                    self._save_failed_fixture(result)
                    if len(buffer) >= self.config.batch_size:
                        self.database.apply_results(buffer)
                        buffer.clear()
                    if (
                        self.config.snapshot_dir is not None
                        and self.metrics.processed % self.config.snapshot_interval == 0
                    ):
                        if buffer:
                            self.database.apply_results(buffer)
                            buffer.clear()
                        output = self.config.snapshot_dir / (
                            f"producthunt-checkpoint-{self.metrics.processed:09d}.parquet"
                        )
                        exported = self.database.snapshot(output)
                        self.metrics.emit(
                            "snapshot",
                            output=str(output),
                            exported=exported,
                            processed=self.metrics.processed,
                        )
                    if remaining is not None:
                        remaining -= 1

                if buffer:
                    self.database.apply_results(buffer)
                    buffer.clear()
                if self.fetcher.blocks.stop:
                    outcome = "paused_sustained_blocking"
                    break
        except BaseException:
            outcome = "interrupted"
            if buffer:
                self.database.apply_results(buffer)
                buffer.clear()
            raise
        finally:
            await self.fetcher.close()
            metrics = self.metrics.as_dict()
            self.database.finish_run(run_id, outcome, metrics)
            self.metrics.emit("crawl_finished", run_id=run_id, outcome=outcome, **metrics)
        return metrics | {"outcome": outcome}

    def _save_failed_fixture(self, result: FetchResult) -> None:
        if result.fixture_html is None or self.config.failed_fixture_cap <= 0:
            return
        fixture_dir = self.config.failed_fixtures_dir
        fixture_dir.mkdir(parents=True, exist_ok=True)
        existing = sorted(fixture_dir.glob("*.html"), key=lambda path: path.stat().st_mtime)
        if len(existing) >= self.config.failed_fixture_cap:
            return
        safe_slug = "".join(
            character
            for character in result.requested_slug
            if character.isalnum() or character in "-_"
        )[:100]
        target = fixture_dir / f"{safe_slug}-attempt-{result.attempt}.html"
        target.write_bytes(result.fixture_html[: self.config.failed_fixture_bytes])
