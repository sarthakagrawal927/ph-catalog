from __future__ import annotations

import asyncio
import re
import time
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx
import orjson
from selectolax.parser import HTMLParser

from ph_catalog.database import CatalogDatabase
from ph_catalog.models import ProductData
from ph_catalog.network import AdaptiveConcurrency, BlockController, GlobalRateLimiter
from ph_catalog.parsing import content_hash, parse_product_page

WAYBACK_AVAILABLE_URL = "https://archive.org/wayback/available"


@dataclass(slots=True, frozen=True)
class ArchiveRecovery:
    slug: str
    attempt: int
    outcome: str
    capture_timestamp: str | None = None
    http_status: int | None = None
    error: str | None = None
    data: ProductData | None = None
    content_hash: str | None = None


@dataclass(slots=True, frozen=True)
class ArchivePostRecovery:
    post_slug: str
    attempt: int
    outcome: str
    capture_timestamp: str | None = None
    http_status: int | None = None
    error: str | None = None
    data: ProductData | None = None
    content_hash: str | None = None


@dataclass(slots=True, frozen=True)
class ArchiveRecoveryConfig:
    workers: int = 4
    requests_per_second: float = 1.0
    batch_size: int = 100
    timeout: float = 45.0
    max_attempts: int = 3
    block_threshold: int = 5
    max_block_backoff: float = 900.0
    target_timestamp: str | None = None
    limit: int | None = None

    def __post_init__(self) -> None:
        if not 1 <= self.workers <= 8:
            raise ValueError("archive recovery workers must be between 1 and 8")
        if self.requests_per_second <= 0:
            raise ValueError("archive recovery requests per second must be positive")
        if not 1 <= self.batch_size <= 500:
            raise ValueError("archive recovery batch size must be between 1 and 500")
        if self.timeout <= 0 or self.max_attempts <= 0:
            raise ValueError("archive recovery timeout and attempts must be positive")
        if self.target_timestamp and (
            len(self.target_timestamp) != 8 or not self.target_timestamp.isdigit()
        ):
            raise ValueError("archive recovery timestamp must be YYYYMMDD")


class WaybackRecovery:
    def __init__(
        self,
        database: CatalogDatabase,
        config: ArchiveRecoveryConfig,
        log_path: Path,
    ) -> None:
        self.database = database
        self.config = config
        self.log_path = log_path
        self.rate = GlobalRateLimiter(config.requests_per_second, 0.05, 0.2)
        self.concurrency = AdaptiveConcurrency(config.workers)
        self.blocks = BlockController(config.block_threshold, config.max_block_backoff)
        self.target_timestamp = config.target_timestamp or datetime.now(UTC).strftime("%Y%m%d")

    async def run(self) -> dict[str, object]:
        started = time.monotonic()
        processed = 0
        requests = 0
        outcomes: Counter[str] = Counter()
        self._emit(
            "archive_recovery_started",
            workers=self.config.workers,
            requests_per_second=self.config.requests_per_second,
            target_timestamp=self.target_timestamp,
            limit=self.config.limit,
        )
        headers = {"User-Agent": "ph-catalog/0.1 (local public-catalogue research)"}
        async with httpx.AsyncClient(
            headers=headers,
            timeout=httpx.Timeout(self.config.timeout),
            follow_redirects=True,
        ) as client:
            while self.config.limit is None or processed < self.config.limit:
                remaining = self.config.batch_size
                if self.config.limit is not None:
                    remaining = min(remaining, self.config.limit - processed)
                claimed = self.database.claim_archive_recoveries(
                    remaining, self.config.max_attempts
                )
                if not claimed:
                    break
                recoveries = await asyncio.gather(
                    *(self._recover(client, slug, attempt) for slug, attempt in claimed)
                )
                self.database.apply_archive_recoveries(recoveries)
                processed += len(recoveries)
                requests += sum(
                    1 if item.outcome == "no_capture" else 2 for item in recoveries
                )
                outcomes.update(item.outcome for item in recoveries)
                self._emit(
                    "archive_recovery_progress",
                    processed=processed,
                    outcomes=dict(outcomes),
                    catalogue=self.database.counts(),
                )
        result = {
            "processed": processed,
            "estimated_requests": requests,
            "outcomes": dict(outcomes),
            "catalogue": self.database.counts(),
            "recovery": self.database.archive_recovery_counts(),
            "elapsed_seconds": round(time.monotonic() - started, 2),
        }
        self._emit("archive_recovery_finished", **result)
        return result

    async def _recover(
        self, client: httpx.AsyncClient, slug: str, attempt: int
    ) -> ArchiveRecovery:
        await self.concurrency.acquire()
        try:
            canonical_url = f"https://www.producthunt.com/products/{slug}"
            try:
                available = await self._request(
                    client,
                    WAYBACK_AVAILABLE_URL,
                    params={"url": canonical_url, "timestamp": self.target_timestamp},
                )
                payload = available.json()
                closest = payload.get("archived_snapshots", {}).get("closest", {})
                timestamp = closest.get("timestamp")
                if not isinstance(timestamp, str) or not timestamp.isdigit():
                    return ArchiveRecovery(slug, attempt, "no_capture")
                playback_url = (
                    f"https://web.archive.org/web/{timestamp}id_/{canonical_url}"
                )
                playback = await self._request(client, playback_url)
                data = parse_product_page(playback.content, slug, canonical_url)
                if not self._has_all_fields(data):
                    return ArchiveRecovery(
                        slug,
                        attempt,
                        "parse_failed",
                        timestamp,
                        playback.status_code,
                        "archived page is missing one or more catalogue fields",
                    )
                return ArchiveRecovery(
                    slug,
                    attempt,
                    "recovered",
                    timestamp,
                    playback.status_code,
                    data=data,
                    content_hash=content_hash(data),
                )
            except (httpx.HTTPError, RuntimeError, ValueError) as exc:
                return ArchiveRecovery(
                    slug,
                    attempt,
                    "retry",
                    error=f"{type(exc).__name__}: {exc}",
                )
        finally:
            await self.concurrency.release()

    async def _request(
        self,
        client: httpx.AsyncClient,
        url: str,
        *,
        params: dict[str, str] | None = None,
    ) -> httpx.Response:
        for attempt in range(4):
            if not await self.blocks.wait():
                raise RuntimeError("archive recovery paused after sustained blocking")
            await self.rate.wait()
            try:
                response = await client.get(url, params=params)
            except httpx.TransportError:
                if attempt == 3:
                    raise
                await asyncio.sleep(2**attempt)
                continue
            if response.status_code in {403, 429}:
                delay, stopped = await self.blocks.blocked(
                    response.headers.get("Retry-After")
                )
                concurrency = await self.concurrency.reduce()
                requests_per_second = await self.rate.reduce()
                self._emit(
                    "archive_recovery_backoff",
                    http_status=response.status_code,
                    delay_seconds=round(delay, 2),
                    concurrency=concurrency,
                    requests_per_second=requests_per_second,
                    sustained_blocking=stopped,
                )
                if stopped:
                    raise RuntimeError("archive recovery paused after sustained blocking")
                continue
            if response.status_code >= 500:
                if attempt == 3:
                    response.raise_for_status()
                await asyncio.sleep(2**attempt)
                continue
            response.raise_for_status()
            await self.blocks.success()
            await self.concurrency.success()
            await self.rate.success()
            return response
        raise RuntimeError("archive recovery retries exhausted")

    @staticmethod
    def _has_all_fields(data: ProductData) -> bool:
        return bool(data.name and data.tagline and data.description and data.website_url)

    def _emit(self, event: str, **fields: object) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        record = {"timestamp": datetime.now(UTC).isoformat(), "event": event, **fields}
        line = orjson.dumps(record).decode()
        with self.log_path.open("a", encoding="utf-8") as stream:
            stream.write(line + "\n")
        print(line, flush=True)


class WaybackPostRecovery(WaybackRecovery):
    """Recover canonical products from archived launch pages."""

    async def run(self) -> dict[str, object]:
        started = time.monotonic()
        processed = additions = 0
        outcomes: Counter[str] = Counter()
        self._emit(
            "archive_post_recovery_started",
            workers=self.config.workers,
            requests_per_second=self.config.requests_per_second,
            target_timestamp=self.target_timestamp,
            limit=self.config.limit,
        )
        headers = {"User-Agent": "ph-catalog/0.1 (local public-catalogue research)"}
        async with httpx.AsyncClient(
            headers=headers,
            timeout=httpx.Timeout(self.config.timeout),
            follow_redirects=True,
        ) as client:
            while self.config.limit is None or processed < self.config.limit:
                remaining = self.config.batch_size
                if self.config.limit is not None:
                    remaining = min(remaining, self.config.limit - processed)
                claimed = self.database.claim_archive_post_recoveries(
                    remaining, self.config.max_attempts
                )
                if not claimed:
                    break
                recoveries = await asyncio.gather(
                    *(self._recover_post(client, slug, attempt) for slug, attempt in claimed)
                )
                additions += self.database.apply_archive_post_recoveries(recoveries)
                processed += len(recoveries)
                outcomes.update(item.outcome for item in recoveries)
                self._emit(
                    "archive_post_recovery_progress",
                    processed=processed,
                    additions=additions,
                    outcomes=dict(outcomes),
                    catalogue=self.database.counts(),
                )
        result = {
            "processed": processed,
            "additions": additions,
            "outcomes": dict(outcomes),
            "catalogue": self.database.counts(),
            "recovery": self.database.archive_post_recovery_counts(),
            "elapsed_seconds": round(time.monotonic() - started, 2),
        }
        self._emit("archive_post_recovery_finished", **result)
        return result

    async def _recover_post(
        self, client: httpx.AsyncClient, post_slug: str, attempt: int
    ) -> ArchivePostRecovery:
        await self.concurrency.acquire()
        try:
            post_url = f"https://www.producthunt.com/posts/{post_slug}"
            try:
                available = await self._request(
                    client,
                    WAYBACK_AVAILABLE_URL,
                    params={"url": post_url, "timestamp": self.target_timestamp},
                )
                payload = available.json()
                closest = payload.get("archived_snapshots", {}).get("closest", {})
                timestamp = closest.get("timestamp")
                if not isinstance(timestamp, str) or not timestamp.isdigit():
                    return ArchivePostRecovery(post_slug, attempt, "no_capture")
                playback = await self._request(
                    client,
                    f"https://web.archive.org/web/{timestamp}id_/{post_url}",
                )
                product_slugs = self._product_slugs(playback.content)
                legacy_data = None
                if not product_slugs:
                    legacy_data = self._legacy_product_data(playback.content, post_slug)
                if len(product_slugs) != 1 and legacy_data is None:
                    return ArchivePostRecovery(
                        post_slug,
                        attempt,
                        "parse_failed",
                        timestamp,
                        playback.status_code,
                        f"expected one canonical product link, found {len(product_slugs)}",
                    )
                if legacy_data is not None:
                    data = legacy_data
                else:
                    product_slug = product_slugs[0]
                    product_url = f"https://www.producthunt.com/products/{product_slug}"
                    data = parse_product_page(playback.content, product_slug, product_url)
                if not self._has_all_fields(data):
                    return ArchivePostRecovery(
                        post_slug,
                        attempt,
                        "parse_failed",
                        timestamp,
                        playback.status_code,
                        "archived launch product is missing catalogue fields",
                    )
                return ArchivePostRecovery(
                    post_slug,
                    attempt,
                    "recovered",
                    timestamp,
                    playback.status_code,
                    data=data,
                    content_hash=content_hash(data),
                )
            except (httpx.HTTPError, RuntimeError, ValueError) as exc:
                return ArchivePostRecovery(
                    post_slug,
                    attempt,
                    "retry",
                    error=f"{type(exc).__name__}: {exc}",
                )
        finally:
            await self.concurrency.release()

    @staticmethod
    def _product_slugs(html: bytes) -> list[str]:
        found: list[str] = []
        tree = HTMLParser(html)
        for node in tree.css("a[href]"):
            path = urlparse(node.attributes.get("href", "")).path
            match = re.match(r"^/products/([a-zA-Z0-9._-]+)", path)
            if match:
                slug = match.group(1).lower()
                if slug not in found:
                    found.append(slug)
        pattern = rb"https?://(?:www\.)?producthunt\.com/products/([a-zA-Z0-9._-]+)"
        for match in re.finditer(pattern, html):
            slug = match.group(1).decode().lower()
            if slug not in found:
                found.append(slug)
        return found

    @staticmethod
    def _legacy_product_data(html: bytes, post_slug: str) -> ProductData | None:
        """Extract a product only when a legacy Post explicitly references it."""

        def mappings(value: object):
            if isinstance(value, dict):
                yield value
                for child in value.values():
                    yield from mappings(child)
            elif isinstance(value, list):
                for child in value:
                    yield from mappings(child)

        def typename(value: object) -> str | None:
            if not isinstance(value, dict):
                return None
            kind = value.get("__typename") or value.get("typename")
            return kind if isinstance(kind, str) else None

        def reference_id(value: object) -> str | None:
            if not isinstance(value, dict):
                return None
            identifier = value.get("id")
            return identifier if isinstance(identifier, str) else None

        def referenced(container: dict[str, object], value: object) -> object:
            identifier = reference_id(value)
            return container.get(identifier) if identifier else value

        candidates: dict[str, ProductData] = {}
        tree = HTMLParser(html)
        for script in tree.css("script:not([src])"):
            raw = script.text().strip()
            if not raw:
                continue
            try:
                payload = orjson.loads(raw)
            except orjson.JSONDecodeError:
                continue
            for container in mappings(payload):
                for post in container.values():
                    if (
                        not isinstance(post, dict)
                        or typename(post) != "Post"
                        or post.get("slug") != post_slug
                    ):
                        continue
                    product = referenced(container, post.get("product"))
                    if not isinstance(product, dict) or typename(product) != "Product":
                        continue
                    product_slug = product.get("slug")
                    if not isinstance(product_slug, str) or not re.fullmatch(
                        r"[a-z0-9](?:[a-z0-9._-]*[a-z0-9])?", product_slug
                    ):
                        continue

                    websites: list[str] = []
                    links = post.get("productLinks") or post.get("product_links")
                    if isinstance(links, list):
                        for link_ref in links:
                            link = referenced(container, link_ref)
                            if not isinstance(link, dict) or typename(link) != "ProductLink":
                                continue
                            store = link.get("storeName") or link.get("store_name")
                            domain = link.get("websiteName") or link.get("website_name")
                            if not (
                                isinstance(store, str)
                                and store.lower() == "website"
                                and isinstance(domain, str)
                            ):
                                continue
                            website = domain.strip()
                            if "://" not in website:
                                website = f"https://{website}"
                            parsed = urlparse(website)
                            if parsed.scheme in {"http", "https"} and parsed.hostname:
                                websites.append(website)
                    websites = list(dict.fromkeys(websites))
                    if len(websites) != 1:
                        continue

                    categories: list[str] = []
                    topics = referenced(container, post.get("topics"))
                    if isinstance(topics, dict):
                        edges = topics.get("edges")
                        if isinstance(edges, list):
                            for edge_ref in edges:
                                edge = referenced(container, edge_ref)
                                if not isinstance(edge, dict):
                                    continue
                                topic = referenced(container, edge.get("node"))
                                name = topic.get("name") if isinstance(topic, dict) else None
                                if isinstance(name, str) and name.strip() not in categories:
                                    categories.append(name.strip())

                    canonical_url = (
                        f"https://www.producthunt.com/products/{product_slug}"
                    )
                    data = parse_product_page(html, product_slug, canonical_url)
                    for field in ("name", "tagline", "description"):
                        current = getattr(data, field)
                        value = post.get(field)
                        if not current and isinstance(value, str) and value.strip():
                            setattr(data, field, " ".join(value.split()))
                    data.website_url = websites[0]
                    if categories:
                        data.categories = categories
                    candidates[product_slug] = data

        if len(candidates) != 1:
            return None
        return next(iter(candidates.values()))
