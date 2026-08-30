from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx

from ph_catalog.config import CrawlConfig


class GlobalRateLimiter:
    def __init__(self, rate: float, jitter_min: float, jitter_max: float) -> None:
        self.initial_rate = rate
        self.rate = rate
        self.interval = 1.0 / rate
        self.jitter_min = jitter_min
        self.jitter_max = jitter_max
        self._next_at = 0.0
        self._successes = 0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            slot = max(now, self._next_at)
            self._next_at = slot + self.interval + random.uniform(self.jitter_min, self.jitter_max)
        delay = slot - now
        if delay > 0:
            await asyncio.sleep(delay)

    async def reduce(self) -> float:
        async with self._lock:
            self.rate = max(0.25, self.rate / 2)
            self.interval = 1.0 / self.rate
            self._successes = 0
            return self.rate

    async def success(self) -> float:
        async with self._lock:
            self._successes += 1
            if self.rate < self.initial_rate and self._successes >= 100:
                self.rate = min(self.initial_rate, self.rate * 1.25)
                self.interval = 1.0 / self.rate
                self._successes = 0
            return self.rate


class AdaptiveConcurrency:
    def __init__(self, initial: int) -> None:
        self.initial = initial
        self.limit = initial
        self.active = 0
        self.successes_since_block = 0
        self._condition = asyncio.Condition()

    async def acquire(self) -> None:
        async with self._condition:
            await self._condition.wait_for(lambda: self.active < self.limit)
            self.active += 1

    async def release(self) -> None:
        async with self._condition:
            self.active -= 1
            self._condition.notify_all()

    async def reduce(self) -> int:
        async with self._condition:
            self.limit = max(1, self.limit - 1)
            self.successes_since_block = 0
            return self.limit

    async def success(self) -> int:
        async with self._condition:
            self.successes_since_block += 1
            if self.limit < self.initial and self.successes_since_block >= 100:
                self.limit += 1
                self.successes_since_block = 0
                self._condition.notify_all()
            return self.limit


def retry_after_seconds(value: str | None, default: float) -> float:
    if not value:
        return default
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
            return max(0.0, (parsed - datetime.now(UTC)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return default


class BlockController:
    def __init__(self, threshold: int, max_backoff: float) -> None:
        self.threshold = threshold
        self.max_backoff = max_backoff
        self.consecutive_blocks = 0
        self.blocked_until = 0.0
        self.stop = False
        self._lock = asyncio.Lock()

    async def wait(self) -> bool:
        while True:
            async with self._lock:
                if self.stop:
                    return False
                delay = self.blocked_until - time.monotonic()
            if delay <= 0:
                return True
            await asyncio.sleep(min(delay, 30.0))

    async def ready(self) -> bool:
        async with self._lock:
            return not self.stop and self.blocked_until <= time.monotonic()

    async def blocked(self, retry_after: str | None) -> tuple[float, bool]:
        async with self._lock:
            self.consecutive_blocks += 1
            fallback = min(30.0 * (2 ** (self.consecutive_blocks - 1)), self.max_backoff)
            delay = min(retry_after_seconds(retry_after, fallback), self.max_backoff)
            self.blocked_until = max(self.blocked_until, time.monotonic() + delay)
            if self.consecutive_blocks >= self.threshold:
                self.stop = True
            return delay, self.stop

    async def success(self) -> None:
        async with self._lock:
            self.consecutive_blocks = 0


@dataclass(slots=True, frozen=True)
class ProxyClient:
    label: str
    client: httpx.AsyncClient


class ProxyPool:
    """Sticky proxy endpoints with one shared fixed-schedule rotation counter."""

    def __init__(self, urls: list[str], config: CrawlConfig) -> None:
        endpoints: list[str | None] = urls or [None]
        self._clients = [
            ProxyClient(
                label="direct" if url is None else f"proxy-{index + 1}",
                client=httpx.AsyncClient(
                    proxy=url,
                    follow_redirects=True,
                    timeout=httpx.Timeout(config.request_timeout),
                    headers={
                        "User-Agent": config.user_agent,
                        "Accept": "text/html,application/xhtml+xml",
                        "Accept-Language": "en-US,en;q=0.8",
                    },
                    limits=httpx.Limits(
                        max_connections=config.workers,
                        max_keepalive_connections=config.workers,
                    ),
                ),
            )
            for index, url in enumerate(endpoints)
        ]
        self.rotation_requests = config.proxy_rotation_requests
        self._index = 0
        self._requests_on_current = 0
        self._lock = asyncio.Lock()

    @property
    def size(self) -> int:
        return len(self._clients)

    async def next(self) -> ProxyClient:
        async with self._lock:
            selected = self._clients[self._index]
            self._requests_on_current += 1
            if self._requests_on_current >= self.rotation_requests:
                self._index = (self._index + 1) % len(self._clients)
                self._requests_on_current = 0
            return selected

    async def transport_failure(self, label: str) -> None:
        async with self._lock:
            if len(self._clients) > 1 and self._clients[self._index].label == label:
                self._index = (self._index + 1) % len(self._clients)
                self._requests_on_current = 0

    async def close(self) -> None:
        await asyncio.gather(*(item.client.aclose() for item in self._clients))


def load_proxy_urls(inline: list[str], proxy_file: Path | None) -> list[str]:
    values = list(inline)
    if proxy_file:
        values.extend(
            line.strip()
            for line in proxy_file.read_text().splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        )
    for value in values:
        parsed = urlparse(value)
        if parsed.scheme not in {"http", "https", "socks5", "socks5h"} or not parsed.hostname:
            raise ValueError("proxy endpoints must be valid HTTP(S) or SOCKS URLs")
    return values
