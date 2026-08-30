from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import socket
import time
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import httpx
import orjson

from ph_catalog.database import CatalogDatabase
from ph_catalog.network import GlobalRateLimiter
from ph_catalog.website_enrichment import extract_website_metadata


@dataclass(slots=True, frozen=True)
class WebsiteEnrichmentConfig:
    workers: int = 4
    requests_per_second: float = 2.0
    timeout: float = 30.0
    max_attempts: int = 3
    batch_size: int = 100
    max_response_bytes: int = 2_000_000
    max_redirects: int = 5
    limit: int | None = None
    apply_fields: bool = False

    def __post_init__(self) -> None:
        if not 1 <= self.workers <= 8:
            raise ValueError("website enrichment workers must be between 1 and 8")
        if not 0 < self.requests_per_second <= 2:
            raise ValueError("website enrichment rate must be greater than zero and at most 2 RPS")
        if self.timeout <= 0 or self.max_attempts < 1:
            raise ValueError("invalid website enrichment retry configuration")
        if not 1 <= self.batch_size <= 500:
            raise ValueError("website enrichment batch size must be between 1 and 500")
        if self.max_response_bytes < 1 or self.max_redirects < 0:
            raise ValueError("invalid website enrichment response limits")


@dataclass(slots=True, frozen=True)
class WebsiteEnrichmentResult:
    slug: str
    requested_url: str
    final_url: str | None
    attempt: int
    outcome: str
    http_status: int | None = None
    tagline: str | None = None
    tagline_source: str | None = None
    description: str | None = None
    description_source: str | None = None
    response_hash: str | None = None
    error: str | None = None


class WebsiteEnricher:
    """Fetch one public landing page per incomplete product and discard its HTML."""

    def __init__(
        self,
        database: CatalogDatabase,
        config: WebsiteEnrichmentConfig,
        log_path: Path,
    ) -> None:
        self.database = database
        self.config = config
        self.log_path = log_path
        self.rate = GlobalRateLimiter(config.requests_per_second, 0.05, 0.2)
        self.semaphore = asyncio.Semaphore(config.workers)

    async def run(self) -> dict[str, object]:
        started = time.monotonic()
        processed = requests = taglines = descriptions = 0
        outcomes: Counter[str] = Counter()
        self._emit(
            "website_enrichment_started",
            workers=self.config.workers,
            requests_per_second=self.config.requests_per_second,
            limit=self.config.limit,
        )
        headers = {
            "User-Agent": "ph-catalog/0.1 (single-page metadata enrichment)",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.8",
        }
        limits = httpx.Limits(
            max_connections=self.config.workers,
            max_keepalive_connections=self.config.workers,
        )
        async with httpx.AsyncClient(
            headers=headers,
            timeout=httpx.Timeout(self.config.timeout),
            limits=limits,
            follow_redirects=False,
            trust_env=False,
        ) as client:
            while self.config.limit is None or processed < self.config.limit:
                remaining = self.config.batch_size
                if self.config.limit is not None:
                    remaining = min(remaining, self.config.limit - processed)
                targets = self.database.claim_website_enrichments(
                    remaining, self.config.max_attempts
                )
                if not targets:
                    break
                results = await asyncio.gather(
                    *(self._enrich(client, *target) for target in targets)
                )
                applied = self.database.apply_website_enrichments(
                    results, apply_fields=self.config.apply_fields
                )
                processed += len(results)
                requests += sum(result.http_status is not None for result in results)
                taglines += applied["taglines_applied"]
                descriptions += applied["descriptions_applied"]
                outcomes.update(result.outcome for result in results)
                self._emit(
                    "website_enrichment_progress",
                    processed=processed,
                    outcomes=dict(outcomes),
                    taglines_applied=taglines,
                    descriptions_applied=descriptions,
                )
        result: dict[str, object] = {
            "processed": processed,
            "requests_with_response": requests,
            "outcomes": dict(outcomes),
            "taglines_applied": taglines,
            "descriptions_applied": descriptions,
            "remaining_missing": self._remaining_missing(),
            "apply_fields": self.config.apply_fields,
            "elapsed_seconds": round(time.monotonic() - started, 2),
        }
        self._emit("website_enrichment_finished", **result)
        return result

    async def _enrich(
        self,
        client: httpx.AsyncClient,
        slug: str,
        product_name: str,
        requested_url: str,
        attempt: int,
        needs_tagline: bool,
        needs_description: bool,
    ) -> WebsiteEnrichmentResult:
        async with self.semaphore:
            try:
                response, body = await self._fetch(client, requested_url)
            except UnsafeWebsiteUrl as exc:
                return WebsiteEnrichmentResult(
                    slug, requested_url, None, attempt, "invalid_url", error=str(exc)
                )
            except ResponseTooLarge as exc:
                return WebsiteEnrichmentResult(
                    slug,
                    requested_url,
                    exc.url,
                    attempt,
                    "too_large",
                    exc.status_code,
                    error=str(exc),
                )
            except httpx.TransportError as exc:
                return WebsiteEnrichmentResult(
                    slug,
                    requested_url,
                    None,
                    attempt,
                    "retry",
                    error=f"{type(exc).__name__}: {exc}",
                )

            final_url = str(response.url)
            if response.status_code in {401, 403, 429}:
                return WebsiteEnrichmentResult(
                    slug, requested_url, final_url, attempt, "blocked", response.status_code
                )
            if response.status_code in {404, 410}:
                return WebsiteEnrichmentResult(
                    slug, requested_url, final_url, attempt, "unavailable", response.status_code
                )
            if response.status_code >= 500:
                return WebsiteEnrichmentResult(
                    slug,
                    requested_url,
                    final_url,
                    attempt,
                    "retry",
                    response.status_code,
                    error=f"website returned HTTP {response.status_code}",
                )
            if response.status_code >= 400:
                return WebsiteEnrichmentResult(
                    slug, requested_url, final_url, attempt, "unavailable", response.status_code
                )
            content_type = response.headers.get("content-type", "").lower()
            if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
                return WebsiteEnrichmentResult(
                    slug, requested_url, final_url, attempt, "non_html", response.status_code
                )

            metadata = extract_website_metadata(body, product_name=product_name)
            tagline = metadata.tagline if needs_tagline else None
            description = metadata.description if needs_description else None
            outcome = "enriched" if tagline or description else "no_metadata"
            return WebsiteEnrichmentResult(
                slug=slug,
                requested_url=requested_url,
                final_url=final_url,
                attempt=attempt,
                outcome=outcome,
                http_status=response.status_code,
                tagline=tagline.value if tagline else None,
                tagline_source=tagline.source if tagline else None,
                description=description.value if description else None,
                description_source=description.source if description else None,
                response_hash=hashlib.sha256(body).hexdigest(),
            )

    async def _fetch(
        self, client: httpx.AsyncClient, requested_url: str
    ) -> tuple[httpx.Response, bytes]:
        current = requested_url
        for _ in range(self.config.max_redirects + 1):
            await _validate_public_url(current)
            await self.rate.wait()
            async with client.stream("GET", current) as response:
                if response.is_redirect:
                    location = response.headers.get("location")
                    if not location:
                        return response, b""
                    current = urljoin(str(response.url), location)
                    continue
                content_length = response.headers.get("content-length")
                if (
                    content_length
                    and content_length.isdigit()
                    and int(content_length) > self.config.max_response_bytes
                ):
                    raise ResponseTooLarge(str(response.url), response.status_code)
                chunks: list[bytes] = []
                size = 0
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > self.config.max_response_bytes:
                        raise ResponseTooLarge(str(response.url), response.status_code)
                    chunks.append(chunk)
                return response, b"".join(chunks)
        raise UnsafeWebsiteUrl("website exceeded the redirect limit")

    def _remaining_missing(self) -> dict[str, int]:
        row = self.database.connection.execute(
            """
            SELECT count(*) FILTER (WHERE coalesce(trim(tagline), '') = ''),
                   count(*) FILTER (WHERE coalesce(trim(description), '') = '')
            FROM products WHERE status = 'fetched'
            """
        ).fetchone()
        return {"tagline": int(row[0]), "description": int(row[1])}

    def _emit(self, event: str, **fields: object) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        record = {"timestamp": datetime.now(UTC).isoformat(), "event": event, **fields}
        line = orjson.dumps(record).decode()
        with self.log_path.open("a", encoding="utf-8") as stream:
            stream.write(line + "\n")
        print(line, flush=True)


class UnsafeWebsiteUrl(ValueError):
    pass


class ResponseTooLarge(ValueError):
    def __init__(self, url: str, status_code: int) -> None:
        self.url = url
        self.status_code = status_code
        super().__init__(f"website response exceeds the configured byte limit: {url}")


async def _validate_public_url(url: str) -> None:
    parsed = urlsplit(url)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        raise UnsafeWebsiteUrl("website URL must use HTTP(S) and contain a hostname")
    if parsed.username or parsed.password:
        raise UnsafeWebsiteUrl("website URL must not contain credentials")
    if parsed.port not in {None, 80, 443}:
        raise UnsafeWebsiteUrl("website URL uses a nonstandard port")
    hostname = parsed.hostname.rstrip(".").lower()
    if hostname == "localhost" or hostname.endswith((".localhost", ".local", ".internal")):
        raise UnsafeWebsiteUrl("website URL points to a local hostname")
    try:
        addresses = [ipaddress.ip_address(hostname)]
    except ValueError:
        loop = asyncio.get_running_loop()
        try:
            records = await loop.getaddrinfo(
                hostname,
                parsed.port or (443 if parsed.scheme.lower() == "https" else 80),
                type=socket.SOCK_STREAM,
            )
        except socket.gaierror as exc:
            raise UnsafeWebsiteUrl(f"website hostname did not resolve: {hostname}") from exc
        addresses = list({ipaddress.ip_address(record[4][0]) for record in records})
    if not addresses or any(not address.is_global for address in addresses):
        raise UnsafeWebsiteUrl("website URL resolves to a non-public address")
