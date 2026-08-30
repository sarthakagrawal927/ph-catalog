from __future__ import annotations

import gzip
import zlib
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import unquote, urlsplit

import httpx
import orjson

from ph_catalog.archive import (
    COLLINFO_URL,
    COMMON_CRAWL_DATA_URL,
    PRODUCTHUNT_SURT_START,
    PRODUCTHUNT_SURT_STOP,
    CommonCrawlDirectImporter,
    DirectArchiveImportConfig,
    _lines,
    archive_slug,
)
from ph_catalog.archive_recovery import (
    ArchivePostRecovery,
    ArchiveRecovery,
    WaybackPostRecovery,
    WaybackRecovery,
)
from ph_catalog.database import CatalogDatabase
from ph_catalog.network import GlobalRateLimiter
from ph_catalog.parsing import content_hash, parse_product_page


@dataclass(slots=True, frozen=True)
class CommonCrawlCapture:
    slug: str
    crawl_id: str
    capture_timestamp: str
    url: str
    filename: str
    byte_offset: int
    byte_length: int
    mime: str | None


@dataclass(slots=True, frozen=True)
class CommonCrawlRecoveryConfig:
    crawls: tuple[str, ...] | None = None
    requests_per_second: float = 2.0
    timeout: float = 120.0
    max_attempts: int = 3
    scan_only: bool = False
    limit: int | None = None

    def __post_init__(self) -> None:
        if self.crawls is not None and (
            not self.crawls or any(not item.startswith("CC-MAIN-") for item in self.crawls)
        ):
            raise ValueError("invalid Common Crawl recovery collection")
        if self.requests_per_second <= 0 or self.timeout <= 0 or self.max_attempts < 1:
            raise ValueError("invalid Common Crawl recovery network configuration")
        if self.limit is not None and self.limit < 1:
            raise ValueError("Common Crawl recovery limit must be positive")


class CommonCrawlRecovery:
    """Recover unavailable products from exact Common Crawl WARC records.

    Only byte-range records selected from the public CDX are downloaded. Parsed HTML
    is held in memory and discarded immediately.
    """

    def __init__(
        self,
        database: CatalogDatabase,
        config: CommonCrawlRecoveryConfig,
        *,
        progress: Callable[[dict[str, object]], None] | None = None,
    ) -> None:
        self.database = database
        self.config = config
        self.progress = progress
        self.rate = GlobalRateLimiter(config.requests_per_second, 0.05, 0.2)

    async def run(self) -> dict[str, object]:
        headers = {"User-Agent": "ph-catalog/0.1 (local public-catalogue research)"}
        timeout = httpx.Timeout(self.config.timeout)
        scans = cdx_rows = transferred = processed = additions = 0
        outcomes: Counter[str] = Counter()
        async with httpx.AsyncClient(
            headers=headers,
            timeout=timeout,
            follow_redirects=True,
        ) as client:
            crawls = await self._crawls(client)
            direct = CommonCrawlDirectImporter(
                self.database,
                DirectArchiveImportConfig(
                    crawls=tuple(crawls),
                    timeout=self.config.timeout,
                    max_attempts=self.config.max_attempts,
                ),
            )
            targets = self._unavailable_slugs()
            for crawl_id in crawls:
                if self.database.common_crawl_recovery_scan_complete(crawl_id):
                    continue
                captures, crawl_rows, crawl_bytes = await self._scan_crawl(
                    client, direct, crawl_id, targets
                )
                self.database.apply_common_crawl_recovery_scan(
                    crawl_id, captures, crawl_rows, crawl_bytes
                )
                scans += 1
                cdx_rows += crawl_rows
                transferred += crawl_bytes
                self._emit(
                    "common_crawl_recovery_scan",
                    crawl_id=crawl_id,
                    scans_this_run=scans,
                    cdx_rows=crawl_rows,
                    matched_slugs=len(captures),
                    transferred_bytes=transferred,
                )

            if not self.config.scan_only:
                while self.config.limit is None or processed < self.config.limit:
                    remaining = 100
                    if self.config.limit is not None:
                        remaining = min(remaining, self.config.limit - processed)
                    pending = self.database.pending_common_crawl_captures(
                        remaining, self.config.max_attempts
                    )
                    if not pending:
                        break
                    for row in pending:
                        capture = CommonCrawlCapture(*row[:8])
                        attempt = row[8]
                        recovery = await self._recover_capture(client, capture, attempt)
                        if recovery.outcome == "recovered":
                            before = self.database.counts()["fetched"]
                            self.database.apply_archive_recoveries([recovery])
                            additions += self.database.counts()["fetched"] - before
                        self.database.apply_common_crawl_capture_outcome(
                            capture.slug,
                            capture.crawl_id,
                            recovery.outcome,
                            attempt,
                            recovery.http_status,
                            recovery.error,
                        )
                        processed += 1
                        outcomes[recovery.outcome] += 1
                        self._emit(
                            "common_crawl_recovery_progress",
                            processed=processed,
                            additions=additions,
                            outcomes=dict(outcomes),
                            catalogue=self.database.counts(),
                        )
                        if self.config.limit is not None and processed >= self.config.limit:
                            break

        return {
            "scans_this_run": scans,
            "cdx_rows_this_run": cdx_rows,
            "transferred_bytes_this_run": transferred,
            "processed": processed,
            "additions": additions,
            "outcomes": dict(outcomes),
            "catalogue": self.database.counts(),
            "recovery": self.database.common_crawl_recovery_counts(),
        }

    async def _crawls(self, client: httpx.AsyncClient) -> list[str]:
        if self.config.crawls is not None:
            return list(self.config.crawls)
        response = await client.get(COLLINFO_URL)
        response.raise_for_status()
        return [str(item["id"]) for item in response.json()]

    def _unavailable_slugs(self) -> set[str]:
        return {
            str(row[0])
            for row in self.database.connection.execute(
                "SELECT slug FROM products WHERE status = 'unavailable'"
            ).fetchall()
        }

    async def _scan_crawl(
        self,
        client: httpx.AsyncClient,
        direct: CommonCrawlDirectImporter,
        crawl_id: str,
        targets: set[str],
        path_kind: str = "products",
    ) -> tuple[list[CommonCrawlCapture], int, int]:
        base = f"{COMMON_CRAWL_DATA_URL}/cc-index/collections/{crawl_id}/indexes"
        cluster_url = f"{base}/cluster.idx"
        size = await direct._content_length(client, cluster_url)
        lower, previous = await direct._lower_bound(
            client, cluster_url, size, PRODUCTHUNT_SURT_START
        )
        upper, _ = await direct._lower_bound(
            client, cluster_url, size, PRODUCTHUNT_SURT_STOP
        )
        start = previous.start if lower.key > PRODUCTHUNT_SURT_START else lower.start
        raw_entries = await direct._get_range(
            client, cluster_url, start, upper.start - 1
        )
        entries = [
            direct._parse_cluster_line(start + offset, line)
            for offset, line in _lines(raw_entries)
        ]
        entries = [entry for entry in entries if entry.key < PRODUCTHUNT_SURT_STOP]
        captures: dict[str, CommonCrawlCapture] = {}
        cdx_rows = 0
        transferred = len(raw_entries)
        for entry in entries:
            compressed = await direct._get_range(
                client,
                f"{base}/{entry.filename}",
                entry.offset,
                entry.offset + entry.length - 1,
            )
            transferred += len(compressed)
            block = gzip.decompress(compressed)
            parsed, block_rows = self._parse_cdx_block(
                block, crawl_id, targets, path_kind
            )
            cdx_rows += block_rows
            for capture in parsed:
                previous_capture = captures.get(capture.slug)
                if previous_capture is None or self._capture_rank(capture) > self._capture_rank(
                    previous_capture
                ):
                    captures[capture.slug] = capture
        return list(captures.values()), cdx_rows, transferred

    @staticmethod
    def _capture_rank(capture: CommonCrawlCapture) -> tuple[str, bool, int]:
        parsed = urlsplit(capture.url)
        return capture.capture_timestamp, not parsed.query, capture.byte_length

    @staticmethod
    def _parse_cdx_block(
        block: bytes,
        crawl_id: str,
        targets: set[str],
        path_kind: str = "products",
    ) -> tuple[list[CommonCrawlCapture], int]:
        captures: list[CommonCrawlCapture] = []
        rows = 0
        for line in block.splitlines():
            try:
                _key, timestamp_bytes, payload = line.split(b" ", 2)
                record = orjson.loads(payload)
                rows += 1
                if str(record.get("status")) != "200":
                    continue
                url = record.get("url")
                if not isinstance(url, str):
                    continue
                slug = archive_slug(url, path_kind)
                if slug not in targets or not CommonCrawlRecovery._is_entity_page(
                    url, path_kind
                ):
                    continue
                filename = record.get("filename")
                byte_offset = int(record.get("offset"))
                byte_length = int(record.get("length"))
                if not isinstance(filename, str) or byte_offset < 0 or byte_length < 1:
                    continue
                mime = record.get("mime")
                captures.append(
                    CommonCrawlCapture(
                        slug=slug,
                        crawl_id=crawl_id,
                        capture_timestamp=timestamp_bytes.decode("ascii"),
                        url=url,
                        filename=filename,
                        byte_offset=byte_offset,
                        byte_length=byte_length,
                        mime=str(mime) if mime is not None else None,
                    )
                )
            except (TypeError, ValueError, UnicodeDecodeError, orjson.JSONDecodeError):
                continue
        return captures, rows

    @staticmethod
    def _is_entity_page(url: str, path_kind: str) -> bool:
        parts = [unquote(item).strip() for item in urlsplit(url).path.split("/") if item]
        return len(parts) == 2 and parts[0] == path_kind

    async def _recover_capture(
        self,
        client: httpx.AsyncClient,
        capture: CommonCrawlCapture,
        attempt: int,
    ) -> ArchiveRecovery:
        await self.rate.wait()
        url = f"{COMMON_CRAWL_DATA_URL}/{capture.filename}"
        try:
            response = await client.get(
                url,
                headers={
                    "Range": (
                        f"bytes={capture.byte_offset}-"
                        f"{capture.byte_offset + capture.byte_length - 1}"
                    )
                },
            )
            response.raise_for_status()
            if response.status_code != 206:
                raise ValueError("Common Crawl ignored WARC byte range")
            body = self._extract_http_body(response.content)
            canonical_url = f"https://www.producthunt.com/products/{capture.slug}"
            data = parse_product_page(body, capture.slug, canonical_url)
            if not WaybackRecovery._has_all_fields(data):
                return ArchiveRecovery(
                    capture.slug,
                    attempt,
                    "parse_failed",
                    capture.capture_timestamp,
                    200,
                    "Common Crawl page is missing one or more catalogue fields",
                )
            return ArchiveRecovery(
                capture.slug,
                attempt,
                "recovered",
                capture.capture_timestamp,
                200,
                data=data,
                content_hash=content_hash(data),
            )
        except (httpx.HTTPError, OSError, RuntimeError, ValueError) as exc:
            return ArchiveRecovery(
                capture.slug,
                attempt,
                "fetch_failed",
                capture.capture_timestamp,
                error=f"{type(exc).__name__}: {exc}",
            )

    @staticmethod
    def _extract_http_body(record: bytes) -> bytes:
        raw = gzip.decompress(record)
        _warc_headers, http_record = raw.split(b"\r\n\r\n", 1)
        http_headers, body = http_record.split(b"\r\n\r\n", 1)
        headers: dict[bytes, bytes] = {}
        for line in http_headers.split(b"\r\n")[1:]:
            if b":" not in line:
                continue
            key, value = line.split(b":", 1)
            headers[key.strip().lower()] = value.strip().lower()
        if headers.get(b"transfer-encoding") == b"chunked":
            body = CommonCrawlRecovery._decode_chunked(body)
        encoding = headers.get(b"content-encoding")
        if encoding == b"gzip":
            body = gzip.decompress(body)
        elif encoding == b"deflate":
            body = zlib.decompress(body)
        elif encoding not in {None, b"identity"}:
            raise ValueError(f"unsupported archived content encoding: {encoding!r}")
        return body

    @staticmethod
    def _decode_chunked(body: bytes) -> bytes:
        decoded = bytearray()
        remaining = body
        while remaining:
            size_line, separator, remaining = remaining.partition(b"\r\n")
            if not separator:
                raise ValueError("malformed chunked WARC response")
            size = int(size_line.split(b";", 1)[0], 16)
            if size == 0:
                break
            if len(remaining) < size + 2:
                raise ValueError("truncated chunked WARC response")
            decoded.extend(remaining[:size])
            remaining = remaining[size + 2 :]
        return bytes(decoded)

    def _emit(self, event: str, **fields: object) -> None:
        if self.progress:
            self.progress({"event": event, **fields})


class CommonCrawlPostRecovery(CommonCrawlRecovery):
    """Recover canonical products from exact archived `/posts/{slug}` pages."""

    async def run(self) -> dict[str, object]:
        headers = {"User-Agent": "ph-catalog/0.1 (local public-catalogue research)"}
        timeout = httpx.Timeout(self.config.timeout)
        scans = cdx_rows = transferred = processed = additions = 0
        outcomes: Counter[str] = Counter()
        async with httpx.AsyncClient(
            headers=headers,
            timeout=timeout,
            follow_redirects=True,
        ) as client:
            crawls = await self._crawls(client)
            direct = CommonCrawlDirectImporter(
                self.database,
                DirectArchiveImportConfig(
                    crawls=tuple(crawls),
                    timeout=self.config.timeout,
                    max_attempts=self.config.max_attempts,
                ),
            )
            targets = self._unavailable_post_slugs()
            for crawl_id in crawls:
                if self.database.common_crawl_post_recovery_scan_complete(crawl_id):
                    continue
                captures, crawl_rows, crawl_bytes = await self._scan_crawl(
                    client, direct, crawl_id, targets, "posts"
                )
                self.database.apply_common_crawl_post_recovery_scan(
                    crawl_id, captures, crawl_rows, crawl_bytes
                )
                scans += 1
                cdx_rows += crawl_rows
                transferred += crawl_bytes
                self._emit(
                    "common_crawl_post_recovery_scan",
                    crawl_id=crawl_id,
                    scans_this_run=scans,
                    cdx_rows=crawl_rows,
                    matched_slugs=len(captures),
                    transferred_bytes=transferred,
                )

            if not self.config.scan_only:
                while self.config.limit is None or processed < self.config.limit:
                    remaining = 100
                    if self.config.limit is not None:
                        remaining = min(remaining, self.config.limit - processed)
                    pending = self.database.pending_common_crawl_post_captures(
                        remaining, self.config.max_attempts
                    )
                    if not pending:
                        break
                    for row in pending:
                        capture = CommonCrawlCapture(*row[:8])
                        attempt = row[8]
                        recovery = await self._recover_post_capture(
                            client, capture, attempt
                        )
                        if recovery.outcome == "recovered":
                            additions += self.database.apply_archive_post_recoveries(
                                [recovery]
                            )
                        self.database.apply_common_crawl_post_capture_outcome(
                            capture.slug,
                            capture.crawl_id,
                            recovery.outcome,
                            attempt,
                            recovery.http_status,
                            recovery.error,
                        )
                        processed += 1
                        outcomes[recovery.outcome] += 1
                        self._emit(
                            "common_crawl_post_recovery_progress",
                            processed=processed,
                            additions=additions,
                            outcomes=dict(outcomes),
                            catalogue=self.database.counts(),
                        )
                        if self.config.limit is not None and processed >= self.config.limit:
                            break

        return {
            "scans_this_run": scans,
            "cdx_rows_this_run": cdx_rows,
            "transferred_bytes_this_run": transferred,
            "processed": processed,
            "additions": additions,
            "outcomes": dict(outcomes),
            "catalogue": self.database.counts(),
            "recovery": self.database.common_crawl_post_recovery_counts(),
        }

    def _unavailable_post_slugs(self) -> set[str]:
        return {
            str(row[0])
            for row in self.database.connection.execute(
                """
                SELECT candidate.slug
                FROM archive_candidates AS candidate
                JOIN archive_post_resolutions AS resolution
                  ON resolution.post_slug = candidate.slug
                WHERE candidate.path_kind = 'posts'
                  AND resolution.outcome = 'unavailable'
                  AND regexp_matches(candidate.slug, '^[a-z0-9][a-z0-9_-]*$')
                  AND NOT regexp_matches(candidate.slug, '^[0-9]+$')
                """
            ).fetchall()
        }

    async def _recover_post_capture(
        self,
        client: httpx.AsyncClient,
        capture: CommonCrawlCapture,
        attempt: int,
    ) -> ArchivePostRecovery:
        await self.rate.wait()
        url = f"{COMMON_CRAWL_DATA_URL}/{capture.filename}"
        try:
            response = await client.get(
                url,
                headers={
                    "Range": (
                        f"bytes={capture.byte_offset}-"
                        f"{capture.byte_offset + capture.byte_length - 1}"
                    )
                },
            )
            response.raise_for_status()
            if response.status_code != 206:
                raise ValueError("Common Crawl ignored WARC byte range")
            body = self._extract_http_body(response.content)
            product_slugs = WaybackPostRecovery._product_slugs(body)
            legacy_data = None
            if not product_slugs:
                legacy_data = WaybackPostRecovery._legacy_product_data(body, capture.slug)
            if len(product_slugs) != 1 and legacy_data is None:
                return ArchivePostRecovery(
                    capture.slug,
                    attempt,
                    "parse_failed",
                    capture.capture_timestamp,
                    200,
                    f"expected one canonical product link, found {len(product_slugs)}",
                )
            if legacy_data is not None:
                data = legacy_data
            else:
                product_slug = product_slugs[0]
                product_url = f"https://www.producthunt.com/products/{product_slug}"
                data = parse_product_page(body, product_slug, product_url)
            if not WaybackRecovery._has_all_fields(data):
                return ArchivePostRecovery(
                    capture.slug,
                    attempt,
                    "parse_failed",
                    capture.capture_timestamp,
                    200,
                    "Common Crawl launch is missing one or more catalogue fields",
                )
            return ArchivePostRecovery(
                capture.slug,
                attempt,
                "recovered",
                capture.capture_timestamp,
                200,
                data=data,
                content_hash=content_hash(data),
            )
        except (httpx.HTTPError, OSError, RuntimeError, ValueError) as exc:
            return ArchivePostRecovery(
                capture.slug,
                attempt,
                "fetch_failed",
                capture.capture_timestamp,
                error=f"{type(exc).__name__}: {exc}",
            )
