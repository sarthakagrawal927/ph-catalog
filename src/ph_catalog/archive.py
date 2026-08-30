from __future__ import annotations

import asyncio
import gzip
import re
import string
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import quote, unquote, urlsplit

import httpx
import orjson

from ph_catalog.database import CatalogDatabase

COLLINFO_URL = "https://index.commoncrawl.org/collinfo.json"
ARCHIVE_PATTERNS = {
    "products": "www.producthunt.com/products/*",
    "posts": "www.producthunt.com/posts/*",
}
ARCHIVE_PREFIXES = string.ascii_lowercase + string.digits
WAYBACK_CDX_URL = "https://web.archive.org/cdx/search/cdx"
ARQUIVO_PT_CDX_URL = "https://arquivo.pt/wayback/cdx"
ARQUIVO_PT_SCAN_NAME = "arquivo-pt-cdx-v1"
INDEPENDENT_CDX_ARCHIVES = {
    "fdlpwa": "https://wayback.archive-it.org/org-593/timemap/cdx?url={url}",
    "iwa": "https://vefsafn.is/cdx?url={url}",
    "euwa": "https://wayback.archive-it.org/12090/timemap/cdx?url={url}",
    "gcwa": (
        "https://webarchiveweb.wayback.bac-lac.canada.ca/web/timemap/cdx?url={url}"
    ),
    "culwa": "https://wayback.archive-it.org/org-304/timemap/cdx?url={url}",
    "nliwa": "https://wayback.archive-it.org/org-1444/timemap/cdx?url={url}",
    "nisv": "https://wayback.archive-it.org/org-2423/timemap/cdx?url={url}",
}
COMMON_CRAWL_DATA_URL = "https://data.commoncrawl.org"
INTERNET_ARCHIVE_DATA_URL = "https://archive.org/download"
INTERNET_ARCHIVE_PRODUCTHUNT_ITEMS = (
    "warc_www_producthunt_com_20160204",
    "warc_www_producthunt_com_20160204_part_2",
    "warc_www_producthunt_com_20160407",
    "warc_www_producthunt_com_20160828",
    "warc_www_producthunt_com_20170521",
    "warc_www_producthunt_com_20170626",
    "warc_www_producthunt_com_20171211",
    "warc_www_producthunt_com_20171211_part_2",
    "warc_www_producthunt_com_20180608",
    "warc_www_producthunt_com_20181207",
    "warc_www_producthunt_com_20181207_part_2",
    "warc_www_producthunt_com_20190607",
    "warc_www_producthunt_com_20191210",
    "warc_www_producthunt_com_20200102",
)
INTERNET_ARCHIVE_LEGACY_ROUTES = frozenset(
    {"apps", "books", "games", "podcasts", "posts", "products", "tech"}
)
PRODUCTHUNT_SURT_START = "com,producthunt"
PRODUCTHUNT_SURT_STOP = "com,producthunu"
CLUSTER_SEARCH_PAD = 64 * 1024
MAX_CLUSTER_BLOCKS = 100
MAX_INTERNET_ARCHIVE_RANGE = 128 * 1024 * 1024
ARCHIVE_ASSET_SUFFIXES = (
    ".css",
    ".gif",
    ".ico",
    ".jpeg",
    ".jpg",
    ".js",
    ".json",
    ".map",
    ".png",
    ".svg",
    ".txt",
    ".tsx",
    ".webp",
    ".woff",
    ".xml",
)


@dataclass(slots=True, frozen=True)
class ArchiveImportConfig:
    requests_per_second: float = 1.0
    timeout: float = 120.0
    max_attempts: int = 5
    limit_indexes: int | None = None


@dataclass(slots=True, frozen=True)
class DirectArchiveImportConfig:
    crawls: tuple[str, ...]
    timeout: float = 60.0
    max_attempts: int = 5
    include_non_200: bool = False

    def __post_init__(self) -> None:
        if not self.crawls:
            raise ValueError("at least one Common Crawl ID is required")
        if self.timeout <= 0 or self.max_attempts < 1:
            raise ValueError("invalid direct archive retry configuration")


@dataclass(slots=True, frozen=True)
class InternetArchiveImportConfig:
    items: tuple[str, ...] = INTERNET_ARCHIVE_PRODUCTHUNT_ITEMS
    workers: int = 3
    timeout: float = 300.0
    max_attempts: int = 5
    include_non_200: bool = False

    def __post_init__(self) -> None:
        if not self.items:
            raise ValueError("at least one Internet Archive item is required")
        if not 1 <= self.workers <= 8:
            raise ValueError("Internet Archive workers must be between 1 and 8")
        if self.timeout <= 0 or self.max_attempts < 1:
            raise ValueError("invalid Internet Archive retry configuration")
        if any(not re.fullmatch(r"[A-Za-z0-9_.-]+", item) for item in self.items):
            raise ValueError("invalid Internet Archive item identifier")


@dataclass(slots=True, frozen=True)
class ArquivoPtImportConfig:
    scan_name: str = ARQUIVO_PT_SCAN_NAME
    path_kinds: tuple[str, ...] = ("products", "posts")
    limit: int = 100_000
    timeout: float = 300.0
    max_attempts: int = 5
    include_non_200: bool = False

    def __post_init__(self) -> None:
        if not self.scan_name.strip():
            raise ValueError("Arquivo.pt scan name must not be empty")
        if not self.path_kinds or any(kind not in ARCHIVE_PATTERNS for kind in self.path_kinds):
            raise ValueError("unsupported Arquivo.pt path kind")
        if not 1 <= self.limit <= 100_000:
            raise ValueError("Arquivo.pt limit must be between 1 and 100,000")
        if self.timeout <= 0 or self.max_attempts < 1:
            raise ValueError("invalid Arquivo.pt retry configuration")


@dataclass(slots=True, frozen=True)
class IndependentCdxImportConfig:
    archives: tuple[str, ...] = tuple(INDEPENDENT_CDX_ARCHIVES)
    path_kinds: tuple[str, ...] = ("products", "posts")
    timeout: float = 120.0
    max_attempts: int = 3
    max_response_bytes: int = 50_000_000

    def __post_init__(self) -> None:
        if not self.archives or any(item not in INDEPENDENT_CDX_ARCHIVES for item in self.archives):
            raise ValueError("unsupported independent CDX archive")
        if not self.path_kinds or any(kind not in ARCHIVE_PATTERNS for kind in self.path_kinds):
            raise ValueError("unsupported independent CDX path kind")
        if self.timeout <= 0 or self.max_attempts < 1 or self.max_response_bytes < 1:
            raise ValueError("invalid independent CDX configuration")


@dataclass(slots=True, frozen=True)
class ClusterEntry:
    start: int
    end: int
    key: str
    filename: str
    offset: int
    length: int


@dataclass(slots=True, frozen=True)
class InternetArchiveChunk:
    key: str
    offset: int
    length: int


class ArquivoPtImporter:
    """Import independent Product Hunt URL captures from Portugal's web archive."""

    def __init__(
        self,
        database: CatalogDatabase,
        config: ArquivoPtImportConfig,
        *,
        progress: Callable[[dict[str, int | str]], None] | None = None,
    ) -> None:
        self.database = database
        self.config = config
        self.progress = progress

    async def run(self) -> dict[str, int]:
        scans = captures = transferred = 0
        headers = {"User-Agent": "ph-catalog/0.1 (local public-catalogue research)"}
        async with httpx.AsyncClient(
            headers=headers,
            timeout=httpx.Timeout(self.config.timeout),
            follow_redirects=True,
        ) as client:
            for kind in self.config.path_kinds:
                scan_id = self._scan_id()
                if self.database.archive_scan_complete(scan_id, kind):
                    continue
                body = await self._fetch(client, kind)
                candidates, kind_captures, malformed = self._parse_jsonl(body, kind)
                if kind_captures >= self.config.limit:
                    raise ValueError(
                        f"Arquivo.pt {kind} query reached its {self.config.limit}-row cap"
                    )
                self.database.apply_archive_scan(
                    scan_id,
                    kind,
                    candidates,
                    kind_captures,
                    malformed,
                )
                scans += 1
                captures += kind_captures
                transferred += len(body)
                if self.progress:
                    self.progress(
                        {
                            "event": "arquivo_pt_progress",
                            "path_kind": kind,
                            "captures": kind_captures,
                            "candidates": len(candidates),
                            "transferred_bytes": transferred,
                        }
                    )
        result = self.database.archive_counts()
        result["arquivo_pt_scans_this_run"] = scans
        result["arquivo_pt_captures_this_run"] = captures
        result["arquivo_pt_transferred_bytes_this_run"] = transferred
        return result

    async def _fetch(self, client: httpx.AsyncClient, kind: str) -> bytes:
        params: list[tuple[str, str]] = [
            ("url", f"www.producthunt.com/{kind}/*"),
            ("output", "json"),
            ("collapse", "urlkey"),
            ("fields", "url,timestamp,status,mime"),
            ("limit", str(self.config.limit)),
        ]
        if not self.config.include_non_200:
            params[2:2] = [
                ("filter", "=status:200"),
                ("filter", "=mime:text/html"),
            ]
        for attempt in range(self.config.max_attempts):
            try:
                response = await client.get(ARQUIVO_PT_CDX_URL, params=params)
                if response.status_code in {429, 500, 502, 503, 504}:
                    response.raise_for_status()
                response.raise_for_status()
                return response.content
            except httpx.HTTPError:
                if attempt + 1 == self.config.max_attempts:
                    raise
                await asyncio.sleep(min(60.0, 2**attempt))
        raise RuntimeError("unreachable Arquivo.pt retry state")

    def _scan_id(self) -> str:
        suffix = ":all-statuses" if self.config.include_non_200 else ""
        return f"{self.config.scan_name}{suffix}"

    @staticmethod
    def _parse_jsonl(body: bytes, kind: str) -> tuple[set[str], int, int]:
        candidates: set[str] = set()
        captures = malformed = 0
        for line in body.splitlines():
            try:
                row = orjson.loads(line)
            except orjson.JSONDecodeError:
                malformed += 1
                continue
            url = row.get("url") if isinstance(row, dict) else None
            if not isinstance(url, str):
                malformed += 1
                continue
            captures += 1
            if slug := archive_slug(url, kind):
                candidates.add(slug)
        return candidates, captures, malformed


class IndependentCdxImporter:
    """Import Product Hunt URL keys from independent public web archives."""

    _url_pattern = re.compile(
        rb"https?://(?:www\.)?producthunt\.com/[^\s\"}]+", re.IGNORECASE
    )

    def __init__(
        self,
        database: CatalogDatabase,
        config: IndependentCdxImportConfig,
        *,
        progress: Callable[[dict[str, int | str]], None] | None = None,
    ) -> None:
        self.database = database
        self.config = config
        self.progress = progress

    async def run(self) -> dict[str, int]:
        scans = captures = transferred = 0
        headers = {"User-Agent": "ph-catalog/0.1 (local public-catalogue research)"}
        async with httpx.AsyncClient(
            headers=headers,
            timeout=httpx.Timeout(self.config.timeout),
            follow_redirects=True,
        ) as client:
            for archive_id in self.config.archives:
                for kind in self.config.path_kinds:
                    scan_id = f"independent-cdx:{archive_id}"
                    if self.database.archive_scan_complete(scan_id, kind):
                        continue
                    body = await self._fetch(client, archive_id, kind)
                    candidates, kind_captures = self._parse_cdx(body, kind)
                    self.database.apply_archive_scan(
                        scan_id, kind, candidates, kind_captures, 0
                    )
                    scans += 1
                    captures += kind_captures
                    transferred += len(body)
                    if self.progress:
                        self.progress(
                            {
                                "event": "independent_cdx_progress",
                                "archive": archive_id,
                                "path_kind": kind,
                                "captures": kind_captures,
                                "candidates": len(candidates),
                                "transferred_bytes": transferred,
                            }
                        )
                    await asyncio.sleep(0.5)
        result = self.database.archive_counts()
        result["independent_cdx_scans_this_run"] = scans
        result["independent_cdx_captures_this_run"] = captures
        result["independent_cdx_transferred_bytes_this_run"] = transferred
        return result

    async def _fetch(
        self, client: httpx.AsyncClient, archive_id: str, kind: str
    ) -> bytes:
        target = f"https://www.producthunt.com/{kind}/*"
        url = INDEPENDENT_CDX_ARCHIVES[archive_id].replace(
            "{url}", quote(target, safe="*")
        )
        for attempt in range(self.config.max_attempts):
            try:
                async with client.stream("GET", url) as response:
                    response.raise_for_status()
                    chunks: list[bytes] = []
                    size = 0
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size > self.config.max_response_bytes:
                            raise ValueError(
                                f"independent CDX response exceeded cap: {archive_id}"
                            )
                        chunks.append(chunk)
                body = b"".join(chunks)
                content_type = response.headers.get("Content-Type", "").lower()
                if body and "text/html" in content_type:
                    raise ValueError(
                        f"independent CDX returned HTML instead of an index: {archive_id}"
                    )
                return body
            except httpx.HTTPError:
                if attempt + 1 == self.config.max_attempts:
                    raise
                await asyncio.sleep(min(30.0, 2**attempt))
        raise RuntimeError("unreachable independent CDX retry state")

    @classmethod
    def _parse_cdx(cls, body: bytes, kind: str) -> tuple[set[str], int]:
        urls = {
            match.group().decode("utf-8", "replace")
            for match in cls._url_pattern.finditer(body)
        }
        candidates = {slug for url in urls if (slug := archive_slug(url, kind))}
        return candidates, len(urls)


def archive_slug(url: str, kind: str) -> str | None:
    """Extract the first path segment after /products/ or legacy /posts/."""
    if kind not in ARCHIVE_PATTERNS:
        raise ValueError(f"unsupported archive path kind: {kind}")
    parsed = urlsplit(url)
    if (parsed.hostname or "").lower() not in {"producthunt.com", "www.producthunt.com"}:
        return None
    parts = [unquote(part).strip() for part in parsed.path.split("/") if part]
    if len(parts) < 2 or parts[0] != kind:
        return None
    slug = parts[1].lower()
    if (
        not slug
        or slug == "..."
        or len(slug) > 300
        or any(char.isspace() for char in slug)
        or slug.endswith(ARCHIVE_ASSET_SUFFIXES)
    ):
        return None
    return slug


def internet_archive_legacy_slug(url: str) -> tuple[str, str] | None:
    """Extract a launch slug from Product Hunt's current and retired route families."""
    parsed = urlsplit(url)
    if (parsed.hostname or "").lower() not in {"producthunt.com", "www.producthunt.com"}:
        return None
    parts = [unquote(part).strip().lower() for part in parsed.path.split("/") if part]
    if len(parts) < 2 or parts[0] not in INTERNET_ARCHIVE_LEGACY_ROUTES:
        return None
    route, slug = parts[:2]
    if (
        not re.fullmatch(r"[a-z0-9][a-z0-9-]*", slug)
        or len(slug) > 300
        or slug.endswith(ARCHIVE_ASSET_SUFFIXES)
    ):
        return None
    return route, slug


class InternetArchiveDirectImporter:
    """Import legacy launch slugs from ArchiveTeam CDX byte ranges.

    The archived crawls are several terabytes in total. Their aggregate CDX files are
    block-gzipped and accompanied by a small byte-offset index, so only the range around
    the Product Hunt SURT keys needs to be transferred. All retired vertical routes map
    to the legacy post/launch entity resolved by Product Hunt's public structured data.
    """

    def __init__(
        self,
        database: CatalogDatabase,
        config: InternetArchiveImportConfig,
        *,
        progress: Callable[[dict[str, int | str]], None] | None = None,
    ) -> None:
        self.database = database
        self.config = config
        self.progress = progress

    async def run(self) -> dict[str, int]:
        pending = [
            item
            for item in self.config.items
            if not self.database.archive_scan_complete(self._scan_id(item), "posts")
        ]
        semaphore = asyncio.Semaphore(self.config.workers)
        timeout = httpx.Timeout(self.config.timeout)
        headers = {"User-Agent": "ph-catalog/0.1 (local public-catalogue research)"}
        scans = 0
        captures = 0
        transferred = 0
        async with httpx.AsyncClient(
            timeout=timeout, headers=headers, follow_redirects=True
        ) as client:

            async def fetch(item: str) -> tuple[str, set[str], int, int, int]:
                async with semaphore:
                    return await self._fetch_item(client, item)

            tasks = [asyncio.create_task(fetch(item)) for item in pending]
            for task in asyncio.as_completed(tasks):
                item, candidates, item_captures, malformed, item_bytes = await task
                self.database.apply_archive_scan(
                    self._scan_id(item),
                    "posts",
                    candidates,
                    item_captures,
                    malformed,
                )
                scans += 1
                captures += item_captures
                transferred += item_bytes
                if self.progress:
                    counts = self.database.archive_counts()
                    self.progress(
                        {
                            "event": "internet_archive_progress",
                            "item": item,
                            "scans": scans,
                            "captures": captures,
                            "transferred_bytes": transferred,
                            "posts": counts["posts"],
                            "unique_candidates": counts["unique_candidates"],
                        }
                    )
        result = self.database.archive_counts()
        result["scans_this_run"] = scans
        result["captures_this_run"] = captures
        result["transferred_bytes_this_run"] = transferred
        return result

    async def _fetch_item(
        self, client: httpx.AsyncClient, item: str
    ) -> tuple[str, set[str], int, int, int]:
        base = f"{INTERNET_ARCHIVE_DATA_URL}/{item}"
        index = await self._request(client, f"{base}/{item}.cdx.idx")
        chunks = self._parse_item_index(index.content)
        hits = [
            position
            for position, chunk in enumerate(chunks)
            if chunk.key.startswith(PRODUCTHUNT_SURT_START)
        ]
        if not hits:
            raise ValueError(f"Product Hunt SURT range not found in {item}")
        first = max(0, min(hits) - 1)
        last = min(len(chunks) - 1, max(hits) + 1)
        start = chunks[first].offset
        end = chunks[last].offset + chunks[last].length - 1
        if end - start + 1 > MAX_INTERNET_ARCHIVE_RANGE:
            raise ValueError(f"unexpectedly large Product Hunt CDX range in {item}")
        response = await self._request(
            client,
            f"{base}/{item}.cdx.gz",
            headers={"Range": f"bytes={start}-{end}"},
        )
        if response.status_code != 206:
            raise ValueError(f"Internet Archive ignored byte range for {item}")
        try:
            block = gzip.decompress(response.content)
        except gzip.BadGzipFile as exc:
            raise ValueError(f"invalid Internet Archive CDX range in {item}") from exc
        candidates, captures, malformed = self._parse_item_cdx(
            block, include_non_200=self.config.include_non_200
        )
        return item, candidates, captures, malformed, len(response.content)

    def _scan_id(self, item: str) -> str:
        suffix = ":all-statuses" if self.config.include_non_200 else ""
        return f"internet-archive:{item}{suffix}"

    async def _request(
        self,
        client: httpx.AsyncClient,
        url: str,
        *,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        for attempt in range(self.config.max_attempts):
            try:
                response = await client.get(url, headers=headers)
                if response.status_code in {429, 500, 502, 503, 504}:
                    response.raise_for_status()
                response.raise_for_status()
                return response
            except httpx.HTTPError:
                if attempt + 1 == self.config.max_attempts:
                    raise
                await asyncio.sleep(min(60.0, 2**attempt))
        raise RuntimeError("unreachable Internet Archive retry state")

    @staticmethod
    def _parse_item_index(body: bytes) -> list[InternetArchiveChunk]:
        chunks: list[InternetArchiveChunk] = []
        for line in body.splitlines():
            fields = line.split(b"\t")
            if len(fields) < 4:
                continue
            try:
                chunks.append(
                    InternetArchiveChunk(
                        key=fields[0].split(b" ", 1)[0].decode("utf-8", "replace"),
                        offset=int(fields[2]),
                        length=int(fields[3]),
                    )
                )
            except ValueError:
                continue
        if not chunks:
            raise ValueError("Internet Archive item index is empty or malformed")
        return chunks

    @staticmethod
    def _parse_item_cdx(
        block: bytes, *, include_non_200: bool = False
    ) -> tuple[set[str], int, int]:
        candidates: set[str] = set()
        captures = 0
        malformed = 0
        for line in block.splitlines():
            fields = line.split(b" ")
            if len(fields) < 11:
                malformed += 1
                continue
            try:
                url = fields[2].decode("utf-8", "replace")
                status = fields[4].decode("ascii")
            except UnicodeDecodeError:
                malformed += 1
                continue
            parsed = internet_archive_legacy_slug(url)
            if (status != "200" and not include_non_200) or parsed is None:
                continue
            _route, slug = parsed
            candidates.add(slug)
            captures += 1
        return candidates, captures, malformed


@dataclass(slots=True, frozen=True)
class WaybackImportConfig:
    batch_size: int = 10_000
    timeout: float = 120.0
    max_attempts: int = 5
    path_kinds: tuple[str, ...] = ("products", "posts")
    limit_batches: int | None = None


class WaybackImporter:
    def __init__(
        self,
        database: CatalogDatabase,
        config: WaybackImportConfig,
        *,
        progress: Callable[[dict[str, int | str]], None] | None = None,
    ) -> None:
        self.database = database
        self.config = config
        self.progress = progress

    async def run(self) -> dict[str, int]:
        batches = 0
        captures = 0
        timeout = httpx.Timeout(self.config.timeout)
        headers = {"User-Agent": "ph-catalog/0.1 (local public-catalogue research)"}
        async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
            for kind in self.config.path_kinds:
                if kind not in ARCHIVE_PATTERNS:
                    raise ValueError(f"unsupported Wayback path kind: {kind}")
                while True:
                    state = self.database.wayback_scan_state(kind)
                    if bool(state["complete"]):
                        break
                    if (
                        self.config.limit_batches is not None
                        and batches >= self.config.limit_batches
                    ):
                        return self._result(batches, captures)
                    urls, next_key = await self._fetch_batch(
                        client, kind, str(state["resume_key"] or "")
                    )
                    candidates = {slug for url in urls if (slug := archive_slug(url, kind))}
                    self.database.apply_wayback_batch(kind, candidates, len(urls), next_key)
                    batches += 1
                    captures += len(urls)
                    if self.progress:
                        self.progress(
                            {
                                "event": "wayback_progress",
                                "path_kind": kind,
                                "batches": batches,
                                "captures": captures,
                                "unique_candidates": self.database.archive_counts()[
                                    "unique_candidates"
                                ],
                            }
                        )
                    if next_key is None:
                        break
        return self._result(batches, captures)

    def _result(self, batches: int, captures: int) -> dict[str, int]:
        result = self.database.archive_counts()
        result["wayback_batches_this_run"] = batches
        result["wayback_captures_this_run"] = captures
        return result

    async def _fetch_batch(
        self, client: httpx.AsyncClient, kind: str, resume_key: str
    ) -> tuple[list[str], str | None]:
        regex = rf"original:^https://www\.producthunt\.com/{kind}/[^/?#]+$"
        params: list[tuple[str, str]] = [
            ("url", f"www.producthunt.com/{kind}/*"),
            ("output", "txt"),
            ("filter", "statuscode:200"),
            ("filter", regex),
            ("collapse", "urlkey"),
            ("fl", "original"),
            ("limit", str(self.config.batch_size)),
            ("showResumeKey", "true"),
        ]
        if resume_key:
            params.append(("resumeKey", resume_key))
        for attempt in range(self.config.max_attempts):
            try:
                response = await client.get(WAYBACK_CDX_URL, params=params)
                response.raise_for_status()
                body = response.text.rstrip("\n")
                urls_text, separator, possible_key = body.rpartition("\n\n")
                if separator and possible_key and "://" not in possible_key:
                    return urls_text.splitlines(), possible_key
                return body.splitlines(), None
            except httpx.HTTPError:
                if attempt + 1 == self.config.max_attempts:
                    raise
                await asyncio.sleep(min(60.0, 2**attempt))
        raise RuntimeError("unreachable Wayback retry state")


class CommonCrawlDirectImporter:
    """Read a domain's CDX blocks directly, without the rate-limited index API."""

    def __init__(
        self,
        database: CatalogDatabase,
        config: DirectArchiveImportConfig,
        *,
        progress: Callable[[dict[str, int | str]], None] | None = None,
    ) -> None:
        self.database = database
        self.config = config
        self.progress = progress

    async def run(self) -> dict[str, int]:
        scans = 0
        captures = 0
        timeout = httpx.Timeout(self.config.timeout)
        headers = {"User-Agent": "ph-catalog/0.1 (local public-catalogue research)"}
        async with httpx.AsyncClient(
            timeout=timeout, headers=headers, follow_redirects=True
        ) as client:
            for crawl in self.config.crawls:
                if not crawl.startswith("CC-MAIN-"):
                    raise ValueError(f"invalid Common Crawl ID: {crawl}")
                scan_id = (
                    f"{crawl}:all-statuses" if self.config.include_non_200 else crawl
                )
                needed = [
                    kind
                    for kind in ARCHIVE_PATTERNS
                    if not self.database.archive_scan_complete(scan_id, kind)
                ]
                if not needed:
                    continue
                urls, malformed, block_count = await self._fetch_crawl(client, crawl)
                for kind in needed:
                    kind_urls = [url for url in urls if archive_slug(url, kind) is not None]
                    candidates = {
                        slug for url in kind_urls if (slug := archive_slug(url, kind))
                    }
                    self.database.apply_archive_scan(
                        scan_id, kind, candidates, len(kind_urls), malformed
                    )
                    scans += 1
                    captures += len(kind_urls)
                if self.progress:
                    counts = self.database.archive_counts()
                    self.progress(
                        {
                            "event": "direct_archive_progress",
                            "crawl": crawl,
                            "blocks": block_count,
                            "captures": len(urls),
                            "products": counts["products"],
                            "posts": counts["posts"],
                            "unique_candidates": counts["unique_candidates"],
                        }
                    )
        result = self.database.archive_counts()
        result["scans_this_run"] = scans
        result["captures_this_run"] = captures
        return result

    async def _fetch_crawl(
        self, client: httpx.AsyncClient, crawl: str
    ) -> tuple[set[str], int, int]:
        base = f"{COMMON_CRAWL_DATA_URL}/cc-index/collections/{crawl}/indexes"
        cluster_url = f"{base}/cluster.idx"
        size = await self._content_length(client, cluster_url)
        lower, previous = await self._lower_bound(
            client, cluster_url, size, PRODUCTHUNT_SURT_START
        )
        upper, _ = await self._lower_bound(
            client, cluster_url, size, PRODUCTHUNT_SURT_STOP
        )
        start = previous.start if lower.key > PRODUCTHUNT_SURT_START else lower.start
        raw_entries = await self._get_range(client, cluster_url, start, upper.start - 1)
        entries = [
            self._parse_cluster_line(start + offset, line)
            for offset, line in _lines(raw_entries)
        ]
        entries = [entry for entry in entries if entry.key < PRODUCTHUNT_SURT_STOP]
        if not entries or len(entries) > MAX_CLUSTER_BLOCKS:
            raise ValueError(
                f"unexpected Common Crawl block count for {crawl}: {len(entries)}"
            )

        urls: set[str] = set()
        malformed = 0
        for entry in entries:
            block_url = f"{base}/{entry.filename}"
            compressed = await self._get_range(
                client,
                block_url,
                entry.offset,
                entry.offset + entry.length - 1,
            )
            try:
                block = gzip.decompress(compressed)
            except gzip.BadGzipFile as exc:
                raise ValueError(f"invalid Common Crawl block: {block_url}") from exc
            block_urls, block_malformed = self._parse_cdx_block(
                block, include_non_200=self.config.include_non_200
            )
            urls.update(block_urls)
            malformed += block_malformed
        return urls, malformed, len(entries)

    async def _content_length(self, client: httpx.AsyncClient, url: str) -> int:
        response = await self._request(client, "HEAD", url)
        value = response.headers.get("Content-Length")
        if value is None or not value.isdigit():
            raise ValueError(f"missing Common Crawl content length: {url}")
        return int(value)

    async def _lower_bound(
        self,
        client: httpx.AsyncClient,
        url: str,
        size: int,
        target: str,
    ) -> tuple[ClusterEntry, ClusterEntry]:
        low = 0
        high = size - 1
        while high - low >= 4096:
            start, end, line = await self._line_at(client, url, size, (low + high) // 2)
            if self._cluster_key(line) < target:
                low = end + 1
            else:
                high = start

        window_start = max(0, low - 2 * CLUSTER_SEARCH_PAD)
        window_end = min(size - 1, high + 2 * CLUSTER_SEARCH_PAD)
        window = await self._get_range(client, url, window_start, window_end)
        parsed = [
            self._parse_cluster_line(window_start + offset, line)
            for offset, line in _lines(window, discard_first=window_start > 0)
        ]
        for index, entry in enumerate(parsed):
            if entry.key >= target:
                if index == 0:
                    raise ValueError("Common Crawl cluster predecessor is unavailable")
                return entry, parsed[index - 1]
        raise ValueError(f"Common Crawl cluster key not found: {target}")

    async def _line_at(
        self,
        client: httpx.AsyncClient,
        url: str,
        size: int,
        position: int,
    ) -> tuple[int, int, bytes]:
        start = max(0, position - CLUSTER_SEARCH_PAD)
        end = min(size - 1, position + CLUSTER_SEARCH_PAD)
        body = await self._get_range(client, url, start, end)
        relative = position - start
        line_start = body.rfind(b"\n", 0, relative) + 1
        line_end = body.find(b"\n", relative)
        if line_end < 0:
            raise ValueError("Common Crawl cluster line exceeds search window")
        return start + line_start, start + line_end, body[line_start:line_end]

    async def _get_range(
        self, client: httpx.AsyncClient, url: str, start: int, end: int
    ) -> bytes:
        if start < 0 or end < start:
            raise ValueError(f"invalid Common Crawl byte range: {start}-{end}")
        response = await self._request(
            client, "GET", url, headers={"Range": f"bytes={start}-{end}"}
        )
        if response.status_code != 206:
            raise ValueError(f"Common Crawl ignored byte range for {url}")
        return response.content

    async def _request(
        self,
        client: httpx.AsyncClient,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        for attempt in range(self.config.max_attempts):
            try:
                response = await client.request(method, url, headers=headers)
                response.raise_for_status()
                return response
            except httpx.HTTPError:
                if attempt + 1 == self.config.max_attempts:
                    raise
                await asyncio.sleep(min(60.0, 2**attempt))
        raise RuntimeError("unreachable direct archive retry state")

    @staticmethod
    def _cluster_key(line: bytes) -> str:
        return line.split(b"\t", 1)[0].split(b" ", 1)[0].decode("utf-8", "replace")

    @staticmethod
    def _parse_cluster_line(start: int, line: bytes) -> ClusterEntry:
        fields = line.split(b"\t")
        if len(fields) < 4:
            raise ValueError("malformed Common Crawl cluster line")
        return ClusterEntry(
            start=start,
            end=start + len(line),
            key=CommonCrawlDirectImporter._cluster_key(line),
            filename=fields[1].decode("ascii"),
            offset=int(fields[2]),
            length=int(fields[3]),
        )

    @staticmethod
    def _parse_cdx_block(
        block: bytes, *, include_non_200: bool = False
    ) -> tuple[set[str], int]:
        urls: set[str] = set()
        malformed = 0
        for line in block.splitlines():
            try:
                key, timestamp, payload = line.split(b" ", 2)
                del key, timestamp
                record = orjson.loads(payload)
                url = record.get("url")
                status = str(record.get("status"))
                if (status == "200" or include_non_200) and isinstance(url, str):
                    urls.add(url)
            except (ValueError, orjson.JSONDecodeError):
                malformed += 1
        return urls, malformed


def _lines(body: bytes, *, discard_first: bool = False) -> list[tuple[int, bytes]]:
    """Return byte offsets and non-empty complete lines from a range response."""
    result: list[tuple[int, bytes]] = []
    offset = 0
    for index, line in enumerate(body.splitlines(keepends=True)):
        content = line.rstrip(b"\r\n")
        complete = line.endswith((b"\n", b"\r"))
        if content and complete and not (discard_first and index == 0):
            result.append((offset, content))
        offset += len(line)
    return result


class CommonCrawlImporter:
    def __init__(
        self,
        database: CatalogDatabase,
        config: ArchiveImportConfig,
        *,
        progress: Callable[[dict[str, int | str]], None] | None = None,
    ) -> None:
        if config.requests_per_second <= 0:
            raise ValueError("archive requests per second must be positive")
        self.database = database
        self.config = config
        self.progress = progress

    async def run(self) -> dict[str, int]:
        timeout = httpx.Timeout(self.config.timeout)
        headers = {"User-Agent": "ph-catalog/0.1 (local public-catalogue research)"}
        async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
            response = await client.get(COLLINFO_URL)
            response.raise_for_status()
            indexes = response.json()
            if self.config.limit_indexes is not None:
                indexes = indexes[: self.config.limit_indexes]

            scans = 0
            failed_scans = 0
            consecutive_failures = 0
            captures = 0
            last_reported = (self.database.archive_counts()["unique_candidates"] // 10_000) * 10_000
            for index in indexes:
                for kind, pattern in ARCHIVE_PATTERNS.items():
                    index_id = str(index["id"])
                    if self.database.archive_scan_complete(index_id, kind):
                        continue
                    try:
                        urls, malformed = await self._fetch_urls(
                            client, str(index["cdx-api"]), pattern
                        )
                    except httpx.HTTPError:
                        if self.progress:
                            self.progress(
                                {
                                    "event": "archive_partition_fallback",
                                    "index": index_id,
                                    "path_kind": kind,
                                }
                            )
                        try:
                            urls, malformed = await self._fetch_partitioned_urls(
                                client, str(index["cdx-api"]), pattern
                            )
                        except httpx.HTTPError:
                            urls = []
                            malformed = 0
                            fallback_failed = True
                        else:
                            fallback_failed = False
                        if not fallback_failed:
                            candidates = {
                                slug for url in urls if (slug := archive_slug(url, kind))
                            }
                            self.database.apply_archive_scan(
                                index_id, kind, candidates, len(urls), malformed
                            )
                            consecutive_failures = 0
                            scans += 1
                            captures += len(urls)
                            continue
                        failed_scans += 1
                        consecutive_failures += 1
                        if self.progress:
                            self.progress(
                                {
                                    "event": "archive_scan_failed",
                                    "index": index_id,
                                    "path_kind": kind,
                                    "error": "HTTPError",
                                }
                            )
                        if consecutive_failures >= 3:
                            if self.progress:
                                self.progress(
                                    {
                                        "event": "archive_global_backoff",
                                        "delay_seconds": 120,
                                    }
                                )
                            await asyncio.sleep(120)
                            consecutive_failures = 0
                        continue
                    candidates = {slug for url in urls if (slug := archive_slug(url, kind))}
                    self.database.apply_archive_scan(
                        index_id, kind, candidates, len(urls), malformed
                    )
                    consecutive_failures = 0
                    scans += 1
                    captures += len(urls)
                    counts = self.database.archive_counts()
                    unique = counts["unique_candidates"]
                    while unique >= last_reported + 10_000:
                        last_reported += 10_000
                        if self.progress:
                            self.progress(
                                {
                                    "event": "archive_progress",
                                    "unique_candidates": unique,
                                    "products": counts["products"],
                                    "posts": counts["posts"],
                                    "scans_complete": counts["scans_complete"],
                                }
                            )

        result = self.database.archive_counts()
        result["scans_this_run"] = scans
        result["failed_scans_this_run"] = failed_scans
        result["captures_this_run"] = captures
        return result

    async def _fetch_partitioned_urls(
        self, client: httpx.AsyncClient, api_url: str, pattern: str
    ) -> tuple[list[str], int]:
        stem = pattern.removesuffix("*")
        urls: list[str] = []
        malformed = 0
        for prefix in ARCHIVE_PREFIXES:
            prefix_urls, prefix_malformed = await self._fetch_urls(
                client, api_url, f"{stem}{prefix}*"
            )
            urls.extend(prefix_urls)
            malformed += prefix_malformed
        return urls, malformed

    async def _fetch_urls(
        self, client: httpx.AsyncClient, api_url: str, pattern: str
    ) -> tuple[list[str], int]:
        base_params = {
            "url": pattern,
            "output": "json",
            "filter": "status:200",
            "fl": "url",
            "pageSize": "1",
        }
        page_count = await self._fetch_page_count(client, api_url, base_params)
        if page_count == 0:
            return [], 0
        urls: list[str] = []
        malformed = 0
        for page in range(page_count):
            page_urls, page_malformed = await self._fetch_page(
                client, api_url, {**base_params, "page": str(page)}
            )
            urls.extend(page_urls)
            malformed += page_malformed
        return urls, malformed

    async def _fetch_page_count(
        self, client: httpx.AsyncClient, api_url: str, params: dict[str, str]
    ) -> int:
        response = await self._request_with_retries(
            client, api_url, {**params, "showNumPages": "true"}
        )
        if response is None:
            return 0
        return int(response.json().get("pages", 0))

    async def _fetch_page(
        self, client: httpx.AsyncClient, api_url: str, params: dict[str, str]
    ) -> tuple[list[str], int]:
        for attempt in range(self.config.max_attempts):
            try:
                async with client.stream("GET", api_url, params=params) as response:
                    await asyncio.sleep(1 / self.config.requests_per_second)
                    if response.status_code == 404:
                        return [], 0
                    if response.status_code in {429, 500, 502, 503, 504}:
                        raise httpx.HTTPStatusError(
                            f"archive index returned {response.status_code}",
                            request=response.request,
                            response=response,
                        )
                    response.raise_for_status()
                    urls: list[str] = []
                    malformed = 0
                    async for line in response.aiter_lines():
                        if not line:
                            continue
                        try:
                            value = orjson.loads(line)
                        except orjson.JSONDecodeError:
                            malformed += 1
                            continue
                        if isinstance(value.get("url"), str):
                            urls.append(value["url"])
                    return urls, malformed
            except httpx.HTTPError:
                if attempt + 1 == self.config.max_attempts:
                    raise
                await asyncio.sleep(min(60.0, 2**attempt))
        raise RuntimeError("unreachable archive retry state")

    async def _request_with_retries(
        self, client: httpx.AsyncClient, api_url: str, params: dict[str, str]
    ) -> httpx.Response | None:
        for attempt in range(self.config.max_attempts):
            try:
                response = await client.get(api_url, params=params)
                await asyncio.sleep(1 / self.config.requests_per_second)
                if response.status_code == 404:
                    return None
                if response.status_code in {429, 500, 502, 503, 504}:
                    response.raise_for_status()
                response.raise_for_status()
                return response
            except httpx.HTTPError:
                if attempt + 1 == self.config.max_attempts:
                    raise
                await asyncio.sleep(min(60.0, 2**attempt))
        raise RuntimeError("unreachable archive retry state")
