from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

SITEMAP_BASE_URL = "https://www.producthunt.com/sitemaps_v3"


def _numbered_sitemaps(family: str, last_shard: int) -> tuple[str, ...]:
    return tuple(
        f"{SITEMAP_BASE_URL}/{family}_sitemap{shard}.xml.gz"
        for shard in range(1, last_shard + 1)
    )


DEFAULT_SITEMAP_URLS = (
    f"{SITEMAP_BASE_URL}/product_about_sitemap.xml.gz",
    f"{SITEMAP_BASE_URL}/product_imported_sitemap.xml.gz",
    f"{SITEMAP_BASE_URL}/product_alternatives_sitemap.xml.gz",
    f"{SITEMAP_BASE_URL}/product_reviews_sitemap.xml.gz",
    f"{SITEMAP_BASE_URL}/product_addons_sitemap.xml.gz",
    f"{SITEMAP_BASE_URL}/product_jobs_sitemap.xml.gz",
) + (
    *_numbered_sitemaps("product_about", 16),
    f"{SITEMAP_BASE_URL}/product_imported_sitemap2.xml.gz",
    *_numbered_sitemaps("product_alternatives", 6),
    *_numbered_sitemaps("product_reviews", 4),
)
# Kept for callers that explicitly want the canonical Product Hunt product manifest.
DEFAULT_SITEMAP_URL = DEFAULT_SITEMAP_URLS[0]
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140 Safari/537.36 "
    "ph-catalog/0.1"
)


@dataclass(slots=True, frozen=True)
class CrawlConfig:
    database_path: Path
    log_path: Path
    failed_fixtures_dir: Path
    snapshot_dir: Path | None = None
    workers: int = 4
    requests_per_second: float = 2.0
    jitter_min: float = 0.05
    jitter_max: float = 0.25
    request_timeout: float = 30.0
    batch_size: int = 250
    max_attempts: int = 8
    progress_interval: int = 10_000
    snapshot_interval: int = 10_000
    proxy_rotation_requests: int = 150
    block_threshold: int = 5
    max_block_backoff: float = 900.0
    failed_fixture_cap: int = 10
    failed_fixture_bytes: int = 512_000
    user_agent: str = DEFAULT_USER_AGENT

    def __post_init__(self) -> None:
        if not 1 <= self.workers <= 8:
            raise ValueError("workers must be between 1 and 8")
        if self.requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        if not 100 <= self.batch_size <= 500:
            raise ValueError("batch_size must be between 100 and 500")
        if not 100 <= self.proxy_rotation_requests <= 250:
            raise ValueError("proxy rotation must be between 100 and 250 requests")
        if self.progress_interval <= 0:
            raise ValueError("progress_interval must be positive")
        if self.snapshot_interval <= 0:
            raise ValueError("snapshot_interval must be positive")
        if self.jitter_min < 0 or self.jitter_max < self.jitter_min:
            raise ValueError("invalid jitter range")
