from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from pathlib import Path

from ph_catalog.config import CrawlConfig
from ph_catalog.network import (
    BlockController,
    GlobalRateLimiter,
    ProxyPool,
    retry_after_seconds,
)


def config(tmp_path: Path) -> CrawlConfig:
    return CrawlConfig(
        database_path=tmp_path / "db.duckdb",
        log_path=tmp_path / "crawl.jsonl",
        failed_fixtures_dir=tmp_path / "failed",
        proxy_rotation_requests=100,
    )


async def test_proxy_rotation_is_fixed_and_not_block_driven(tmp_path: Path) -> None:
    pool = ProxyPool(["http://one.test", "http://two.test"], config(tmp_path))
    try:
        labels = [(await pool.next()).label for _ in range(101)]
        assert labels[:100] == ["proxy-1"] * 100
        assert labels[100] == "proxy-2"
    finally:
        await pool.close()


async def test_persistent_blocks_stop_globally() -> None:
    blocks = BlockController(threshold=2, max_backoff=10)
    _, stopped = await blocks.blocked("0")
    assert not stopped
    _, stopped = await blocks.blocked("0")
    assert stopped
    assert not await blocks.wait()


async def test_rate_reduces_after_blocks_and_recovers_slowly() -> None:
    limiter = GlobalRateLimiter(2.0, 0, 0)
    assert await limiter.reduce() == 1.0
    for _ in range(100):
        await limiter.success()
    assert limiter.rate == 1.25


def test_retry_after_supports_seconds_and_http_dates() -> None:
    assert retry_after_seconds("12", 1) == 12
    future = format_datetime(datetime.now(UTC) + timedelta(seconds=30))
    assert 25 <= retry_after_seconds(future, 1) <= 30
