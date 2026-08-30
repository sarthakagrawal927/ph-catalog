import orjson

from ph_catalog.config import CrawlConfig
from ph_catalog.crawler import CrawlMetrics
from ph_catalog.models import FetchResult, ProductStatus


def test_progress_interval_defaults_to_ten_thousand(tmp_path, capsys) -> None:
    config = CrawlConfig(
        database_path=tmp_path / "db.duckdb",
        log_path=tmp_path / "crawl.jsonl",
        failed_fixtures_dir=tmp_path / "failed",
    )
    assert config.progress_interval == 10_000
    assert config.snapshot_interval == 10_000

    metrics = CrawlMetrics(config.log_path, progress_interval=2)
    for index in range(2):
        slug = f"product-{index}"
        url = f"https://www.producthunt.com/products/{slug}"
        metrics.record(
            FetchResult(
                requested_slug=slug,
                canonical_slug=slug,
                requested_url=url,
                canonical_url=url,
                attempt=1,
                status=ProductStatus.FETCHED,
                http_status=200,
                proxy_label="proxy-1",
                elapsed_ms=10,
            ),
            concurrency=4,
        )

    output = [orjson.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert len(output) == 1
    assert output[0]["event"] == "progress"
    assert output[0]["processed"] == 2
