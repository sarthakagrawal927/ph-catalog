from __future__ import annotations

import argparse
import asyncio
import html
import sys
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

import orjson

from ph_catalog.archive import (
    ARQUIVO_PT_SCAN_NAME,
    ArchiveImportConfig,
    ArquivoPtImportConfig,
    ArquivoPtImporter,
    CommonCrawlDirectImporter,
    CommonCrawlImporter,
    DirectArchiveImportConfig,
    IndependentCdxImportConfig,
    IndependentCdxImporter,
    InternetArchiveDirectImporter,
    InternetArchiveImportConfig,
    WaybackImportConfig,
    WaybackImporter,
)
from ph_catalog.archive_recovery import (
    ArchiveRecoveryConfig,
    WaybackPostRecovery,
    WaybackRecovery,
)
from ph_catalog.catalog_api import (
    CATALOG_SCAN_NAME,
    CatalogScanConfig,
    CatalogScanner,
    enrich_pending_catalog,
)
from ph_catalog.category_api import CategoryScanConfig, CategoryScanner
from ph_catalog.collection_api import CollectionScanConfig, CollectionScanner
from ph_catalog.common_crawl_recovery import (
    CommonCrawlPostRecovery,
    CommonCrawlRecovery,
    CommonCrawlRecoveryConfig,
)
from ph_catalog.compact_export import export_compact_archive
from ph_catalog.config import DEFAULT_SITEMAP_URLS, CrawlConfig
from ph_catalog.crawler import Crawler
from ph_catalog.database import CatalogDatabase
from ph_catalog.external_import import (
    import_daily_csv_archive,
    import_kaggle_launches,
    import_markdown_archive,
    import_product_dump,
    import_product_metadata,
)
from ph_catalog.network import load_proxy_urls
from ph_catalog.post_api import (
    POST_ID_SCAN_NAME,
    PostIdScanConfig,
    PostIdScanner,
    PostResolveConfig,
    PostResolver,
)
from ph_catalog.product_slug_api import ProductSlugResolveConfig, ProductSlugResolver
from ph_catalog.search_api import (
    TWO_CHARACTER_SEARCH_QUERIES,
    SearchScanConfig,
    SearchScanner,
)
from ph_catalog.sitemap import discover_products
from ph_catalog.website_enricher import WebsiteEnricher, WebsiteEnrichmentConfig


def _json(value: object) -> str:
    return orjson.dumps(value, option=orjson.OPT_INDENT_2).decode()


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _add_network_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--workers", type=int, default=4, help="Concurrent workers (1-8).")
    parser.add_argument("--rps", type=float, default=2.0, help="Global requests per second.")
    parser.add_argument("--batch-size", type=int, default=250, help="DuckDB write batch (100-500).")
    parser.add_argument("--max-attempts", type=int, default=8)
    parser.add_argument("--snapshot-every", type=int, default=10_000)
    parser.add_argument(
        "--proxy", action="append", default=[], help="Proxy URL; repeat for a pool."
    )
    parser.add_argument("--proxy-file", type=Path, help="One proxy URL per line.")
    parser.add_argument(
        "--proxy-rotation",
        type=int,
        default=150,
        help="Rotate after this many requests (100-250).",
    )
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--block-threshold", type=int, default=5)
    parser.add_argument("--max-block-backoff", type=float, default=900.0)
    parser.add_argument("--limit", type=int, help="Bound this run; useful for daily fallback.")


def _parser() -> argparse.ArgumentParser:
    root = _project_root()
    parser = argparse.ArgumentParser(prog="ph-catalog")
    parser.add_argument("--database", type=Path, default=root / "data/producthunt.duckdb")
    parser.add_argument("--log", type=Path, default=root / "logs/crawl.jsonl")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init", help="Create or migrate the local database.")

    sitemap = subparsers.add_parser("import-sitemap", help="Refresh the product queue.")
    sitemap.add_argument(
        "--sitemap-url",
        action="append",
        dest="sitemap_urls",
        help="Root sitemap URL; repeat to override the default product manifest set.",
    )

    catalog = subparsers.add_parser(
        "import-catalog",
        help="Scan Product Hunt's anonymous numeric product catalogue into DuckDB.",
    )
    catalog.add_argument("--start-id", type=int, default=1)
    catalog.add_argument("--stop-id", type=int, default=1_400_000)
    catalog.add_argument(
        "--scan-name",
        default=CATALOG_SCAN_NAME,
        help="Independent resumable cursor name; use a new name for a safe rescan.",
    )
    catalog.add_argument("--id-batch-size", type=int, default=200)
    catalog.add_argument("--workers", type=int, default=4)
    catalog.add_argument("--rps", type=float, default=2.0)
    catalog.add_argument("--timeout", type=float, default=60.0)
    catalog.add_argument("--block-threshold", type=int, default=5)
    catalog.add_argument("--max-block-backoff", type=float, default=900.0)

    enrich = subparsers.add_parser(
        "enrich-catalog",
        help="Fill incomplete catalogue rows from Product Hunt's structured page metadata.",
    )
    enrich.add_argument("--id-batch-size", type=int, default=200)
    enrich.add_argument("--workers", type=int, default=4)
    enrich.add_argument("--rps", type=float, default=2.0)
    enrich.add_argument("--timeout", type=float, default=60.0)
    enrich.add_argument("--block-threshold", type=int, default=5)
    enrich.add_argument("--max-block-backoff", type=float, default=900.0)
    enrich.add_argument("--limit", type=int)

    archive = subparsers.add_parser(
        "import-archive",
        help="Import historical Product Hunt URL candidates from Common Crawl indexes.",
    )
    archive.add_argument("--rps", type=float, default=1.0)
    archive.add_argument("--timeout", type=float, default=120.0)
    archive.add_argument("--max-attempts", type=int, default=5)
    archive.add_argument("--limit-indexes", type=int)

    direct_archive = subparsers.add_parser(
        "import-archive-direct",
        help="Import exact Product Hunt URL ranges from free Common Crawl CDX files.",
    )
    direct_archive.add_argument(
        "--crawl",
        action="append",
        required=True,
        help="Common Crawl ID such as CC-MAIN-2021-43; repeat for multiple crawls.",
    )
    direct_archive.add_argument("--timeout", type=float, default=60.0)
    direct_archive.add_argument("--max-attempts", type=int, default=5)
    direct_archive.add_argument(
        "--include-non-200",
        action="store_true",
        help=(
            "Import URL keys for redirects, blocks, and missing pages as a separate "
            "resumable scan."
        ),
    )

    internet_archive = subparsers.add_parser(
        "import-internet-archive",
        help="Import retired Product Hunt routes from ArchiveTeam CDX byte ranges.",
    )
    internet_archive.add_argument(
        "--item",
        action="append",
        help="Internet Archive item ID; repeat to override the built-in Product Hunt set.",
    )
    internet_archive.add_argument("--workers", type=int, default=3)
    internet_archive.add_argument("--timeout", type=float, default=300.0)
    internet_archive.add_argument("--max-attempts", type=int, default=5)
    internet_archive.add_argument(
        "--include-non-200",
        action="store_true",
        help=(
            "Import legacy URL keys for redirects, blocks, and missing pages as a "
            "separate resumable scan."
        ),
    )

    arquivo_pt = subparsers.add_parser(
        "import-arquivo-pt",
        help="Import Product Hunt captures from Portugal's independent web archive.",
    )
    arquivo_pt.add_argument("--scan-name", default=ARQUIVO_PT_SCAN_NAME)
    arquivo_pt.add_argument("--path-kind", action="append", choices=["products", "posts"])
    arquivo_pt.add_argument("--limit", type=int, default=100_000)
    arquivo_pt.add_argument("--timeout", type=float, default=300.0)
    arquivo_pt.add_argument("--max-attempts", type=int, default=5)
    arquivo_pt.add_argument(
        "--include-non-200",
        action="store_true",
        help=(
            "Import redirects, blocks, and missing-page URL keys as a separate "
            "resumable scan."
        ),
    )

    independent_cdx = subparsers.add_parser(
        "import-independent-cdx",
        help="Import Product Hunt URL keys from independent public web archives.",
    )
    independent_cdx.add_argument("--archive", action="append")
    independent_cdx.add_argument("--path-kind", action="append", choices=["products", "posts"])
    independent_cdx.add_argument("--timeout", type=float, default=120.0)
    independent_cdx.add_argument("--max-attempts", type=int, default=3)

    wayback = subparsers.add_parser(
        "import-wayback",
        help="Import historical Product Hunt candidates from the Internet Archive.",
    )
    wayback.add_argument("--batch-size", type=int, default=10_000)
    wayback.add_argument("--timeout", type=float, default=120.0)
    wayback.add_argument("--max-attempts", type=int, default=5)
    wayback.add_argument("--limit-batches", type=int)
    wayback.add_argument("--path-kind", action="append", choices=["products", "posts"])

    recovery = subparsers.add_parser(
        "recover-wayback",
        help="Recover unavailable products from archived pages without retaining HTML.",
    )
    recovery.add_argument("--workers", type=int, default=4)
    recovery.add_argument("--rps", type=float, default=1.0)
    recovery.add_argument("--batch-size", type=int, default=100)
    recovery.add_argument("--timeout", type=float, default=45.0)
    recovery.add_argument("--max-attempts", type=int, default=3)
    recovery.add_argument("--block-threshold", type=int, default=5)
    recovery.add_argument("--max-block-backoff", type=float, default=900.0)
    recovery.add_argument("--timestamp", help="Closest capture target in YYYYMMDD form.")
    recovery.add_argument("--limit", type=int)

    common_crawl_recovery = subparsers.add_parser(
        "recover-common-crawl",
        help="Recover unavailable products from exact Common Crawl WARC byte ranges.",
    )
    common_crawl_recovery.add_argument(
        "--crawl", action="append", help="Collection ID; repeat to restrict the scan."
    )
    common_crawl_recovery.add_argument("--rps", type=float, default=2.0)
    common_crawl_recovery.add_argument("--timeout", type=float, default=120.0)
    common_crawl_recovery.add_argument("--max-attempts", type=int, default=3)
    common_crawl_recovery.add_argument("--scan-only", action="store_true")
    common_crawl_recovery.add_argument("--limit", type=int)

    common_crawl_post_recovery = subparsers.add_parser(
        "recover-common-crawl-posts",
        help="Recover canonical products from exact Common Crawl launch records.",
    )
    common_crawl_post_recovery.add_argument(
        "--crawl", action="append", help="Collection ID; repeat to restrict the scan."
    )
    common_crawl_post_recovery.add_argument("--rps", type=float, default=2.0)
    common_crawl_post_recovery.add_argument("--timeout", type=float, default=120.0)
    common_crawl_post_recovery.add_argument("--max-attempts", type=int, default=3)
    common_crawl_post_recovery.add_argument("--scan-only", action="store_true")
    common_crawl_post_recovery.add_argument("--limit", type=int)

    post_recovery = subparsers.add_parser(
        "recover-wayback-posts",
        help="Recover canonical products from live-unavailable archived launch pages.",
    )
    post_recovery.add_argument("--workers", type=int, default=4)
    post_recovery.add_argument("--rps", type=float, default=1.0)
    post_recovery.add_argument("--batch-size", type=int, default=100)
    post_recovery.add_argument("--timeout", type=float, default=45.0)
    post_recovery.add_argument("--max-attempts", type=int, default=3)
    post_recovery.add_argument("--block-threshold", type=int, default=5)
    post_recovery.add_argument("--max-block-backoff", type=float, default=900.0)
    post_recovery.add_argument("--timestamp", help="Closest capture target in YYYYMMDD form.")
    post_recovery.add_argument("--limit", type=int)

    recovery_requeue = subparsers.add_parser(
        "requeue-archive-recovery",
        help="Retry selected archive outcomes against a different capture date.",
    )
    recovery_requeue.add_argument(
        "--outcome",
        action="append",
        choices=["no_capture", "parse_failed", "retry"],
        required=True,
    )

    post_recovery_requeue = subparsers.add_parser(
        "requeue-archive-post-recovery",
        help="Retry archived launch outcomes against a different capture date.",
    )
    post_recovery_requeue.add_argument(
        "--outcome",
        action="append",
        choices=["no_capture", "parse_failed", "retry"],
        required=True,
    )

    posts = subparsers.add_parser(
        "resolve-posts",
        help="Resolve legacy archive post slugs to canonical Product Hunt products.",
    )
    posts.add_argument("--batch-size", type=int, default=100)
    posts.add_argument("--workers", type=int, default=1)
    posts.add_argument("--rps", type=float, default=0.5)
    posts.add_argument("--timeout", type=float, default=60.0)
    posts.add_argument("--block-threshold", type=int, default=5)
    posts.add_argument("--max-block-backoff", type=float, default=900.0)
    posts.add_argument("--limit", type=int)

    post_ids = subparsers.add_parser(
        "scan-post-ids",
        help="Exhaust public numeric launch IDs and stage their canonical products.",
    )
    post_ids.add_argument("--start-id", type=int, default=1)
    post_ids.add_argument("--stop-id", type=int, default=1_240_000)
    post_ids.add_argument("--id-batch-size", type=int, default=200)
    post_ids.add_argument("--workers", type=int, default=4)
    post_ids.add_argument("--rps", type=float, default=1.0)
    post_ids.add_argument("--timeout", type=float, default=60.0)
    post_ids.add_argument("--block-threshold", type=int, default=5)
    post_ids.add_argument("--max-block-backoff", type=float, default=900.0)

    products = subparsers.add_parser(
        "resolve-products",
        help="Resolve archive-only product slugs through Product Hunt structured data.",
    )
    products.add_argument("--batch-size", type=int, default=100)
    products.add_argument("--rps", type=float, default=0.5)
    products.add_argument("--timeout", type=float, default=60.0)
    products.add_argument("--block-threshold", type=int, default=5)
    products.add_argument("--max-block-backoff", type=float, default=900.0)
    products.add_argument("--limit", type=int)

    search = subparsers.add_parser(
        "scan-search",
        help="Exhaust Product Hunt search result windows and import unseen product IDs.",
    )
    search.add_argument("--query", action="append")
    search.add_argument(
        "--two-character",
        action="store_true",
        help="Scan all 676 two-letter search partitions.",
    )
    search.add_argument("--page-batch-size", type=int, default=100)
    search.add_argument("--workers", type=int, default=4)
    search.add_argument("--rps", type=float, default=1.0)
    search.add_argument("--timeout", type=float, default=60.0)
    search.add_argument("--block-threshold", type=int, default=5)
    search.add_argument("--max-block-backoff", type=float, default=900.0)

    categories = subparsers.add_parser(
        "scan-categories",
        help="Exhaust live and retired Product Hunt category memberships.",
    )
    categories.add_argument("--workers", type=int, default=4)
    categories.add_argument("--page-batch-size", type=int, default=20)
    categories.add_argument("--rps", type=float, default=2.0)
    categories.add_argument("--timeout", type=float, default=60.0)
    categories.add_argument("--block-threshold", type=int, default=5)
    categories.add_argument("--max-block-backoff", type=float, default=900.0)
    categories.add_argument("--limit-pages", type=int)

    collections = subparsers.add_parser(
        "scan-collections",
        help="Exhaust public collection memberships and import unseen product IDs.",
    )
    collections.add_argument("--workers", type=int, default=4)
    collections.add_argument("--rps", type=float, default=2.0)
    collections.add_argument("--timeout", type=float, default=60.0)
    collections.add_argument("--block-threshold", type=int, default=5)
    collections.add_argument("--max-block-backoff", type=float, default=900.0)
    collections.add_argument("--limit-pages", type=int)

    external = subparsers.add_parser(
        "import-external",
        help="Stage archive-only canonical products from a public Kaggle launch export.",
    )
    external.add_argument("--launches-csv", type=Path, required=True)
    external.add_argument("--redirects-csv", type=Path)
    external.add_argument("--product-dump-json", type=Path)

    product_dump = subparsers.add_parser(
        "import-product-dump",
        help="Import canonical products and ID-backed aliases from a public JSON dump.",
    )
    product_dump.add_argument("--product-dump-json", type=Path, required=True)

    product_metadata = subparsers.add_parser(
        "import-product-metadata",
        help="Stage complete canonical products from a public metadata CSV.",
    )
    product_metadata.add_argument("--metadata-csv", type=Path, required=True)

    daily_archive = subparsers.add_parser(
        "import-daily-archive",
        help="Stage complete canonical products from a ZIP of daily CSV exports.",
    )
    daily_archive.add_argument("--archive-zip", type=Path, required=True)

    markdown_archive = subparsers.add_parser(
        "import-markdown-archive",
        help="Stage complete canonical products from the nbox Markdown archive.",
    )
    markdown_archive.add_argument("--archive-zip", type=Path, required=True)

    subparsers.add_parser(
        "promote-external",
        help="Promote complete archive records after current aliases have been resolved.",
    )

    website_enrichment = subparsers.add_parser(
        "enrich-websites",
        help="Fill missing copy from public product-site metadata with provenance.",
    )
    website_enrichment.add_argument("--workers", type=int, default=4)
    website_enrichment.add_argument("--rps", type=float, default=2.0)
    website_enrichment.add_argument("--timeout", type=float, default=30.0)
    website_enrichment.add_argument("--max-attempts", type=int, default=3)
    website_enrichment.add_argument("--batch-size", type=int, default=100)
    website_enrichment.add_argument("--max-response-bytes", type=int, default=2_000_000)
    website_enrichment.add_argument("--max-redirects", type=int, default=5)
    website_enrichment.add_argument("--limit", type=int)
    website_enrichment.add_argument(
        "--apply",
        action="store_true",
        help="Apply selected metadata to still-empty fields; default only records candidates.",
    )
    apply_website_enrichment = subparsers.add_parser(
        "apply-website-enrichments",
        help="Apply explicitly reviewed stored website-copy candidates.",
    )
    apply_website_enrichment.add_argument("--slug", action="append", required=True)
    subparsers.add_parser(
        "exclude-incomplete-copy",
        help="Quarantine and remove fetched products still missing tagline or description.",
    )

    crawl = subparsers.add_parser("crawl", help="Drain pending/retry products.")
    _add_network_options(crawl)

    backfill = subparsers.add_parser(
        "backfill", help="Refresh the manifest, crawl, then write a snapshot."
    )
    backfill.add_argument(
        "--sitemap-url",
        action="append",
        dest="sitemap_urls",
        help="Root sitemap URL; repeat to override the default product manifest set.",
    )
    backfill.add_argument("--snapshot", type=Path)
    _add_network_options(backfill)

    daily = subparsers.add_parser(
        "daily", help="Fallback: refresh new products, then process a bounded batch."
    )
    daily.add_argument(
        "--sitemap-url",
        action="append",
        dest="sitemap_urls",
        help="Root sitemap URL; repeat to override the default product manifest set.",
    )
    daily.add_argument("--snapshot", type=Path)
    _add_network_options(daily)
    daily.set_defaults(limit=500)

    snapshot = subparsers.add_parser("snapshot", help="Export fetched rows as Zstd Parquet.")
    snapshot.add_argument("--output", type=Path)

    compact = subparsers.add_parser(
        "compact-archive", help="Export useful catalogue data as a maximum-compression tar.zst."
    )
    compact.add_argument("--output", type=Path, required=True)
    compact.add_argument("--compression-level", type=int, default=22)

    restore_snapshot = subparsers.add_parser(
        "restore-snapshot",
        help="Seed missing fetched rows from a seven-field Parquet snapshot.",
    )
    restore_snapshot.add_argument("--input", type=Path, required=True)

    subparsers.add_parser("status", help="Show queue and alias counts.")
    subparsers.add_parser("verify", help="Check duplicates and fetched completeness.")

    requeue = subparsers.add_parser("requeue", help="Return failed rows to the retry queue.")
    requeue.add_argument(
        "--status", action="append", choices=["parse_failed", "unavailable", "retry"], required=True
    )
    requeue.add_argument("--reset-attempts", action="store_true")

    launchd = subparsers.add_parser(
        "launchd-plist", help="Render (but do not install) the low-rate daily job."
    )
    launchd.add_argument("--output", type=Path, required=True)
    launchd.add_argument("--hour", type=int, default=3)
    launchd.add_argument("--minute", type=int, default=15)
    launchd.add_argument("--daily-limit", type=int, default=500)
    return parser


def _config(args: argparse.Namespace) -> CrawlConfig:
    root = _project_root()
    return CrawlConfig(
        database_path=args.database,
        log_path=args.log,
        failed_fixtures_dir=root / "data/failed_pages",
        snapshot_dir=root / "snapshots",
        workers=args.workers,
        requests_per_second=args.rps,
        request_timeout=args.timeout,
        batch_size=args.batch_size,
        max_attempts=args.max_attempts,
        snapshot_interval=args.snapshot_every,
        proxy_rotation_requests=args.proxy_rotation,
        block_threshold=args.block_threshold,
        max_block_backoff=args.max_block_backoff,
    )


async def _import_sitemap(
    database: CatalogDatabase, urls: list[str] | tuple[str, ...] | None
) -> dict[str, object]:
    roots = urls or DEFAULT_SITEMAP_URLS
    discovery = await discover_products(roots)
    inserted, discovered = database.import_products(discovery.products)
    return {
        "roots": len(roots),
        "sitemaps": discovery.sitemap_count,
        "discovered": discovered,
        "inserted": inserted,
        "duplicates_in_manifest": discovered - len({item.slug for item in discovery.products}),
    }


async def _crawl(database: CatalogDatabase, args: argparse.Namespace) -> dict[str, object]:
    config = _config(args)
    proxy_urls = load_proxy_urls(args.proxy, args.proxy_file)
    return await Crawler(database, config, proxy_urls).run(args.limit)


def _snapshot_path(value: Path | None) -> Path:
    if value:
        return value
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return _project_root() / f"snapshots/producthunt-{stamp}.parquet"


def _launchd_plist(args: argparse.Namespace) -> str:
    root = _project_root()
    executable = root / ".venv/bin/ph-catalog"
    command = [
        str(executable),
        "--database",
        str(args.database.resolve()),
        "--log",
        str(args.log.resolve()),
        "daily",
        "--limit",
        str(args.daily_limit),
        "--workers",
        "2",
        "--rps",
        "0.25",
    ]
    arguments = "\n".join(f"      <string>{html.escape(item)}</string>" for item in command)
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>local.ph-catalog.daily</string>
  <key>ProgramArguments</key>
  <array>
{arguments}
  </array>
  <key>WorkingDirectory</key>
  <string>{html.escape(str(root))}</string>
  <key>StartCalendarInterval</key>
  <dict>
    <key>Hour</key><integer>{args.hour}</integer>
    <key>Minute</key><integer>{args.minute}</integer>
  </dict>
  <key>ProcessType</key><string>Background</string>
  <key>StandardOutPath</key><string>{html.escape(str(root / "logs/launchd.out.log"))}</string>
  <key>StandardErrorPath</key><string>{html.escape(str(root / "logs/launchd.err.log"))}</string>
</dict>
</plist>
"""


async def _run(args: argparse.Namespace) -> int:
    if args.command == "launchd-plist":
        if not 0 <= args.hour <= 23 or not 0 <= args.minute <= 59:
            raise ValueError("launchd hour/minute are out of range")
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(_launchd_plist(args), encoding="utf-8")
        print(_json({"output": str(args.output), "installed": False}))
        return 0

    with CatalogDatabase(args.database) as database:
        if args.command == "init":
            print(_json({"database": str(args.database), "status": "ready"}))
        elif args.command == "import-sitemap":
            print(_json(await _import_sitemap(database, args.sitemap_urls)))
        elif args.command == "import-catalog":
            config = CatalogScanConfig(
                scan_name=args.scan_name,
                start_id=args.start_id,
                stop_id=args.stop_id,
                batch_size=args.id_batch_size,
                workers=args.workers,
                requests_per_second=args.rps,
                timeout=args.timeout,
                block_threshold=args.block_threshold,
                max_block_backoff=args.max_block_backoff,
            )
            print(_json(await CatalogScanner(database, config, args.log).run()))
        elif args.command == "enrich-catalog":
            config = CatalogScanConfig(
                stop_id=2,
                batch_size=args.id_batch_size,
                workers=args.workers,
                requests_per_second=args.rps,
                timeout=args.timeout,
                block_threshold=args.block_threshold,
                max_block_backoff=args.max_block_backoff,
            )
            print(
                _json(
                    await enrich_pending_catalog(
                        database, config, args.log, limit=args.limit
                    )
                )
            )
        elif args.command == "import-archive":
            config = ArchiveImportConfig(
                requests_per_second=args.rps,
                timeout=args.timeout,
                max_attempts=args.max_attempts,
                limit_indexes=args.limit_indexes,
            )
            importer = CommonCrawlImporter(
                database,
                config,
                progress=lambda event: print(_json(event), flush=True),
            )
            print(_json(await importer.run()))
        elif args.command == "import-archive-direct":
            config = DirectArchiveImportConfig(
                crawls=tuple(args.crawl),
                timeout=args.timeout,
                max_attempts=args.max_attempts,
                include_non_200=args.include_non_200,
            )
            importer = CommonCrawlDirectImporter(
                database,
                config,
                progress=lambda event: print(_json(event), flush=True),
            )
            print(_json(await importer.run()))
        elif args.command == "import-internet-archive":
            config = InternetArchiveImportConfig(
                items=tuple(args.item) if args.item else InternetArchiveImportConfig().items,
                workers=args.workers,
                timeout=args.timeout,
                max_attempts=args.max_attempts,
                include_non_200=args.include_non_200,
            )
            importer = InternetArchiveDirectImporter(
                database,
                config,
                progress=lambda event: print(_json(event), flush=True),
            )
            print(_json(await importer.run()))
        elif args.command == "import-arquivo-pt":
            config = ArquivoPtImportConfig(
                scan_name=args.scan_name,
                path_kinds=tuple(args.path_kind or ("products", "posts")),
                limit=args.limit,
                timeout=args.timeout,
                max_attempts=args.max_attempts,
                include_non_200=args.include_non_200,
            )
            importer = ArquivoPtImporter(
                database,
                config,
                progress=lambda event: print(_json(event), flush=True),
            )
            print(_json(await importer.run()))
        elif args.command == "import-independent-cdx":
            defaults = IndependentCdxImportConfig()
            config = IndependentCdxImportConfig(
                archives=tuple(args.archive) if args.archive else defaults.archives,
                path_kinds=tuple(args.path_kind or ("products", "posts")),
                timeout=args.timeout,
                max_attempts=args.max_attempts,
            )
            importer = IndependentCdxImporter(
                database,
                config,
                progress=lambda event: print(_json(event), flush=True),
            )
            print(_json(await importer.run()))
        elif args.command == "import-wayback":
            config = WaybackImportConfig(
                batch_size=args.batch_size,
                timeout=args.timeout,
                max_attempts=args.max_attempts,
                path_kinds=tuple(args.path_kind or ("products", "posts")),
                limit_batches=args.limit_batches,
            )
            importer = WaybackImporter(
                database,
                config,
                progress=lambda event: print(_json(event), flush=True),
            )
            print(_json(await importer.run()))
        elif args.command == "recover-wayback":
            config = ArchiveRecoveryConfig(
                workers=args.workers,
                requests_per_second=args.rps,
                batch_size=args.batch_size,
                timeout=args.timeout,
                max_attempts=args.max_attempts,
                block_threshold=args.block_threshold,
                max_block_backoff=args.max_block_backoff,
                target_timestamp=args.timestamp,
                limit=args.limit,
            )
            print(_json(await WaybackRecovery(database, config, args.log).run()))
        elif args.command == "recover-common-crawl":
            config = CommonCrawlRecoveryConfig(
                crawls=tuple(args.crawl) if args.crawl else None,
                requests_per_second=args.rps,
                timeout=args.timeout,
                max_attempts=args.max_attempts,
                scan_only=args.scan_only,
                limit=args.limit,
            )
            recovery = CommonCrawlRecovery(
                database,
                config,
                progress=lambda event: print(_json(event), flush=True),
            )
            print(_json(await recovery.run()))
        elif args.command == "recover-common-crawl-posts":
            config = CommonCrawlRecoveryConfig(
                crawls=tuple(args.crawl) if args.crawl else None,
                requests_per_second=args.rps,
                timeout=args.timeout,
                max_attempts=args.max_attempts,
                scan_only=args.scan_only,
                limit=args.limit,
            )
            recovery = CommonCrawlPostRecovery(
                database,
                config,
                progress=lambda event: print(_json(event), flush=True),
            )
            print(_json(await recovery.run()))
        elif args.command == "recover-wayback-posts":
            config = ArchiveRecoveryConfig(
                workers=args.workers,
                requests_per_second=args.rps,
                batch_size=args.batch_size,
                timeout=args.timeout,
                max_attempts=args.max_attempts,
                block_threshold=args.block_threshold,
                max_block_backoff=args.max_block_backoff,
                target_timestamp=args.timestamp,
                limit=args.limit,
            )
            print(_json(await WaybackPostRecovery(database, config, args.log).run()))
        elif args.command == "requeue-archive-recovery":
            print(
                _json(
                    {
                        "requeued": database.requeue_archive_recoveries(args.outcome),
                        "outcomes": args.outcome,
                    }
                )
            )
        elif args.command == "requeue-archive-post-recovery":
            print(
                _json(
                    {
                        "requeued": database.requeue_archive_post_recoveries(
                            args.outcome
                        ),
                        "outcomes": args.outcome,
                    }
                )
            )
        elif args.command == "resolve-posts":
            config = PostResolveConfig(
                batch_size=args.batch_size,
                workers=args.workers,
                requests_per_second=args.rps,
                timeout=args.timeout,
                block_threshold=args.block_threshold,
                max_block_backoff=args.max_block_backoff,
                limit=args.limit,
            )
            print(_json(await PostResolver(database, config, args.log).run()))
        elif args.command == "scan-post-ids":
            config = PostIdScanConfig(
                start_id=args.start_id,
                stop_id=args.stop_id,
                batch_size=args.id_batch_size,
                workers=args.workers,
                requests_per_second=args.rps,
                timeout=args.timeout,
                block_threshold=args.block_threshold,
                max_block_backoff=args.max_block_backoff,
            )
            print(_json(await PostIdScanner(database, config, args.log).run()))
        elif args.command == "resolve-products":
            config = ProductSlugResolveConfig(
                batch_size=args.batch_size,
                requests_per_second=args.rps,
                timeout=args.timeout,
                block_threshold=args.block_threshold,
                max_block_backoff=args.max_block_backoff,
                limit=args.limit,
            )
            print(_json(await ProductSlugResolver(database, config, args.log).run()))
        elif args.command == "scan-search":
            queries = (
                TWO_CHARACTER_SEARCH_QUERIES
                if args.two_character
                else tuple(args.query) if args.query else SearchScanConfig().queries
            )
            config = SearchScanConfig(
                queries=queries,
                workers=args.workers,
                page_batch_size=args.page_batch_size,
                requests_per_second=args.rps,
                timeout=args.timeout,
                block_threshold=args.block_threshold,
                max_block_backoff=args.max_block_backoff,
            )
            print(_json(await SearchScanner(database, config, args.log).run()))
        elif args.command == "scan-categories":
            config = CategoryScanConfig(
                workers=args.workers,
                page_batch_size=args.page_batch_size,
                requests_per_second=args.rps,
                timeout=args.timeout,
                block_threshold=args.block_threshold,
                max_block_backoff=args.max_block_backoff,
                limit_pages=args.limit_pages,
            )
            print(_json(await CategoryScanner(database, config, args.log).run()))
        elif args.command == "scan-collections":
            config = CollectionScanConfig(
                workers=args.workers,
                requests_per_second=args.rps,
                timeout=args.timeout,
                block_threshold=args.block_threshold,
                max_block_backoff=args.max_block_backoff,
                limit_pages=args.limit_pages,
            )
            print(_json(await CollectionScanner(database, config, args.log).run()))
        elif args.command == "import-external":
            result = import_kaggle_launches(
                database,
                args.launches_csv,
                redirects_csv=args.redirects_csv,
                product_dump_json=args.product_dump_json,
            )
            print(_json(asdict(result)))
        elif args.command == "import-product-dump":
            print(_json(asdict(import_product_dump(database, args.product_dump_json))))
        elif args.command == "import-product-metadata":
            print(_json(asdict(import_product_metadata(database, args.metadata_csv))))
        elif args.command == "import-daily-archive":
            print(_json(asdict(import_daily_csv_archive(database, args.archive_zip))))
        elif args.command == "import-markdown-archive":
            print(_json(asdict(import_markdown_archive(database, args.archive_zip))))
        elif args.command == "promote-external":
            print(_json({"promoted": database.promote_external_products()}))
        elif args.command == "enrich-websites":
            config = WebsiteEnrichmentConfig(
                workers=args.workers,
                requests_per_second=args.rps,
                timeout=args.timeout,
                max_attempts=args.max_attempts,
                batch_size=args.batch_size,
                max_response_bytes=args.max_response_bytes,
                max_redirects=args.max_redirects,
                limit=args.limit,
                apply_fields=args.apply,
            )
            print(_json(await WebsiteEnricher(database, config, args.log).run()))
        elif args.command == "apply-website-enrichments":
            print(_json(database.apply_stored_website_enrichments(args.slug)))
        elif args.command == "exclude-incomplete-copy":
            print(_json(database.exclude_incomplete_copy_products()))
        elif args.command == "crawl":
            print(_json(await _crawl(database, args)))
        elif args.command in {"backfill", "daily"}:
            imported = await _import_sitemap(database, args.sitemap_urls)
            crawled = await _crawl(database, args)
            output = _snapshot_path(args.snapshot)
            exported = database.snapshot(output)
            print(
                _json(
                    {
                        "manifest": imported,
                        "crawl": crawled,
                        "snapshot": str(output),
                        "exported": exported,
                    }
                )
            )
        elif args.command == "snapshot":
            output = _snapshot_path(args.output)
            print(_json({"output": str(output), "exported": database.snapshot(output)}))
        elif args.command == "compact-archive":
            print(
                _json(
                    export_compact_archive(
                        database,
                        args.output,
                        compression_level=args.compression_level,
                    )
                )
            )
        elif args.command == "restore-snapshot":
            print(
                _json(
                    {
                        "input": str(args.input),
                        "restored": database.restore_snapshot(args.input),
                    }
                )
            )
        elif args.command == "status":
            status: dict[str, object] = database.counts()
            catalog_scan = database.manifest_scan_state(CATALOG_SCAN_NAME)
            if catalog_scan is not None:
                status["catalog_scan"] = catalog_scan
            post_id_scan = database.manifest_scan_state(POST_ID_SCAN_NAME)
            if post_id_scan is not None:
                status["post_id_scan"] = post_id_scan
            archive = database.archive_counts()
            if archive["scans_complete"]:
                status["archive"] = archive
            recovery = database.archive_recovery_counts()
            if len(recovery) > 1 or recovery.get("eligible_unavailable"):
                status["archive_recovery"] = recovery
            common_crawl_recovery = database.common_crawl_recovery_counts()
            if common_crawl_recovery["scans_complete"]:
                status["common_crawl_recovery"] = common_crawl_recovery
            common_crawl_post_recovery = database.common_crawl_post_recovery_counts()
            if common_crawl_post_recovery["scans_complete"]:
                status["common_crawl_post_recovery"] = common_crawl_post_recovery
            post_recovery = database.archive_post_recovery_counts()
            if post_recovery:
                status["archive_post_recovery"] = post_recovery
            search = database.search_counts()
            if search["queries"]:
                status["search"] = search
            categories = database.category_counts()
            if categories["categories"]:
                status["categories"] = categories
            collections = database.collection_counts()
            if collections["collections_reported"]:
                status["collections"] = collections
            external = database.external_counts()
            if external["staged"]:
                status["external"] = external
            website_enrichment = database.website_enrichment_counts()
            if website_enrichment:
                status["website_enrichment"] = website_enrichment
            exclusions = database.exclusion_counts()
            if exclusions["products"]:
                status["excluded"] = exclusions
            print(_json(status))
        elif args.command == "verify":
            report = database.verify()
            print(_json(report))
            duplicates = report["duplicates"]
            return int(bool(duplicates["slug"] or duplicates["producthunt_url"]))
        elif args.command == "requeue":
            changed = database.requeue(args.status, reset_attempts=args.reset_attempts)
            print(_json({"requeued": changed, "statuses": args.status}))
    return 0


def main(argv: list[str] | None = None) -> None:
    parser = _parser()
    try:
        code = asyncio.run(_run(parser.parse_args(argv)))
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    except KeyboardInterrupt:
        print("operation interrupted; committed work remains resumable", file=sys.stderr)
        code = 130
    raise SystemExit(code)
