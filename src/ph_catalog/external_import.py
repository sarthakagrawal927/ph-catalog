from __future__ import annotations

import csv
import html
import json
import re
import zipfile
from dataclasses import dataclass
from io import TextIOWrapper
from pathlib import Path

from ph_catalog.archive import archive_slug
from ph_catalog.database import CatalogDatabase

NBOX_ROW_RE = re.compile(
    r"^\|\s*\d+\s*\|\s*\[(?P<name>.*?)\]"
    r"\(<https://www\.producthunt\.com/products/(?P<slug>[a-z0-9][a-z0-9-]*)"
    r"[^>]*>\)\s*\|\s*(?P<body>.*?)\s*\|\s*[\d,]+\s*\|\s*\d+\s*\|"
    r"\s*\[.*?\]\(<(?P<website>https?://[^>]+)>\)\s*\|$",
    re.MULTILINE,
)
NBOX_DESCRIPTION_RE = re.compile(
    r"<summary><strong>Full description</strong></summary><br>"
    r"(?P<description>.*?)<br></details>"
)


@dataclass(slots=True, frozen=True)
class ExternalImportResult:
    source_rows: int
    source_product_slugs: int
    staged_candidates: int
    direct_websites: int


@dataclass(slots=True, frozen=True)
class ExternalProductDumpResult:
    source_rows: int
    source_ids_added: int
    aliases_added: int
    staged_candidates: int


def import_kaggle_launches(
    database: CatalogDatabase,
    launches_csv: Path,
    *,
    redirects_csv: Path | None = None,
    product_dump_json: Path | None = None,
) -> ExternalImportResult:
    """Stage archive-only canonical products without counting launch slugs as products."""
    if not launches_csv.is_file():
        raise ValueError(f"Kaggle launches CSV does not exist: {launches_csv}")
    launches = str(launches_csv.resolve()).replace("'", "''")
    connection = database.connection
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW external_launches AS
        SELECT *, lower(regexp_extract(ph_url, '/products/([^/]+)/', 1)) AS product_slug,
               lower(regexp_extract(website, '/r/([^?]+)', 1)) AS redirect_token
        FROM read_csv('{launches}', header = true, auto_detect = true, sample_size = -1)
        """
    )
    if redirects_csv is not None:
        if not redirects_csv.is_file():
            raise ValueError(f"redirect CSV does not exist: {redirects_csv}")
        redirects = str(redirects_csv.resolve()).replace("'", "''")
        connection.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW external_redirects AS
            SELECT lower(regexp_extract("start url", '/r/([^?]+)', 1)) AS redirect_token,
                   nullif(trim("redirect_to zonder ?ref=producthunt"), '') AS direct_url
            FROM read_csv('{redirects}', header = true, auto_detect = true, sample_size = -1)
            QUALIFY row_number() OVER (
                PARTITION BY lower(regexp_extract("start url", '/r/([^?]+)', 1))
                ORDER BY direct_url IS NOT NULL DESC
            ) = 1
            """
        )
    else:
        connection.execute(
            """
            CREATE OR REPLACE TEMP VIEW external_redirects AS
            SELECT NULL::VARCHAR AS redirect_token, NULL::VARCHAR AS direct_url
            WHERE false
            """
        )

    candidates = connection.execute(
        """
        WITH ranked AS (
            SELECT launch.product_slug AS slug,
                   nullif(trim(launch.name), '') AS name,
                   nullif(trim(launch.tagline), '') AS tagline,
                   nullif(trim(launch.description), '') AS description,
                   coalesce(redirect.direct_url, nullif(trim(launch.website), '')) AS website_url,
                   CASE WHEN nullif(trim(launch.topic_slugs), '') IS NULL THEN []::VARCHAR[]
                        ELSE string_split(launch.topic_slugs, '|') END AS categories,
                   row_number() OVER (
                       PARTITION BY launch.product_slug
                       ORDER BY launch.description IS NOT NULL DESC,
                                launch.launch_date DESC NULLS LAST
                   ) AS position
            FROM external_launches AS launch
            LEFT JOIN external_redirects AS redirect USING (redirect_token)
            WHERE launch.product_slug != ''
        )
        SELECT ranked.slug, ranked.name, ranked.tagline, ranked.description,
               ranked.website_url, ranked.categories
        FROM ranked
        LEFT JOIN products USING (slug)
        LEFT JOIN product_aliases ON product_aliases.alias_slug = ranked.slug
        WHERE ranked.position = 1 AND products.slug IS NULL
          AND product_aliases.alias_slug IS NULL
        ORDER BY ranked.slug
        """
    ).fetchall()

    dump_products = _selected_dump_products(
        product_dump_json, {str(row[0]) for row in candidates}
    )
    merged = []
    for slug, name, tagline, description, website_url, categories in candidates:
        dump = dump_products.get(str(slug))
        merged.append(
            (
                str(slug),
                (dump or {}).get("name") or name,
                (dump or {}).get("tagline") or tagline,
                (dump or {}).get("description") or description,
                (dump or {}).get("website") or website_url,
                list(categories or []),
                f"https://www.producthunt.com/products/{slug}",
                int(dump["id"]) if dump and str(dump.get("id", "")).isdigit() else None,
            )
        )
    database.stage_external_products("kaggle-product-hunt-launches", merged)
    counts = connection.execute(
        """
        SELECT count(*), count(distinct product_slug)
        FROM external_launches WHERE product_slug != ''
        """
    ).fetchone()
    return ExternalImportResult(
        source_rows=int(counts[0]),
        source_product_slugs=int(counts[1]),
        staged_candidates=len(merged),
        direct_websites=sum(
            bool(row[4]) and not str(row[4]).startswith("https://www.producthunt.com/r/")
            for row in merged
        ),
    )


def import_product_dump(
    database: CatalogDatabase,
    product_dump_json: Path,
) -> ExternalProductDumpResult:
    """Import historical Product records and ID-backed rename relationships."""
    if not product_dump_json.is_file():
        raise ValueError(f"product dump JSON does not exist: {product_dump_json}")

    connection = database.connection
    product_slugs = {
        str(row[0]) for row in connection.execute("SELECT slug FROM products").fetchall()
    }
    alias_map = {
        str(row[0]): str(row[1])
        for row in connection.execute(
            "SELECT alias_slug, canonical_slug FROM product_aliases"
        ).fetchall()
    }
    source_map = {
        int(row[0]): str(row[1])
        for row in connection.execute(
            "SELECT product_id, slug FROM source_products"
        ).fetchall()
    }
    known_slugs = product_slugs | set(alias_map)

    source_rows = 0
    source_relations: list[tuple[int, str]] = []
    aliases: list[tuple[str, str]] = []
    staged: list[
        tuple[
            str,
            str | None,
            str | None,
            str | None,
            str | None,
            list[str],
            str,
            int | None,
        ]
    ] = []
    in_products = False
    with product_dump_json.open(encoding="utf-8") as stream:
        for line in stream:
            if line == '  "products": [\n':
                in_products = True
                continue
            if in_products and line == '  "posts": [\n':
                break
            if not in_products:
                continue
            value = line.strip().removesuffix(",")
            if not value or value in {"[", "]"}:
                continue
            product = json.loads(value)
            source_rows += 1
            slug = str(product.get("slug", "")).strip().lower()
            raw_id = product.get("id")
            product_id = int(raw_id) if str(raw_id).isdigit() else None
            if not slug or product_id is None:
                continue

            canonical_for_slug = slug if slug in product_slugs else alias_map.get(slug)
            canonical_for_id = source_map.get(product_id)
            if product_id not in source_map:
                source_relations.append((product_id, canonical_for_slug or slug))

            if slug in known_slugs:
                continue
            if canonical_for_id and canonical_for_id != slug:
                if canonical_for_id in product_slugs:
                    aliases.append((slug, canonical_for_id))
                continue
            staged.append(
                (
                    slug,
                    _text(product.get("name")),
                    _text(product.get("tagline")),
                    _text(product.get("description")),
                    _text(product.get("website")),
                    [],
                    f"https://www.producthunt.com/products/{slug}",
                    product_id,
                )
            )

    database.record_external_product_relations(source_relations, aliases)
    database.stage_external_products("kaggle-alanhamlett-product-dump", staged)
    return ExternalProductDumpResult(
        source_rows=source_rows,
        source_ids_added=len(source_relations),
        aliases_added=len(aliases),
        staged_candidates=len(staged),
    )


def import_product_metadata(
    database: CatalogDatabase,
    metadata_csv: Path,
) -> ExternalImportResult:
    """Stage complete canonical rows from a public Product Hunt metadata export."""
    if not metadata_csv.is_file():
        raise ValueError(f"product metadata CSV does not exist: {metadata_csv}")
    connection = database.connection
    known_slugs = {
        str(row[0])
        for row in connection.execute(
            "SELECT slug FROM products UNION SELECT alias_slug FROM product_aliases"
        ).fetchall()
    }
    source_rows = 0
    source_slugs: set[str] = set()
    staged = []
    with metadata_csv.open(newline="", encoding="utf-8") as stream:
        for row in csv.DictReader(stream):
            source_rows += 1
            product_url = str(row.get("url") or "")
            slug = archive_slug(product_url, "products")
            if slug is None:
                continue
            source_slugs.add(slug)
            if slug in known_slugs:
                continue
            name = _text(row.get("name"))
            description = _text(row.get("description"))
            website_url = _text(row.get("homepage"))
            tagline = _metadata_tagline(name, _text(row.get("title")))
            if not all((name, tagline, description, website_url)):
                continue
            staged.append(
                (
                    slug,
                    name,
                    tagline,
                    description,
                    website_url,
                    [],
                    f"https://www.producthunt.com/products/{slug}",
                    None,
                )
            )
    database.stage_external_products("kaggle-jessysisca-product-metadata", staged)
    return ExternalImportResult(
        source_rows=source_rows,
        source_product_slugs=len(source_slugs),
        staged_candidates=len(staged),
        direct_websites=len(staged),
    )


def import_daily_csv_archive(
    database: CatalogDatabase,
    archive_zip: Path,
) -> ExternalImportResult:
    """Stage complete canonical rows from a ZIP of Product Hunt daily CSVs."""
    if not archive_zip.is_file():
        raise ValueError(f"daily CSV archive does not exist: {archive_zip}")
    known_slugs = _known_slugs(database)
    source_rows = 0
    source_slugs: set[str] = set()
    candidates: dict[str, tuple] = {}
    with zipfile.ZipFile(archive_zip) as archive:
        for member in sorted(name for name in archive.namelist() if name.endswith(".csv")):
            with archive.open(member) as raw, TextIOWrapper(
                raw, encoding="utf-8-sig", newline=""
            ) as stream:
                for row in csv.DictReader(stream):
                    source_rows += 1
                    slug = archive_slug(str(row.get("URL") or ""), "products")
                    if slug is None:
                        continue
                    source_slugs.add(slug)
                    if slug in known_slugs:
                        continue
                    name = _text(row.get("Name"))
                    tagline = _text(row.get("Tagline"))
                    description = _text(row.get("Description"))
                    website = _text(row.get("Website"))
                    if not all((name, tagline, description, website)):
                        continue
                    categories = [
                        item.strip()
                        for item in str(row.get("Topics") or "").split(",")
                        if item.strip()
                    ]
                    candidates[slug] = (
                        slug,
                        name,
                        tagline,
                        description,
                        website,
                        categories,
                        f"https://www.producthunt.com/products/{slug}",
                        None,
                    )
    rows = list(candidates.values())
    database.stage_external_products("github-ranbot-product-hunt", rows)
    return ExternalImportResult(
        source_rows=source_rows,
        source_product_slugs=len(source_slugs),
        staged_candidates=len(rows),
        direct_websites=sum(
            not str(row[4]).startswith("https://www.producthunt.com/r/") for row in rows
        ),
    )


def import_markdown_archive(
    database: CatalogDatabase,
    archive_zip: Path,
) -> ExternalImportResult:
    """Stage complete canonical rows from nbox's public daily Markdown archive."""
    if not archive_zip.is_file():
        raise ValueError(f"Markdown archive does not exist: {archive_zip}")
    known_slugs = _known_slugs(database)
    source_rows = 0
    source_slugs: set[str] = set()
    candidates: dict[str, tuple] = {}
    with zipfile.ZipFile(archive_zip) as archive:
        for member in sorted(name for name in archive.namelist() if name.endswith(".md")):
            document = archive.read(member).decode("utf-8", "replace")
            for match in NBOX_ROW_RE.finditer(document):
                source_rows += 1
                slug = match.group("slug")
                source_slugs.add(slug)
                if slug in known_slugs:
                    continue
                body = match.group("body")
                description_match = NBOX_DESCRIPTION_RE.search(body)
                name = _clean_markdown_text(match.group("name"))
                tagline = _clean_markdown_text(body.split("<br><details", 1)[0])
                description = (
                    _clean_markdown_text(description_match.group("description"))
                    if description_match
                    else None
                )
                website = _text(html.unescape(match.group("website")))
                if not all((name, tagline, description, website)):
                    continue
                candidates[slug] = (
                    slug,
                    name,
                    tagline,
                    description,
                    website,
                    [],
                    f"https://www.producthunt.com/products/{slug}",
                    None,
                )
    rows = list(candidates.values())
    database.stage_external_products("github-nbox-producthunt-statistic", rows)
    return ExternalImportResult(
        source_rows=source_rows,
        source_product_slugs=len(source_slugs),
        staged_candidates=len(rows),
        direct_websites=sum(
            not str(row[4]).startswith("https://www.producthunt.com/r/") for row in rows
        ),
    )


def _selected_dump_products(path: Path | None, wanted: set[str]) -> dict[str, dict[str, object]]:
    if path is None:
        return {}
    if not path.is_file():
        raise ValueError(f"product dump JSON does not exist: {path}")
    selected: dict[str, dict[str, object]] = {}
    in_products = False
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            if line == '  "products": [\n':
                in_products = True
                continue
            if in_products and line == '  "posts": [\n':
                break
            if not in_products:
                continue
            value = line.strip().removesuffix(",")
            if not value or value in {"[", "]"}:
                continue
            product = json.loads(value)
            slug = str(product.get("slug", "")).strip().lower()
            if slug in wanted:
                selected[slug] = product
    return selected


def _text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _known_slugs(database: CatalogDatabase) -> set[str]:
    return {
        str(row[0])
        for row in database.connection.execute(
            "SELECT slug FROM products UNION SELECT alias_slug FROM product_aliases"
        ).fetchall()
    }


def _clean_markdown_text(value: str) -> str | None:
    text = html.unescape(value).replace("<br>", "\n").strip()
    return text or None


def _metadata_tagline(name: str | None, title: str | None) -> str | None:
    if not name or not title:
        return None
    for separator in (" - ", ": "):
        prefix = f"{name}{separator}"
        if title.startswith(prefix):
            return _text(title.removeprefix(prefix))
    return None
