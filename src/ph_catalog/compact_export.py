from __future__ import annotations

import shutil
import subprocess
import tarfile
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import orjson

from ph_catalog.database import CatalogDatabase

TABLES: dict[str, dict[str, object]] = {
    "products.csv": {
        "description": "Complete, active catalogue products.",
        "columns": [
            ["slug", "VARCHAR"],
            ["name", "VARCHAR"],
            ["tagline", "VARCHAR"],
            ["description", "VARCHAR"],
            ["website_url", "VARCHAR"],
            ["categories", "VARCHAR[]"],
            ["producthunt_url", "VARCHAR"],
        ],
        "query": """
            SELECT slug, name, tagline, description, website_url,
                   coalesce(categories, []::VARCHAR[]) AS categories,
                   producthunt_url
            FROM products
            WHERE status = 'fetched'
            ORDER BY slug
        """,
    },
    "product_ids.csv": {
        "description": "Product Hunt numeric IDs; multiple historical IDs may map to one slug.",
        "columns": [["slug", "VARCHAR"], ["product_id", "BIGINT"]],
        "query": """
            SELECT slug, product_id
            FROM source_products
            ORDER BY slug, product_id
        """,
    },
    "launches.csv": {
        "description": (
            "Resolved launch-to-product relationships. resolved_at is deliberately excluded: "
            "it is crawler time, not launch time."
        ),
        "columns": [
            ["product_slug", "VARCHAR"],
            ["post_slug", "VARCHAR"],
            ["post_id", "BIGINT"],
        ],
        "query": """
            SELECT product_slug, post_slug, post_id
            FROM archive_post_resolutions
            WHERE product_slug IS NOT NULL
            ORDER BY product_slug, post_id, post_slug
        """,
    },
    "aliases.csv": {
        "description": "Redirected Product Hunt slugs mapped to canonical products.",
        "columns": [
            ["alias_slug", "VARCHAR"],
            ["canonical_slug", "VARCHAR"],
            ["alias_url", "VARCHAR"],
            ["canonical_url", "VARCHAR"],
        ],
        "query": """
            SELECT alias_slug, canonical_slug, alias_url, canonical_url
            FROM product_aliases
            ORDER BY canonical_slug, alias_slug
        """,
    },
    "provenance.csv": {
        "description": (
            "Field origins and sitemap modification time. source_lastmod is not a launch date."
        ),
        "columns": [
            ["slug", "VARCHAR"],
            ["tagline_source", "VARCHAR"],
            ["description_source", "VARCHAR"],
            ["source_lastmod", "TIMESTAMP WITH TIME ZONE"],
        ],
        "query": """
            SELECT slug, tagline_source, description_source, source_lastmod
            FROM products
            WHERE status = 'fetched'
            ORDER BY slug
        """,
    },
}


def _escaped(path: Path) -> str:
    return str(path.resolve()).replace("'", "''")


def _normalized_tar_info(info: tarfile.TarInfo) -> tarfile.TarInfo:
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mtime = 0
    return info


def export_compact_archive(
    database: CatalogDatabase,
    output: Path,
    *,
    compression_level: int = 22,
) -> dict[str, object]:
    """Write all useful catalogue data as a maximally compressed, portable archive."""
    if not 1 <= compression_level <= 22:
        raise ValueError("compression level must be between 1 and 22")
    zstd = shutil.which("zstd")
    if zstd is None:
        raise RuntimeError("zstd is required; install it with `brew install zstd`")

    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    partial = output.with_name(f".{output.name}.partial")
    partial.unlink(missing_ok=True)
    row_counts: dict[str, int] = {}

    try:
        with tempfile.TemporaryDirectory(prefix="ph-catalog-compact-") as temporary:
            staging = Path(temporary)
            for filename, spec in TABLES.items():
                query = str(spec["query"])
                destination = staging / filename
                database.connection.execute(
                    f"COPY ({query}) TO '{_escaped(destination)}' (FORMAT CSV, HEADER TRUE)"
                )  # noqa: S608 - static query and escaped local path
                row_counts[filename] = int(
                    database.connection.execute(f"SELECT count(*) FROM ({query})").fetchone()[0]
                )

            schema = {
                "format_version": 1,
                "generated_at": datetime.now(UTC).isoformat(),
                "compression": f"zstd-ultra-{compression_level}",
                "notes": [
                    "Only complete active products are included.",
                    "CSV nulls and empty strings use DuckDB's standard CSV encoding.",
                    "Product Hunt categories are unmodified.",
                    "No field in this archive is a verified launch date.",
                ],
                "tables": {
                    filename: {
                        "description": spec["description"],
                        "columns": spec["columns"],
                        "rows": row_counts[filename],
                    }
                    for filename, spec in TABLES.items()
                },
            }
            (staging / "schema.json").write_bytes(orjson.dumps(schema, option=orjson.OPT_INDENT_2))

            command = [
                zstd,
                "--ultra",
                f"-{compression_level}",
                "-T1",
                "-q",
                "-f",
                "-o",
                str(partial),
            ]
            process = subprocess.Popen(command, stdin=subprocess.PIPE)  # noqa: S603
            if process.stdin is None:  # pragma: no cover - guaranteed by PIPE
                raise RuntimeError("failed to open zstd input stream")
            try:
                with tarfile.open(fileobj=process.stdin, mode="w|") as archive:
                    for filename in [*TABLES, "schema.json"]:
                        archive.add(
                            staging / filename,
                            arcname=filename,
                            recursive=False,
                            filter=_normalized_tar_info,
                        )
            finally:
                process.stdin.close()
            return_code = process.wait()
            if return_code != 0:
                raise RuntimeError(f"zstd exited with status {return_code}")
        partial.replace(output)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise

    return {
        "output": str(output),
        "bytes": output.stat().st_size,
        "compression_level": compression_level,
        "tables": row_counts,
    }
