"""Read-only HTTP data layer for the local catalogue analytics UI."""

from __future__ import annotations

import mimetypes
from functools import partial
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import duckdb
import orjson


class AnalyticsStore:
    """Parameterized, read-only access to the generated analytics mart."""

    def __init__(self, database: Path) -> None:
        self.database = database.resolve()
        if not self.database.is_file():
            raise FileNotFoundError(self.database)
        connection = self._connect()
        try:
            connection.execute("SELECT 1 FROM product_facts LIMIT 1")
        finally:
            connection.close()
        self._dashboard: dict[str, Any] | None = None

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.database), read_only=True)

    @staticmethod
    def _records(
        connection: duckdb.DuckDBPyConnection,
        query: str,
        parameters: list[object] | None = None,
    ) -> list[dict[str, Any]]:
        cursor = connection.execute(query, parameters or [])
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    def dashboard(self) -> dict[str, Any]:
        if self._dashboard is not None:
            return self._dashboard
        connection = self._connect()
        try:
            metrics = self._records(
                connection,
                """
                SELECT count(*) AS products,
                       count_if(has_external_website) AS external_websites,
                       count_if(has_source_categories) AS categorized_products,
                       sum(source_category_count) AS category_assignments,
                       count_if(has_trusted_label) AS labeled_products,
                       sum(trusted_label_count) AS label_assignments,
                       count_if(has_entity) AS entity_products,
                       sum(entity_count) AS entity_assignments,
                       count_if(has_launch_mapping) AS launched_products,
                       sum(launch_count) AS launch_relationships,
                       count_if(launch_count > 1) AS relaunched_products
                FROM product_facts
                """,
            )[0]
            metrics["distinct_domains"] = connection.execute(
                "SELECT count(*) FROM domain_stats"
            ).fetchone()[0]
            data = {
                "metrics": metrics,
                "labels": self._records(
                    connection,
                    """
                    SELECT label, products, catalogue_share, average_score,
                           average_launches, relaunch_rate
                    FROM label_stats ORDER BY products DESC
                    """,
                ),
                "categories": self._records(
                    connection,
                    """
                    SELECT category, products, catalogue_share, average_launches, relaunch_rate
                    FROM category_stats ORDER BY products DESC LIMIT 20
                    """,
                ),
                "entity_types": self._records(
                    connection,
                    """
                    SELECT entity_type, assignments, products, distinct_values, average_score
                    FROM entity_type_stats ORDER BY products DESC
                    """,
                ),
                "entities": self._records(
                    connection,
                    """
                    SELECT entity_type, canonical_value, products, average_score
                    FROM entity_stats ORDER BY products DESC LIMIT 30
                    """,
                ),
                "domains": self._records(
                    connection,
                    """
                    SELECT website_host, products, catalogue_share, mapped_launches
                    FROM domain_stats ORDER BY products DESC LIMIT 15
                    """,
                ),
                "cohorts": self._records(
                    connection,
                    """
                    SELECT relative_cohort, products, labeled_products, entity_products,
                           average_launches, relaunch_rate, category_coverage,
                           trusted_label_coverage
                    FROM relative_launch_cohort_stats ORDER BY relative_cohort
                    """,
                ),
                "label_trends": self._records(
                    connection,
                    """
                    SELECT label, early_products, recent_products, early_labeled_share,
                           recent_labeled_share, labeled_share_change,
                           coverage_normalized_growth_index
                    FROM label_relative_trends ORDER BY labeled_share_change DESC
                    """,
                ),
                "entity_risers": self._records(
                    connection,
                    """
                    SELECT entity_type, canonical_value, all_products,
                           early_entity_product_share, recent_entity_product_share,
                           share_change, coverage_normalized_growth_index
                    FROM entity_relative_trends
                    WHERE all_products >= 100
                    ORDER BY share_change DESC LIMIT 18
                    """,
                ),
                "entity_fallers": self._records(
                    connection,
                    """
                    SELECT entity_type, canonical_value, all_products,
                           early_entity_product_share, recent_entity_product_share,
                           share_change, coverage_normalized_growth_index
                    FROM entity_relative_trends
                    WHERE all_products >= 100
                    ORDER BY share_change ASC LIMIT 18
                    """,
                ),
                "label_pairs": self._records(
                    connection,
                    """
                    SELECT label_a, label_b, products
                    FROM label_cooccurrence ORDER BY products DESC LIMIT 15
                    """,
                ),
            }
        finally:
            connection.close()
        self._dashboard = data
        return data

    def search_products(
        self,
        *,
        query: str = "",
        label: str = "",
        entity_type: str = "",
        entity: str = "",
        sort: str = "recent",
        limit: int = 24,
        offset: int = 0,
    ) -> dict[str, Any]:
        query = query.strip()[:120]
        label = label.strip()[:80]
        entity_type = entity_type.strip()[:80]
        entity = entity.strip()[:120]
        limit = max(1, min(limit, 48))
        offset = max(0, min(offset, 1_000_000))

        clauses = []
        parameters: list[object] = []
        if query:
            clauses.append(
                "(p.name ILIKE ? OR p.slug ILIKE ? OR p.tagline ILIKE ? OR p.website_host ILIKE ?)"
            )
            pattern = f"%{query}%"
            parameters.extend([pattern, pattern, pattern, pattern])
        if label:
            clauses.append(
                "EXISTS (SELECT 1 FROM product_labels l WHERE l.slug=p.slug AND l.label=?)"
            )
            parameters.append(label)
        if entity_type:
            clauses.append(
                "EXISTS (SELECT 1 FROM product_entities e WHERE e.slug=p.slug AND e.entity_type=?)"
            )
            parameters.append(entity_type)
        if entity:
            clauses.append(
                "EXISTS (SELECT 1 FROM product_entities e "
                "WHERE e.slug=p.slug AND e.canonical_value=?)"
            )
            parameters.append(entity)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        orderings = {
            "recent": "p.latest_post_id DESC NULLS LAST, p.name, p.slug",
            "relaunches": "p.launch_count DESC, p.latest_post_id DESC NULLS LAST, p.name",
            "name": "p.name, p.slug",
        }
        ordering = orderings.get(sort, orderings["recent"])
        parameters.extend([limit, offset])
        connection = self._connect()
        try:
            items = self._records(
                connection,
                f"""
                SELECT p.slug, p.name, p.tagline, p.website_host, p.producthunt_url,
                       p.source_categories, p.launch_count, p.latest_post_id,
                       p.top_trusted_label, p.top_trusted_label_score, p.entity_count,
                       count(*) OVER () AS total_matches
                FROM product_facts p
                {where}
                ORDER BY {ordering}
                LIMIT ? OFFSET ?
                """,
                parameters,
            )
        finally:
            connection.close()
        total = int(items[0].pop("total_matches")) if items else 0
        return {"total": total, "limit": limit, "offset": offset, "items": items}

    def product(self, slug: str) -> dict[str, Any] | None:
        connection = self._connect()
        try:
            rows = self._records(
                connection,
                """
                SELECT slug, name, tagline, description, website_url, website_host,
                       producthunt_url, source_categories, source_category_count,
                       first_product_id, latest_product_id, product_id_count,
                       launch_count, first_post_id, latest_post_id,
                       trusted_label_count, top_trusted_label, top_trusted_label_score,
                       entity_count, tagline_source, description_source,
                       has_external_website, has_launch_mapping
                FROM product_facts WHERE slug=?
                """,
                [slug],
            )
            if not rows:
                return None
            product = rows[0]
            product["labels"] = self._records(
                connection,
                """
                SELECT label, score, method FROM product_labels
                WHERE slug=? ORDER BY score DESC, label
                """,
                [slug],
            )
            product["entities"] = self._records(
                connection,
                """
                SELECT entity_type, canonical_value, source_span, score, support_products, method
                FROM product_entities WHERE slug=?
                ORDER BY entity_type, canonical_value
                """,
                [slug],
            )
            return product
        finally:
            connection.close()


class AnalyticsRequestHandler(SimpleHTTPRequestHandler):
    """Same-origin static and JSON server with a deliberately narrow API."""

    server: AnalyticsHTTPServer

    def end_headers(self) -> None:
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data:; "
            "connect-src 'self'; base-uri 'none'; frame-ancestors 'none'",
        )
        super().end_headers()

    def _json(self, value: object, status: HTTPStatus = HTTPStatus.OK) -> None:
        payload = orjson.dumps(value)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    @staticmethod
    def _integer(values: dict[str, list[str]], name: str, default: int) -> int:
        try:
            return int(values.get(name, [str(default)])[0])
        except ValueError:
            return default

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/health":
            products = self.server.store.dashboard()["metrics"]["products"]
            self._json({"ok": True, "products": products})
            return
        if parsed.path == "/api/dashboard":
            self._json(self.server.store.dashboard())
            return
        if parsed.path == "/api/products":
            values = parse_qs(parsed.query)
            try:
                result = self.server.store.search_products(
                    query=values.get("q", [""])[0],
                    label=values.get("label", [""])[0],
                    entity_type=values.get("entity_type", [""])[0],
                    entity=values.get("entity", [""])[0],
                    sort=values.get("sort", ["recent"])[0],
                    limit=self._integer(values, "limit", 24),
                    offset=self._integer(values, "offset", 0),
                )
            except duckdb.Error as error:
                self._json({"error": str(error)}, HTTPStatus.INTERNAL_SERVER_ERROR)
                return
            self._json(result)
            return
        prefix = "/api/products/"
        if parsed.path.startswith(prefix):
            slug = unquote(parsed.path.removeprefix(prefix)).strip()
            if not slug or "/" in slug or len(slug) > 240:
                self._json({"error": "invalid slug"}, HTTPStatus.BAD_REQUEST)
                return
            product = self.server.store.product(slug)
            if product is None:
                self._json({"error": "product not found"}, HTTPStatus.NOT_FOUND)
                return
            self._json(product)
            return
        if parsed.path.startswith("/api/"):
            self._json({"error": "endpoint not found"}, HTTPStatus.NOT_FOUND)
            return
        self.path = parsed.path
        super().do_GET()

    def guess_type(self, path: str) -> str:
        return mimetypes.guess_type(path)[0] or "application/octet-stream"

    def log_message(self, format: str, *args: object) -> None:
        print(f"analytics-ui {self.address_string()} {format % args}", flush=True)


class AnalyticsHTTPServer(ThreadingHTTPServer):
    store: AnalyticsStore
    site_directory: Path


def serve(database: Path, site_directory: Path, host: str, port: int) -> None:
    site_directory = site_directory.resolve()
    if not (site_directory / "index.html").is_file():
        raise FileNotFoundError(site_directory / "index.html")
    handler = partial(AnalyticsRequestHandler, directory=str(site_directory))
    server = AnalyticsHTTPServer((host, port), handler)
    server.store = AnalyticsStore(database)
    server.site_directory = site_directory
    print(f"analytics UI listening on http://{host}:{port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
