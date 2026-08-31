"""Build a standalone, reproducible internal analytics mart."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import orjson


def _escaped(path: Path) -> str:
    return str(path.resolve()).replace("'", "''")


def _rows_as_dicts(connection: duckdb.DuckDBPyConnection, query: str) -> list[dict[str, object]]:
    cursor = connection.execute(query)
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def build_analytics(
    catalog_database: Path,
    trusted_labels: Path,
    output_database: Path,
    summary_output: Path,
    *,
    entities: Path | None = None,
) -> dict[str, object]:
    """Materialize the catalogue's useful analytical dimensions atomically."""
    catalog_database = catalog_database.resolve()
    trusted_labels = trusted_labels.resolve()
    output_database = output_database.resolve()
    summary_output = summary_output.resolve()
    entities = entities.resolve() if entities else None

    if not catalog_database.is_file():
        raise FileNotFoundError(catalog_database)
    if not trusted_labels.is_file():
        raise FileNotFoundError(trusted_labels)
    if entities is not None and not entities.is_file():
        raise FileNotFoundError(entities)
    if output_database == catalog_database:
        raise ValueError("analytics output must not overwrite the catalogue database")

    output_database.parent.mkdir(parents=True, exist_ok=True)
    summary_output.parent.mkdir(parents=True, exist_ok=True)
    partial_database = output_database.with_name(f".{output_database.name}.partial")
    partial_summary = summary_output.with_name(f".{summary_output.name}.partial")
    partial_database.unlink(missing_ok=True)
    partial_summary.unlink(missing_ok=True)

    connection = duckdb.connect(str(partial_database))
    try:
        connection.execute(
            f"ATTACH '{_escaped(catalog_database)}' AS source_catalog (READ_ONLY)"
        )
        connection.execute(
            f"""
            CREATE TABLE product_labels AS
            SELECT slug, tag AS label, score, rank, taxonomy_version, method
            FROM read_parquet('{_escaped(trusted_labels)}')
            QUALIFY row_number() OVER (
                PARTITION BY slug, tag ORDER BY score DESC, rank, method
            ) = 1
            """
        )

        if entities is not None:
            connection.execute(
                f"""
                CREATE TABLE product_entities AS
                SELECT slug, entity_type, canonical_value, source_span, score,
                       support_products, method
                FROM read_parquet('{_escaped(entities)}')
                QUALIFY row_number() OVER (
                    PARTITION BY slug, entity_type, canonical_value
                    ORDER BY score DESC, source_span
                ) = 1
                """
            )
        else:
            connection.execute(
                """
                CREATE TABLE product_entities (
                    slug VARCHAR,
                    entity_type VARCHAR,
                    canonical_value VARCHAR,
                    source_span VARCHAR,
                    score FLOAT,
                    support_products BIGINT,
                    method VARCHAR
                )
                """
            )

        connection.execute(
            """
            CREATE TABLE product_categories AS
            SELECT DISTINCT p.slug, trim(category) AS category
            FROM source_catalog.products p,
                 unnest(coalesce(p.categories, []::VARCHAR[])) AS item(category)
            WHERE p.status = 'fetched' AND trim(category) <> ''
            """
        )
        connection.execute(
            """
            CREATE TABLE product_launches AS
            SELECT r.product_slug AS slug, r.post_slug, r.post_id, r.product_id
            FROM source_catalog.archive_post_resolutions r
            JOIN source_catalog.products p ON p.slug = r.product_slug
            WHERE p.status = 'fetched' AND r.product_slug IS NOT NULL
            """
        )
        connection.execute(
            """
            CREATE TABLE product_ids AS
            SELECT s.slug, s.product_id
            FROM source_catalog.source_products s
            JOIN source_catalog.products p USING (slug)
            WHERE p.status = 'fetched'
            """
        )

        connection.execute(
            r"""
            CREATE TABLE product_facts AS
            WITH product_id_stats AS (
                SELECT slug, min(product_id) AS first_product_id,
                       max(product_id) AS latest_product_id,
                       count(DISTINCT product_id) AS product_id_count
                FROM product_ids GROUP BY slug
            ),
            launch_stats AS (
                SELECT slug, count(*) AS launch_count,
                       min(post_id) AS first_post_id, max(post_id) AS latest_post_id
                FROM product_launches GROUP BY slug
            ),
            label_stats AS (
                SELECT slug, count(*) AS trusted_label_count,
                       first(label ORDER BY rank, score DESC, label) AS top_trusted_label,
                       first(score ORDER BY rank, score DESC, label) AS top_trusted_label_score
                FROM product_labels GROUP BY slug
            ),
            entity_stats AS (
                SELECT slug, count(*) AS entity_count
                FROM product_entities GROUP BY slug
            ),
            base AS (
                SELECT p.*,
                       lower(nullif(regexp_extract(
                           p.website_url,
                           '^(?:[A-Za-z][A-Za-z0-9+.-]*://)?(?:www\.)?([^/:?#]+)',
                           1
                       ), '')) AS website_host
                FROM source_catalog.products p
                WHERE p.status = 'fetched'
            )
            SELECT
                p.slug,
                p.name,
                p.tagline,
                p.description,
                p.website_url,
                p.website_host,
                p.producthunt_url,
                coalesce(p.categories, []::VARCHAR[]) AS source_categories,
                len(coalesce(p.categories, []::VARCHAR[]))::INTEGER AS source_category_count,
                ids.first_product_id,
                ids.latest_product_id,
                coalesce(ids.product_id_count, 0)::INTEGER AS product_id_count,
                coalesce(launches.launch_count, 0)::INTEGER AS launch_count,
                launches.first_post_id,
                launches.latest_post_id,
                coalesce(labels.trusted_label_count, 0)::INTEGER AS trusted_label_count,
                labels.top_trusted_label,
                labels.top_trusted_label_score,
                coalesce(entities.entity_count, 0)::INTEGER AS entity_count,
                length(trim(coalesce(p.name, '')))::INTEGER AS name_characters,
                length(trim(coalesce(p.tagline, '')))::INTEGER AS tagline_characters,
                length(trim(coalesce(p.description, '')))::INTEGER AS description_characters,
                p.tagline_source,
                p.description_source,
                p.source_lastmod,
                p.first_seen_at,
                p.fetched_at,
                p.content_hash,
                p.website_host IS NOT NULL
                    AND NOT ends_with(p.website_host, 'producthunt.com') AS has_external_website,
                p.name IS NOT NULL AND trim(p.name) <> '' AS has_name,
                p.tagline IS NOT NULL AND trim(p.tagline) <> '' AS has_tagline,
                p.description IS NOT NULL AND trim(p.description) <> '' AS has_description,
                len(coalesce(p.categories, []::VARCHAR[])) > 0 AS has_source_categories,
                launches.launch_count IS NOT NULL AS has_launch_mapping,
                labels.trusted_label_count IS NOT NULL AS has_trusted_label,
                entities.entity_count IS NOT NULL AS has_entity
            FROM base p
            LEFT JOIN product_id_stats ids USING (slug)
            LEFT JOIN launch_stats launches USING (slug)
            LEFT JOIN label_stats labels USING (slug)
            LEFT JOIN entity_stats entities USING (slug)
            """
        )

        connection.execute(
            """
            CREATE TABLE product_relative_launch_cohorts AS
            SELECT slug, first_post_id,
                   ntile(20) OVER (ORDER BY first_post_id, slug)::UTINYINT AS relative_cohort
            FROM product_facts
            WHERE first_post_id IS NOT NULL
            """
        )
        connection.execute(
            """
            CREATE TABLE category_stats AS
            SELECT c.category, count(*) AS products,
                   count(*) / (SELECT count(*) FROM product_facts)::DOUBLE AS catalogue_share,
                   avg(p.launch_count) AS average_launches,
                   count_if(p.launch_count > 1) / count(*)::DOUBLE AS relaunch_rate
            FROM product_categories c
            JOIN product_facts p USING (slug)
            GROUP BY c.category
            ORDER BY products DESC, c.category
            """
        )
        connection.execute(
            """
            CREATE TABLE label_stats AS
            SELECT l.label, count(*) AS products,
                   count(*) / (SELECT count(*) FROM product_facts)::DOUBLE AS catalogue_share,
                   avg(l.score) AS average_score,
                   avg(p.launch_count) AS average_launches,
                   count_if(p.launch_count > 1) / count(*)::DOUBLE AS relaunch_rate
            FROM product_labels l
            JOIN product_facts p USING (slug)
            GROUP BY l.label
            ORDER BY products DESC, l.label
            """
        )
        connection.execute(
            """
            CREATE TABLE entity_stats AS
            SELECT entity_type, canonical_value, count(*) AS products,
                   avg(score) AS average_score
            FROM product_entities
            GROUP BY entity_type, canonical_value
            ORDER BY products DESC, entity_type, canonical_value
            """
        )
        connection.execute(
            """
            CREATE TABLE entity_type_stats AS
            SELECT entity_type, count(*) AS assignments,
                   count(DISTINCT slug) AS products,
                   count(DISTINCT canonical_value) AS distinct_values,
                   avg(score) AS average_score
            FROM product_entities
            GROUP BY entity_type
            ORDER BY products DESC, entity_type
            """
        )
        connection.execute(
            """
            CREATE TABLE domain_stats AS
            SELECT website_host, count(*) AS products,
                   count(*) / (SELECT count(*) FROM product_facts)::DOUBLE AS catalogue_share,
                   sum(launch_count) AS mapped_launches
            FROM product_facts
            WHERE has_external_website
            GROUP BY website_host
            ORDER BY products DESC, website_host
            """
        )
        connection.execute(
            """
            CREATE TABLE launch_multiplicity_stats AS
            SELECT launch_count, count(*) AS products,
                   count(*) / (SELECT count(*) FROM product_facts)::DOUBLE AS catalogue_share
            FROM product_facts
            GROUP BY launch_count
            ORDER BY launch_count
            """
        )
        connection.execute(
            """
            CREATE TABLE provenance_stats AS
            SELECT tagline_source, description_source, count(*) AS products,
                   count(*) / (SELECT count(*) FROM product_facts)::DOUBLE AS catalogue_share
            FROM product_facts
            GROUP BY tagline_source, description_source
            ORDER BY products DESC, tagline_source, description_source
            """
        )
        connection.execute(
            """
            CREATE TABLE relative_launch_cohort_stats AS
            SELECT c.relative_cohort, count(*) AS products,
                   count_if(p.has_trusted_label) AS labeled_products,
                   count_if(p.has_entity) AS entity_products,
                   min(c.first_post_id) AS minimum_first_post_id,
                   max(c.first_post_id) AS maximum_first_post_id,
                   avg(p.launch_count) AS average_launches,
                   count_if(p.launch_count > 1) / count(*)::DOUBLE AS relaunch_rate,
                   count_if(p.has_source_categories) / count(*)::DOUBLE AS category_coverage,
                   count_if(p.has_trusted_label) / count(*)::DOUBLE AS trusted_label_coverage
            FROM product_relative_launch_cohorts c
            JOIN product_facts p USING (slug)
            GROUP BY c.relative_cohort
            ORDER BY c.relative_cohort
            """
        )
        connection.execute(
            """
            CREATE TABLE relative_launch_cohort_entities AS
            SELECT c.relative_cohort, e.entity_type, e.canonical_value,
                   count(*) AS products,
                   count(*) / first(s.products)::DOUBLE AS catalogue_share,
                   count(*) / first(s.entity_products)::DOUBLE AS share_among_entity_products
            FROM product_relative_launch_cohorts c
            JOIN product_entities e USING (slug)
            JOIN relative_launch_cohort_stats s USING (relative_cohort)
            GROUP BY c.relative_cohort, e.entity_type, e.canonical_value
            ORDER BY c.relative_cohort, products DESC, e.entity_type, e.canonical_value
            """
        )
        connection.execute(
            """
            CREATE TABLE relative_launch_cohort_labels AS
            SELECT c.relative_cohort, l.label, count(*) AS products,
                   count(*) / first(s.products)::DOUBLE AS catalogue_share,
                   count(*) / first(s.labeled_products)::DOUBLE AS share_among_labeled
            FROM product_relative_launch_cohorts c
            JOIN product_labels l USING (slug)
            JOIN relative_launch_cohort_stats s USING (relative_cohort)
            GROUP BY c.relative_cohort, l.label
            ORDER BY c.relative_cohort, products DESC, l.label
            """
        )
        connection.execute(
            """
            CREATE TABLE entity_relative_trends AS
            WITH sizes AS (
                SELECT
                    sum(entity_products) FILTER (WHERE relative_cohort <= 5)
                        AS early_entity_products,
                    sum(entity_products) FILTER (WHERE relative_cohort >= 16)
                        AS recent_entity_products
                FROM relative_launch_cohort_stats
            ),
            counts AS (
                SELECT e.entity_type, e.canonical_value,
                       count(*) FILTER (WHERE c.relative_cohort <= 5) AS early_products,
                       count(*) FILTER (WHERE c.relative_cohort >= 16) AS recent_products,
                       count(*) AS all_products
                FROM product_entities e
                JOIN product_relative_launch_cohorts c USING (slug)
                GROUP BY e.entity_type, e.canonical_value
            )
            SELECT entity_type, canonical_value, all_products,
                   early_products / sizes.early_entity_products::DOUBLE
                       AS early_entity_product_share,
                   recent_products / sizes.recent_entity_products::DOUBLE
                       AS recent_entity_product_share,
                   recent_entity_product_share - early_entity_product_share AS share_change,
                   CASE WHEN early_entity_product_share = 0 THEN NULL
                        ELSE recent_entity_product_share / early_entity_product_share END
                       AS coverage_normalized_growth_index
            FROM counts CROSS JOIN sizes
            WHERE all_products >= 25
            ORDER BY share_change DESC, entity_type, canonical_value
            """
        )
        connection.execute(
            """
            CREATE TABLE label_relative_trends AS
            WITH sizes AS (
                SELECT
                    sum(products) FILTER (WHERE relative_cohort <= 5) AS early_products,
                    sum(products) FILTER (WHERE relative_cohort >= 16) AS recent_products,
                    sum(labeled_products) FILTER (WHERE relative_cohort <= 5)
                        AS early_labeled_products,
                    sum(labeled_products) FILTER (WHERE relative_cohort >= 16)
                        AS recent_labeled_products
                FROM relative_launch_cohort_stats
            ),
            counts AS (
                SELECT l.label,
                       count(*) FILTER (WHERE c.relative_cohort <= 5) AS early_products,
                       count(*) FILTER (WHERE c.relative_cohort >= 16) AS recent_products
                FROM product_labels l
                JOIN product_relative_launch_cohorts c USING (slug)
                GROUP BY l.label
            )
            SELECT label, counts.early_products, counts.recent_products,
                   counts.early_products / sizes.early_products::DOUBLE
                       AS early_catalogue_share,
                   counts.recent_products / sizes.recent_products::DOUBLE
                       AS recent_catalogue_share,
                   recent_catalogue_share - early_catalogue_share AS catalogue_share_change,
                   counts.early_products / sizes.early_labeled_products::DOUBLE
                       AS early_labeled_share,
                   counts.recent_products / sizes.recent_labeled_products::DOUBLE
                       AS recent_labeled_share,
                   recent_labeled_share - early_labeled_share AS labeled_share_change,
                   CASE WHEN early_labeled_share = 0 THEN NULL
                        ELSE recent_labeled_share / early_labeled_share END
                       AS coverage_normalized_growth_index
            FROM counts CROSS JOIN sizes
            ORDER BY labeled_share_change DESC, label
            """
        )
        connection.execute(
            """
            CREATE TABLE label_cooccurrence AS
            SELECT a.label AS label_a, b.label AS label_b, count(*) AS products
            FROM product_labels a
            JOIN product_labels b ON a.slug = b.slug AND a.label < b.label
            GROUP BY a.label, b.label
            ORDER BY products DESC, label_a, label_b
            """
        )
        connection.execute(
            """
            CREATE TABLE category_cooccurrence AS
            SELECT a.category AS category_a, b.category AS category_b, count(*) AS products
            FROM product_categories a
            JOIN product_categories b ON a.slug = b.slug AND a.category < b.category
            GROUP BY a.category, b.category
            ORDER BY products DESC, category_a, category_b
            """
        )
        connection.execute(
            """
            CREATE TABLE entity_cooccurrence AS
            SELECT a.entity_type AS entity_type_a, a.canonical_value AS value_a,
                   b.entity_type AS entity_type_b, b.canonical_value AS value_b,
                   count(*) AS products
            FROM product_entities a
            JOIN product_entities b
              ON a.slug = b.slug
             AND (a.entity_type, a.canonical_value) < (b.entity_type, b.canonical_value)
            GROUP BY a.entity_type, a.canonical_value, b.entity_type, b.canonical_value
            ORDER BY products DESC, entity_type_a, value_a, entity_type_b, value_b
            """
        )
        connection.execute("CREATE UNIQUE INDEX product_facts_slug ON product_facts(slug)")
        connection.execute("CREATE INDEX product_labels_slug ON product_labels(slug)")
        connection.execute("CREATE INDEX product_categories_slug ON product_categories(slug)")
        connection.execute("CREATE INDEX product_launches_slug ON product_launches(slug)")
        connection.execute("CHECKPOINT")

        cursor = connection.execute(
            """
            SELECT
                count(*) AS products,
                count_if(has_external_website) AS products_with_external_website,
                count(DISTINCT website_host) FILTER (WHERE has_external_website)
                    AS distinct_external_domains,
                count_if(has_source_categories) AS products_with_source_categories,
                sum(source_category_count) AS source_category_assignments,
                count_if(has_trusted_label) AS products_with_trusted_labels,
                sum(trusted_label_count) AS trusted_label_assignments,
                count_if(has_entity) AS products_with_entities,
                sum(entity_count) AS entity_assignments,
                count_if(has_launch_mapping) AS products_with_launches,
                sum(launch_count) AS mapped_launches,
                count_if(launch_count > 1) AS relaunched_products,
                count_if(has_name AND has_tagline AND has_description)
                    AS products_with_complete_text,
                count_if(source_lastmod IS NOT NULL) AS products_with_source_lastmod
            FROM product_facts
            """
        )
        core_columns = [item[0] for item in cursor.description]
        core_metrics = dict(zip(core_columns, cursor.fetchone(), strict=True))
        table_counts = {
            table: connection.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]
            for (table,) in connection.execute("SHOW TABLES").fetchall()
        }
        report: dict[str, object] = {
            "generated_at": datetime.now(UTC).isoformat(),
            "scope": "internal exploratory analytics",
            "important_limitations": [
                (
                    "source_lastmod, first_seen_at, and fetched_at are crawler timestamps, "
                    "not launch dates"
                ),
                (
                    "post IDs provide relative launch order only; relative cohorts are not "
                    "calendar periods"
                ),
                "trusted labels are high-precision exploratory model outputs, not ground truth",
                "primary_category has not yet been assigned",
            ],
            "inputs": {
                "catalog_database": str(catalog_database),
                "trusted_labels": str(trusted_labels),
                "entities": str(entities) if entities else None,
            },
            "core_metrics": core_metrics,
            "table_rows": table_counts,
            "top_labels": _rows_as_dicts(
                connection,
                "SELECT * FROM label_stats ORDER BY products DESC, label LIMIT 20",
            ),
            "top_source_categories": _rows_as_dicts(
                connection,
                "SELECT * FROM category_stats ORDER BY products DESC, category LIMIT 25",
            ),
            "top_domains": _rows_as_dicts(
                connection,
                "SELECT * FROM domain_stats ORDER BY products DESC, website_host LIMIT 25",
            ),
            "relative_label_trends": _rows_as_dicts(
                connection,
                """
                SELECT * FROM label_relative_trends
                ORDER BY labeled_share_change DESC, label
                """,
            ),
            "relative_entity_trends": _rows_as_dicts(
                connection,
                """
                SELECT * FROM entity_relative_trends
                ORDER BY share_change DESC, entity_type, canonical_value
                LIMIT 50
                """,
            ),
        }
        partial_summary.write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2))
    except BaseException:
        connection.close()
        partial_database.unlink(missing_ok=True)
        partial_summary.unlink(missing_ok=True)
        raise
    else:
        connection.close()

    partial_database.replace(output_database)
    partial_summary.replace(summary_output)
    report["output_database"] = str(output_database)
    report["summary_output"] = str(summary_output)
    report["output_bytes"] = output_database.stat().st_size
    return report
