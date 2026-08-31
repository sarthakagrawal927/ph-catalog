from pathlib import Path

import duckdb

from ph_catalog.analytics_server import AnalyticsStore


def _analytics_fixture(path: Path) -> None:
    connection = duckdb.connect(str(path))
    connection.execute(
        """
        CREATE TABLE product_facts AS SELECT
          'alpha'::VARCHAR AS slug, 'Alpha'::VARCHAR AS name,
          'Rust analytics'::VARCHAR AS tagline,
          'A useful description'::VARCHAR AS description,
          'https://alpha.test'::VARCHAR AS website_url,
          'alpha.test'::VARCHAR AS website_host,
          'https://www.producthunt.com/products/alpha'::VARCHAR AS producthunt_url,
          ['Developer Tools']::VARCHAR[] AS source_categories,
          1::INTEGER AS source_category_count,
          10::BIGINT AS first_product_id, 10::BIGINT AS latest_product_id,
          1::INTEGER AS product_id_count, 2::INTEGER AS launch_count,
          100::BIGINT AS first_post_id, 200::BIGINT AS latest_post_id,
          1::INTEGER AS trusted_label_count,
          'developer_tools'::VARCHAR AS top_trusted_label,
          0.95::FLOAT AS top_trusted_label_score, 1::INTEGER AS entity_count,
          'website'::VARCHAR AS tagline_source,
          'website'::VARCHAR AS description_source,
          true::BOOLEAN AS has_external_website, true::BOOLEAN AS has_launch_mapping
        """
    )
    connection.execute(
        """
        CREATE TABLE product_labels AS SELECT
          'alpha'::VARCHAR AS slug, 'developer_tools'::VARCHAR AS label,
          0.95::FLOAT AS score, 'test'::VARCHAR AS method
        """
    )
    connection.execute(
        """
        CREATE TABLE product_entities AS SELECT
          'alpha'::VARCHAR AS slug, 'technology_or_tool'::VARCHAR AS entity_type,
          'rust'::VARCHAR AS canonical_value, 'Rust'::VARCHAR AS source_span,
          0.9::FLOAT AS score, 10::BIGINT AS support_products,
          'test'::VARCHAR AS method
        """
    )
    connection.close()


def test_analytics_store_searches_with_parameterized_filters(tmp_path: Path) -> None:
    database = tmp_path / "analytics.duckdb"
    _analytics_fixture(database)
    store = AnalyticsStore(database)

    result = store.search_products(
        query="Rust", label="developer_tools", entity_type="technology_or_tool"
    )

    assert result["total"] == 1
    assert result["items"][0]["slug"] == "alpha"
    assert store.search_products(query="missing")["total"] == 0


def test_analytics_store_returns_product_evidence(tmp_path: Path) -> None:
    database = tmp_path / "analytics.duckdb"
    _analytics_fixture(database)
    store = AnalyticsStore(database)

    product = store.product("alpha")

    assert product is not None
    assert product["labels"][0]["label"] == "developer_tools"
    assert product["entities"][0]["canonical_value"] == "rust"
    assert store.product("not-found") is None
