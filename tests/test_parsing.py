from pathlib import Path

from ph_catalog.parsing import content_hash, parse_product_page

FIXTURES = Path(__file__).parent / "fixtures"


def test_embedded_json_precedes_structured_and_metadata() -> None:
    data = parse_product_page(
        (FIXTURES / "product.html").read_bytes(),
        "fixture-product",
        "https://www.producthunt.com/products/fixture-product",
    )

    assert data.slug == "fixture-product"
    assert data.name == "Fixture Product"
    assert data.tagline == "Embedded tagline wins"
    assert data.description == "Embedded description wins"
    assert data.website_url == "https://fixture.example"
    assert data.categories == ["Developer Tools", "Productivity"]
    assert data.is_complete_enough
    assert len(content_hash(data)) == 64


def test_metadata_and_semantic_fallbacks() -> None:
    html = b"""
    <meta property="og:title" content="Fallback: Simple local tool | Product Hunt">
    <meta property="og:description" content="A useful fallback description.">
    <main><h1>Fallback</h1>
    <a data-test="visit-website-button" href="https://fallback.example">Visit</a></main>
    """
    data = parse_product_page(
        html, "fallback", "https://www.producthunt.com/products/fallback?ref=test"
    )

    assert data.name == "Fallback"
    assert data.tagline == "Simple local tool"
    assert data.description == "A useful fallback description."
    assert data.website_url == "https://fallback.example"


def test_redirected_slug_uses_final_product_slug() -> None:
    data = parse_product_page(
        (FIXTURES / "product.html").read_bytes(),
        "old-fixture",
        "https://www.producthunt.com/products/fixture-product",
    )
    assert data.slug == "fixture-product"
    assert data.producthunt_url.endswith("/fixture-product")


def test_legacy_normalized_product_without_slug_is_parsed() -> None:
    html = b"""
    <h1>Legacy Product</h1>
    <script type="application/json">
      {"Product123":{"__typename":"Product","name":"Legacy Product",
       "tagline":"Preserved legacy tagline","description":"Legacy description",
       "websiteUrl":"https://legacy.example","categories":[{"name":"Tools"}]}}
    </script>
    """

    data = parse_product_page(
        html,
        "legacy-product",
        "https://www.producthunt.com/products/legacy-product",
    )

    assert data.name == "Legacy Product"
    assert data.tagline == "Preserved legacy tagline"
    assert data.description == "Legacy description"
    assert data.website_url == "https://legacy.example"
    assert data.categories == ["Tools"]
