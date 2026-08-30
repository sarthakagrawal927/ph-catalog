import orjson

from ph_catalog.archive_recovery import WaybackPostRecovery, WaybackRecovery
from ph_catalog.models import ProductData


def test_archive_recovery_requires_every_exported_text_field() -> None:
    complete = ProductData(
        slug="example",
        producthunt_url="https://www.producthunt.com/products/example",
        name="Example",
        tagline="Archived tagline",
        description="Archived description",
        website_url="https://example.com",
        categories=[],
    )
    missing_website = ProductData(
        slug="example",
        producthunt_url="https://www.producthunt.com/products/example",
        name="Example",
        tagline="Archived tagline",
        description="Archived description",
    )

    assert WaybackRecovery._has_all_fields(complete)
    assert not WaybackRecovery._has_all_fields(missing_website)


def test_archived_post_requires_a_unique_product_link() -> None:
    html = b"""
    <a href="/products/canonical">Product</a>
    <script>const url = "https://www.producthunt.com/products/canonical";</script>
    """
    assert WaybackPostRecovery._product_slugs(html) == ["canonical"]

    ambiguous = html + b'<a href="/products/related">Related</a>'
    assert WaybackPostRecovery._product_slugs(ambiguous) == ["canonical", "related"]


def test_legacy_post_requires_explicit_product_and_website_references() -> None:
    payload = {
        "props": {
            "apollo": {
                "Post7": {
                    "slug": "demo-launch",
                    "name": "Demo",
                    "tagline": "A careful demo",
                    "description": "The full description",
                    "product": {"id": "Product8", "typename": "Product"},
                    "productLinks": [
                        {"id": "ProductLink9", "typename": "ProductLink"}
                    ],
                    "topics": {"id": "$Post7.topics", "typename": "TopicConnection"},
                    "__typename": "Post",
                },
                "Product8": {
                    "slug": "demo",
                    "visible": False,
                    "__typename": "Product",
                },
                "ProductLink9": {
                    "storeName": "Website",
                    "websiteName": "demo.example",
                    "__typename": "ProductLink",
                },
                "$Post7.topics": {
                    "edges": [{"id": "$Post7.topics.edges.0"}],
                },
                "$Post7.topics.edges.0": {"node": {"id": "Topic10"}},
                "Topic10": {"name": "Developer Tools", "__typename": "Topic"},
            }
        }
    }
    html = (
        b'<html><head><meta property="og:title" content="Demo: A careful demo">'
        b'<meta property="og:description" content="The full description"></head>'
        b'<body><h1>Demo</h1><script>' + orjson.dumps(payload) + b"</script></body></html>"
    )

    data = WaybackPostRecovery._legacy_product_data(html, "demo-launch")

    assert data is not None
    assert data.slug == "demo"
    assert data.website_url == "https://demo.example"
    assert data.categories == ["Developer Tools"]
    assert WaybackPostRecovery._legacy_product_data(html, "other-launch") is None
