from ph_catalog.product_slug_api import ProductSlugResolver


def test_product_slug_query_escapes_slugs() -> None:
    query = ProductSlugResolver._query(['plain', 'quote"slug'])
    assert 'p0:product(slug:"plain")' in query
    assert 'p1:product(slug:"quote\\"slug")' in query
    assert "categories{name}" in query
