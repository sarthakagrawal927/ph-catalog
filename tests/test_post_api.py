from ph_catalog.post_api import PostIdScanner, PostResolution, PostResolver


def test_post_query_escapes_slugs() -> None:
    query = PostResolver._query(['plain', 'quote"slug'])
    assert 'p0:post(slug:"plain")' in query
    assert 'p1:post(slug:"quote\\"slug")' in query


def test_post_resolutions_cover_null_posts_and_products() -> None:
    data = {
        "p0": None,
        "p1": {"id": "11", "slug": "launch", "product": None},
        "p2": {
            "id": "12",
            "slug": "launch-2",
            "product": {"id": "99", "slug": "Canonical"},
        },
    }
    rows = PostResolver._resolutions(["gone", "orphan", "renamed"], data)
    assert [row.outcome for row in rows] == ["unavailable", "no_product", "resolved"]
    assert rows[2].product_id == 99
    assert rows[2].product_slug == "canonical"


def test_post_id_scan_rejects_numeric_slug_collisions() -> None:
    rows = [
        PostResolution("100", "resolved", 100, 1, "exact"),
        PostResolution("101", "resolved", 999, 2, "numeric-slug-collision"),
        PostResolution("102", "unavailable"),
    ]

    exact = PostIdScanner._exact_resolutions(100, rows)

    assert [row.outcome for row in exact] == ["resolved", "unavailable", "unavailable"]
    assert exact[1].post_id is None
