from ph_catalog.search_api import SearchScanner


def test_search_page_batch_parses_and_deduplicates_records() -> None:
    data = {
        "p0": {
            "edges": [
                {
                    "node": {
                        "id": "42",
                        "slug": "First",
                        "name": "First",
                        "tagline": "One",
                    }
                }
            ]
        },
        "p1": {
            "edges": [
                {
                    "node": {
                        "id": "42",
                        "slug": "renamed",
                        "name": "Renamed",
                        "tagline": "Two",
                    }
                }
            ]
        },
    }
    records = SearchScanner._records([1, 2], data)
    assert len(records) == 1
    assert records[0].product_id == 42
    assert records[0].slug == "renamed"
