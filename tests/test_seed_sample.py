from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from ph_catalog.database import CatalogDatabase
from ph_catalog.models import SitemapProduct

ROOT = Path(__file__).resolve().parents[1]
SEED_SAMPLE_PATH = ROOT / "tools" / "seed_sample.py"


def _load_seed_module():
    spec = importlib.util.spec_from_file_location("seed_sample", SEED_SAMPLE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["seed_sample"] = module
    spec.loader.exec_module(module)
    return module


def test_seed_sample_loads_synthetic_fixtures_offline(tmp_path: Path) -> None:
    seed_sample = _load_seed_module()
    database_path = tmp_path / "catalog.duckdb"

    report = seed_sample.seed(database_path, ROOT / "samples")

    assert report["fixtures"] == 3
    assert report["inserted"] == 3
    assert report["parsed"] == 3
    slugs = {product["slug"] for product in report["products"]}
    assert slugs == {"acme-toolkit", "pixelboard", "quantify"}

    with CatalogDatabase(database_path) as database:
        counts = database.counts()
        assert counts["fetched"] == 3
        verify = database.verify()
        assert verify["duplicates"] == {"slug": 0, "producthunt_url": 0}
        assert verify["fetched_completeness"] == 1.0
        assert verify["reachable_parse_success"] == 1.0


def test_seed_sample_is_idempotent(tmp_path: Path) -> None:
    seed_sample = _load_seed_module()
    database_path = tmp_path / "catalog.duckdb"

    first = seed_sample.seed(database_path, ROOT / "samples")
    second = seed_sample.seed(database_path, ROOT / "samples")

    assert first["parsed"] == 3
    # Re-running re-imports the same sitemap slugs (deduplicated) and re-claims
    # the already-fetched rows, which apply_results no-ops back to fetched.
    assert second["inserted"] == 0
    with CatalogDatabase(database_path) as database:
        assert database.counts()["fetched"] == 3


def test_seed_refuses_existing_owner_catalog_without_mutating_it(tmp_path: Path) -> None:
    seed_sample = _load_seed_module()
    database_path = tmp_path / "owner.duckdb"
    with CatalogDatabase(database_path) as database:
        database.import_products([SitemapProduct("owner-product", "https://example.com/owner")])
    before = database_path.read_bytes()
    with pytest.raises(ValueError, match="non-sample catalogue"):
        seed_sample.seed(database_path, ROOT / "samples")
    assert database_path.read_bytes() == before
