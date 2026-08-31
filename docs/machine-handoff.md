# Machine handoff

The private GitHub release
[`catalog-handoff-v1`](https://github.com/sarthakagrawal927/ph-catalog/releases/tag/catalog-handoff-v1)
is the immutable portable boundary. Generated data is kept out of Git history
and uploaded as release assets. The separate `catalog-state` release remains
mutable for the daily incremental workflow.

## Download

```bash
git clone https://github.com/sarthakagrawal927/ph-catalog
cd ph-catalog
mkdir -p data/release
gh release download catalog-handoff-v1 --dir data/release

uv sync
uv run python tools/verify_handoff.py \
  --manifest data/release/handoff-manifest.json \
  --directory data/release
```

## Asset roles

- `ph-catalog-full.tar.zst`: smallest complete portable catalogue archive,
  including products, Product Hunt IDs, launch relations, aliases, and field
  provenance.
- `producthunt-full.parquet`: seven-field product table for direct analytical
  or model use.
- `catalog-analytics.duckdb.zst`: standalone internal analytics mart. Decompress
  it with `unzstd` before opening it in DuckDB.
- `catalog-analytics-summary.json`: compact metrics and initial trend report.
- `product-tags-v1.parquet`: all broad-label candidates.
- `product-tags-v1-trusted.parquet`: audited precision-oriented subset used by
  the analytics mart.
- `product-entities-v3.parquet`: filtered GLiNER entity assignments accepted by
  precision filter v3.
- `gliner-raw-v1.tar.zst`: resumable raw inference shards. Retain these so new
  filters can be tested without rerunning the model. It contains the portable
  inference manifest and the complete `parts/` directory; model weights are not
  bundled.
- `ner-final-audit-v3.json`: fresh type-stratified manual validation evidence.
- `tagging-*` and `website-audit-*`: broad-label model and validation evidence.
- `handoff-manifest.json`: byte sizes and SHA-256 checksums for every other
  release asset.

## Open the analytics database

```bash
unzstd data/release/catalog-analytics.duckdb.zst \
  -o data/catalog-analytics.duckdb
duckdb data/catalog-analytics.duckdb
```

Useful starting queries are in [`internal-analytics.md`](internal-analytics.md).
The database is exploratory: it keeps Product Hunt categories unchanged,
stores broad labels and concrete entities separately, and does not invent launch
dates, votes, comments, or a primary category.

## Rebuild derived data

The raw GLiNER shards allow filter changes without expensive inference:

```bash
mkdir -p data/ner-v1
unzstd -c data/release/gliner-raw-v1.tar.zst | tar -xf - -C data/ner-v1

uv sync --extra ner
uv run --extra ner python tools/ner_filter.py \
  --parts-dir data/ner-v1/parts \
  --output data/product-entities-v3.parquet
```

To rebuild the analytics mart, use the command documented in
[`analytics-pipeline-plan.md`](analytics-pipeline-plan.md) and pass the entity
Parquet file through `--entities`.
