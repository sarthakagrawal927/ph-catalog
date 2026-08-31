# Catalogue intelligence prototype

This dependency-free static prototype uses real aggregate statistics and ten
representative catalogue records. It demonstrates the intended overview,
product detail, and enrichment-plan experience without embedding the full
private dataset in Git.

Run it locally:

```bash
python3 -m http.server 8080 --directory site
```

Then open <http://localhost:8080>.

The committed product records are a display sample. The full catalogue remains
in the `catalog-state` GitHub Release as compressed Parquet and tar.zst assets.
