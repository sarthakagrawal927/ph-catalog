# Catalogue analytics UI

The dependency-free frontend is served with a small read-only Python API backed
by `catalog-analytics.duckdb`. It includes live overview metrics, label and
entity composition, domain concentration, relative launch-order trends,
search across all products, and evidence-level product detail.

Run it locally:

```bash
uv run python tools/serve_analytics.py \
  --database data/analytics/catalog-analytics.duckdb \
  --port 8765
```

Then open <http://127.0.0.1:8765>.

The service binds to loopback by default. Its API exposes only fixed,
parameterized read queries; it does not expose arbitrary SQL or database-file
downloads. For a temporary remote preview:

```bash
cloudflared tunnel --config /dev/null --url http://127.0.0.1:8765
```

The explicit empty config keeps an existing named-tunnel configuration from
leaking into the preview. Quick Tunnel URLs are public, ephemeral, and should
not be treated as permanent hosting.
