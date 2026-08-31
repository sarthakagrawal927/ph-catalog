# Catalogue analytics and enrichment plan

## Objective

Deliver useful internal analytics and a browsable catalogue immediately, while
incrementally improving one `primary_category` and a unified `labels` array.
Expensive network or model work must be routed only to uncertain products.

## Canonical analytical shape

```json
{
  "slug": "example-product",
  "primary_category": {
    "value": "developer_tools",
    "confidence": 0.91,
    "method": "local_classifier",
    "taxonomy_version": "v1"
  },
  "labels": [
    {
      "value": "python",
      "kind": "technology",
      "confidence": 0.95,
      "method": "gliner_small_v2_1",
      "evidence": "Built with Python"
    }
  ],
  "source_categories": [
    {"value": "Developer Tools", "source": "producthunt"}
  ]
}
```

Rules:

- `primary_category` is outside `labels` and is null when evidence is weak.
- A product has no more than one primary category per taxonomy version.
- Tags and NER entities share `labels`; `kind` preserves their semantics.
- Original Product Hunt categories are unchanged source evidence.
- Every derived value retains method, confidence, taxonomy version, and optional
  evidence.

## Stage 0 — Existing-data product

Status: implemented as a reproducible DuckDB analytics mart.

Ship without new crawling or model inference:

- Catalogue composition from 574,752 complete products.
- Product Hunt category analytics for 93,184 categorized products.
- Existing trusted label analytics for 247,788 products.
- 709,644 product-to-launch relationships and relaunch multiplicity.
- Domain/host concentration and distribution-channel analysis.
- Text clusters, keyword prevalence, boilerplate, spam, and anomaly detection.
- Search/category/collection discovery-surface coverage.
- Static browsing pages and individual product details.

Build the internal mart from the rich crawler database and the audited trusted
label file:

```bash
uv run python tools/build_analytics.py \
  --catalog-db data/producthunt.duckdb \
  --trusted-labels data/analytics/product-tags-v1-trusted.parquet \
  --output data/analytics/catalog-analytics.duckdb \
  --summary-output data/analytics/catalog-analytics-summary.json
```

After the full NER output passes a fresh audit, include it without changing the
base catalogue by adding:

```text
--entities data/ner-v1/product-entities.parquet
```

The mart includes `product_facts`, normalized category/label/entity/launch
tables, domain and relaunch summaries, co-occurrences, and 20 relative launch
cohorts. `label_relative_trends` compares the earliest and latest quarter of
products by first post ID and reports both raw catalogue share and share among
products that received any trusted label, controlling for changing label
coverage. It is useful for directional exploration but is explicitly not a
calendar-time series.

Boundaries:

- The portable archive has no verified launch date.
- Product and post IDs provide relative order, not calendar dates.
- Votes and comment counts are not present.

## Stage 1 — Freeze a primary taxonomy

Define approximately 12–18 mutually exclusive top-level market categories.
Map the 540 original Product Hunt category values into that taxonomy. Review the
mapping independently before it becomes training data. Keep `uncategorized` as
a presentation value while storing genuine uncertainty as null.

Acceptance gate:

- Every mapped source category has a documented rationale.
- Ambiguous Product Hunt categories can remain unmapped.
- The taxonomy is versioned and frozen before model training.

## Stage 2 — Primary-category classifier

Train a local multiclass text classifier using name, tagline, and description
from the approximately 93,000 source-categorized products. Use deterministic
train/validation/test splits by slug. Calibrate confidence and class-margin
thresholds and abstain below them.

Outputs:

```text
slug, primary_category, confidence, runner_up, margin,
method, taxonomy_version, evidence_hash
```

Acceptance gate:

- Report macro precision/recall and per-category confusion.
- Audit a fresh random sample and a stratified sample per category.
- Do not infer a primary category merely from implementation technology.

## Stage 3 — Concrete local NER labels

Status: implemented and benchmarked.

The Apache-2.0 GLiNER Small v2.1 pipeline extracts concrete evidence for
technology, platform mentions, operating system, file format, hardware,
audience, and explicit industry phrases. It writes atomic 10,000-product shards
and resumes by skipping complete shards.

```bash
uv sync --extra ner

uv run --extra ner python tools/ner_enrich.py \
  --source producthunt-full.parquet \
  --output-dir data/ner-v1 \
  --device auto --batch-size 32 --shard-size 10000

uv run --extra ner python tools/ner_filter.py \
  --parts-dir data/ner-v1/parts \
  --output data/ner-v1/product-entities-v3.parquet \
  --audit-output data/ner-v1/audit-sample.json

uv run python tools/ner_audit_sample.py \
  --entities data/ner-v1/product-entities-v3.parquet \
  --products producthunt-full.parquet \
  --output data/ner-v1/stratified-audit.json \
  --per-type 10 --rare-per-type 5
```

The audit generator samples each retained entity type independently, adds a
separate low-support tail stratum, and embeds the relevant product text. Fill
its verdict fields manually; do not tune rules on that sample and then reuse it
as final quality evidence.

The completed run processed 572,993 products with at least 40 characters of
tagline/description text. It produced 58 atomic raw shards and 278,610 raw
candidates. Frozen precision filter v3 retained 115,504 assignments for 75,713
products. The untouched 100-assignment final sample included 10 random
assignments per entity type plus up to five assignments with support of five or
fewer products. Manual review found 97/100 correct types and 100/100 relevant
mentions. Hardware was the weakest type at 13/15; the three failures remain in
the audit evidence and were not used for post-audit tuning.

## Stage 4 — Confidence routing

Combine classifier confidence, disagreement, text-quality flags, and NER
evidence. Accept high-confidence products immediately. Route only products with
one or more of these conditions:

- No primary category above threshold.
- Small margin between the two most likely categories.
- Product Hunt tagline and description contradict one another.
- Boilerplate, placeholder, spam, or unusually short copy.
- No useful labels or a strong label/category conflict.
- A destination type known to carry better structured metadata.

## Stage 5 — Selective website evidence

Fetch only public pages for routed products. Respect robots directives,
Retry-After, persistent denial, and the existing global request cap. Do not
download media or bypass access controls.

Extract, in order:

1. JSON-LD Product, SoftwareApplication, and Organization fields.
2. Open Graph title and description.
3. Standard title and meta description.
4. Main heading and short feature/value-proposition sections.
5. At most one obvious about, features, or product page when justified.

Store evidence text, URL, field type, fetch time, content hash, and outcome.
Never overwrite Product Hunt source text; reclassify from a combined evidence
view.

## Stage 6 — Local 8B/27B fallback

Run only on products still unresolved after website enrichment. Batch multiple
products per prompt and require schema-constrained output. Do not store verbose
reasoning. Store the selected category, labels, confidence, short cited evidence
spans, model identifier, prompt version, and input hash.

Use the larger model for:

- Primary-category arbitration.
- Abstract themes and use cases that NER cannot extract.
- Resolving conflicts between Product Hunt and website evidence.
- Proposing labels from the frozen vocabulary.

The model must be allowed to return null/unknown.

## Stage 7 — Validation and promotion

Every taxonomy/model/prompt version produces immutable candidate outputs. It is
promoted only after:

- A new deterministic random audit not used for tuning.
- A stratified audit by primary category and confidence band.
- Separate checks for source-only, website-enriched, and LLM-fallback records.
- Confusion/error analysis and documented exclusions.
- Coverage and precision reported independently.

Do not tune on an audit sample and then reuse it as evidence for accuracy.

## Distribution to another machine

The private GitHub Release `catalog-state` already contains:

```text
ph-catalog-full.tar.zst
producthunt-full.parquet
```

Fresh-machine setup:

```bash
git clone https://github.com/sarthakagrawal927/ph-catalog
cd ph-catalog
gh release download catalog-state -p producthunt-full.parquet
uv sync --extra ner
```

Generated datasets stay in Release assets or local storage, not Git history.
Source code, schemas, validation reports, and the plan remain version-controlled.
