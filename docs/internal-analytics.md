# Internal catalogue analytics

The generated `catalog-analytics.duckdb` is a standalone exploratory mart. It
does not modify the crawler database. Rebuilding it replaces the previous mart
atomically.

## Current baseline

The first full build on August 31, 2026 contains:

- 574,752 unique products, all with name, tagline, and description.
- 517,853 products with an external website, spanning 406,208 exact hosts.
- 93,184 products with 199,654 original Product Hunt category assignments.
- 247,788 products with 281,228 audited broad-label assignments.
- 547,763 products connected to 709,323 launch records; 54,558 products have
  more than one mapped launch.
- 20 equal-sized relative launch-order cohorts for 547,725 products whose first
  mapped post has a numeric ID.

This baseline intentionally has no NER entities. Add them only after the full
entity run passes a fresh audit, then rebuild with `--entities`.

## Tables

`product_facts` is the main one-row-per-product table. It retains the catalogue
text and URLs and adds exact website host, source-category count, Product Hunt
ID range, launch count and post-ID range, trusted-label summary, provenance,
text lengths, and coverage flags.

Normalized evidence tables:

- `product_categories`
- `product_labels`
- `product_entities`
- `product_launches`
- `product_ids`
- `product_relative_launch_cohorts`

Ready-to-query aggregates:

- `category_stats`, `label_stats`, and `entity_stats`
- `domain_stats` and `provenance_stats`
- `launch_multiplicity_stats`
- `category_cooccurrence` and `label_cooccurrence`
- `relative_launch_cohort_stats` and `relative_launch_cohort_labels`
- `label_relative_trends`

## Initial signals

- External destinations are highly diverse: 397,503 of 406,208 hosts occur for
  exactly one product. The four largest distribution hosts—Apple App Store
  current and legacy domains, Google Play, and GitHub—account for 55,493
  products.
- 493,205 products have one mapped launch, 54,558 have multiple mapped launches,
  and 26,989 have none. A small number of umbrella products such as Google and
  Apple map to hundreds of posts, so average launch count should not be treated
  as a typical-product statistic without capping or stratifying.
- The most frequent trusted broad labels are AI/ML (41,710), design/creative
  (37,413), health/wellness (36,073), and finance/fintech (32,688).
- Strong label intersections include AI/ML with developer tools (6,269
  products), AI/ML with design/creative (4,682), and crypto/Web3 with
  finance/fintech (4,066).
- After normalizing for changing model-label coverage, AI/ML represents 1.32%
  of labels in the earliest launch-order quarter versus 23.35% in the latest.
  Developer tools move from 5.74% to 11.23%. These are strong directional
  signals, but they are not calendar growth rates and may still contain
  taxonomy or source-coverage effects.

## Example queries

Open the mart:

```bash
duckdb data/analytics/catalog-analytics.duckdb
```

Largest original Product Hunt categories:

```sql
SELECT category, products, round(catalogue_share * 100, 2) AS catalogue_percent
FROM category_stats
ORDER BY products DESC
LIMIT 25;
```

Broad-label intersections:

```sql
SELECT label_a, label_b, products
FROM label_cooccurrence
ORDER BY products DESC
LIMIT 25;
```

Coverage-normalized movement through relative launch order:

```sql
SELECT label,
       round(early_labeled_share * 100, 2) AS early_percent,
       round(recent_labeled_share * 100, 2) AS recent_percent,
       round(coverage_normalized_growth_index, 2) AS relative_index
FROM label_relative_trends
ORDER BY labeled_share_change DESC;
```

Categories with frequent relaunching, using a minimum sample size:

```sql
SELECT category, products,
       round(relaunch_rate * 100, 1) AS relaunch_percent,
       round(average_launches, 2) AS average_launches
FROM category_stats
WHERE products >= 500
ORDER BY relaunch_rate DESC
LIMIT 25;
```

Products for a destination host:

```sql
SELECT slug, name, tagline, launch_count, top_trusted_label
FROM product_facts
WHERE website_host = 'github.com'
ORDER BY launch_count DESC, name;
```

## Interpretation boundaries

- `source_lastmod`, `first_seen_at`, and `fetched_at` are crawler timestamps,
  not launch dates.
- Post IDs are used only as a relative ordering signal. Cohorts do not represent
  equal time periods.
- Recent products have had less opportunity to relaunch, so relaunch rates must
  not be compared across cohorts as if observation windows were equal.
- Trusted labels are precision-oriented exploratory predictions. They do not
  yet constitute the single `primary_category` field.
- Votes and comment counts are absent, so the mart measures supply and catalogue
  structure—not popularity or commercial success.
