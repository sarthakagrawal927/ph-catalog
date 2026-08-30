# ph-catalog

`ph-catalog` builds one local, deduplicated catalogue of public Product Hunt
products. By default it unions Product Hunt's public product sitemap families:
`product_about`, `product_imported`, `product_alternatives`, `product_reviews`,
`product_addons`, and `product_jobs`. Supplemental subpage URLs are normalized
to their canonical `/products/{slug}` page. Product pages supply only:

```text
slug, name, tagline, description, website_url, categories, producthunt_url
```

Operational state (status, attempts, timestamps, errors, aliases, and hashes)
stays in DuckDB. HTML and media are not retained, apart from at most 10 capped
failed-parse/block fixtures under `data/failed_pages/`.

## Setup

Requires Python 3.11+ and works natively on Apple silicon.

```bash
uv sync
uv run ph-catalog init
```

The database is `data/producthunt.duckdb`. DuckDB compresses its storage, and
snapshots contain only the seven catalogue fields in Zstd-compressed Parquet.

## Full backfill

First run a small direct-connection sample and inspect `verify` before supplying
the full paid proxy pool:

```bash
uv run ph-catalog import-sitemap
uv run ph-catalog crawl --limit 1000
uv run ph-catalog verify
uv run ph-catalog status
```

As measured on August 30, 2026, the unnumbered public manifests contain 26,004
unique product slugs: the advertised `product_about` manifest contains 21,411,
while the legacy `product_imported` manifest contributes 4,519 not present
there. Product Hunt also serves unlinked numbered siblings. The default root
set includes the confirmed `product_about` shards 1–16, `product_imported`
shard 2, `product_alternatives` shards 1–6, and `product_reviews` shards 1–4.
The 16 current `product_about` shards contain roughly 728,000 raw entries but
only 214,203 unique slugs after overlapping shard generations are deduplicated.
All 33 confirmed default roots union to 228,252 unique product slugs; a repeated
import inserted zero duplicates.
[Product Hunt separately says](https://www.producthunt.com/newsletters/archive/50058-your-slack-is-day-trading)
its platform contains over one million products, but that larger search corpus
is not enumerated by its public sitemap inventory. The importer therefore
reports what it actually discovers and does not infer or fabricate missing
slugs.

That April 24, 2026 announcement links to the Product Hunt launch page rather
than a downloadable corpus. The anonymous frontend still names its former AI
route as `search.llm -> /experiments/search`, but the route now returns 404.
Arquivo.pt has seven captures of the route from June 2024 through October 2025;
the four pre-removal pages are Product Hunt `Not Authorized` shells, and their
dated route bundles were not captured. Common Crawl's May 2026 Parquet URL
index was also checked directly: its exact `com,producthunt` range contains no
Product Hunt host capture from which the April client could be recovered. The
live `productSearch` GraphQL connection is not a count substitute because both
`pagesCount` and `totalCount` are capped at 10,000.

Repeat `--sitemap-url` to replace the default manifest set with explicit roots.
Each root may itself be a recursively sharded sitemap index.

## Complete anonymous catalogue import

Product Hunt's anonymous first-party frontend exposes public products by numeric
ID and returns the seven required catalogue fields directly. The observed public
ID range reached at least 1,305,087 on August 30, 2026. `import-catalog` scans
through 1,399,999 in batches of 200, below Product Hunt's GraphQL complexity and
query size guards:

```bash
uv run ph-catalog import-catalog --workers 4 --rps 2 --id-batch-size 200
```

The numeric cursor and every batch are committed in one DuckDB transaction, so
rerunning the command resumes at the first uncommitted ID. Existing sitemap rows
are updated rather than duplicated. Complete API records are immediately marked
`fetched`; genuinely incomplete records remain `pending` for the HTML fallback.
HTTP 403/429 responses trigger the same shared backoff, concurrency reduction,
and sustained-blocking stop used by the page crawler. This anonymous direct
import does not require a developer token, proxy, browser session, or paid API.
It reports the database total every 10,000 unique products.

Use an independent cursor name to safely rescan a range that may have gained
records after the original pass:

```bash
uv run ph-catalog import-catalog \
  --scan-name product-tail-20260830 --start-id 1290000 --stop-id 1306000
```

The completed `[1, 1,400,000)` scan made 5,600 requests and returned 573,256
records at scan time. A named tail rescan later returned 14,991 live records in
`[1,290,000, 1,306,000)` and contributed 338 rows that became visible after the
original pass. A second `[1,305,000, 1,315,000)` tail refresh reconciled 83 newly
visible rows reported by the live aggregate. The resulting source table has
573,622 product-ID rows and 573,621 unique live slugs. Every ID in the empty
upper tail was checked; the
sparse ID space is not itself a product count.

The live first-party `products` connection independently reported exactly
573,622 records at the same checkpoint. It exposes at most 5,000 rows per sort
window, but direct numeric lookup has no such pagination ceiling. Every unique
slug reported by that connection is present and complete in DuckDB.

A further read-only stratified probe queried 24,000 IDs in 120 batches spread
from 1.4 million through 100 million. It returned zero products and encountered
no 403 or 429 responses. That makes a second dense numeric-ID range an unlikely
explanation for the claimed million; changing IPs would not change these null
application-level responses.

Product Hunt's native search was audited independently. The empty query, all
single letters/digits, and all 676 two-letter queries produced 185,270 result
pages and 96,713 distinct product IDs. Every one was already represented by the
numeric catalogue. Search is therefore a ranked window over the same live
product table, not an additional hidden catalogue. The scan is resumable:

```bash
uv run ph-catalog scan-search --two-character --workers 8 --rps 1
```

The 248 anonymous category feeds were also exhaustively paginated with
`liveOnly:false`, including every retired-only membership exposed by the same
first-party relation. The completed scan covered 15,220 pages, 302,010 category
memberships, and 85,970 distinct products. It found nine IDs not present at the
start of the scan; all nine were newly published products at the moving numeric
tail, not a hidden historical category corpus. The scan is crash-safe per
category and page:

```bash
uv run ph-catalog scan-categories --workers 8 --page-batch-size 25 --rps 2
```

Product Hunt's public collections form a much larger relationship surface. The
anonymous connection reported 453,628 collections during the final pass. A
crash-safe exhaustive scan committed all 4,537 root pages and separately drained
3,840 overflow pages for all 2,067 collections containing more than 100
products:

```bash
uv run ph-catalog scan-collections --workers 8 --rps 2
```

The scan observed 453,629 collection rows as the live total shifted during
pagination, 2,676,469 reported product memberships, and 137,681 distinct product
IDs. Every ID was already in DuckDB. This proves collection totals and
memberships cannot be added to the product count; they are overlapping bookmarks
over the same catalogue.

## Exhaustive numeric launch audit

Launch/post IDs are a second anonymous enumeration surface. They are not product
IDs: many are missing, orphaned, or repeat launches of one canonical product.
`scan-post-ids` accepts only an exact returned post ID, maps its canonical product,
and commits each batch and cursor atomically:

```bash
uv run ph-catalog scan-post-ids \
  --stop-id 1240000 --id-batch-size 200 --workers 8 --rps 2
uv run ph-catalog enrich-catalog --workers 1 --rps 1
```

The completed `[1, 1,240,000)` pass made 6,200 requests. It found 642,536
product-bearing launches and 597,463 unavailable, orphaned, or numeric-slug
collision IDs. The highest exact surviving post was 1,236,167, leaving a checked
empty upper tail. Despite enumerating more than a million IDs, it contributed
only 26 products absent from DuckDB: four recoverable deleted products and 22
current products created after the original product-ID scan. All 26 were enriched
to the complete seven-field schema. No proxy, IP rotation, account, paid API,
403, or 429 was involved. Progress is logged every 10,000 scanned IDs and every
10,000 catalogue products.

## Historical archive audit

Two free public web archives can first be imported without downloading archived
page bodies:

```bash
# Common Crawl monthly URL indexes; crash-safe per crawl and path family.
uv run ph-catalog import-archive --rps 0.25

# If the interactive index is throttled, read only Product Hunt's exact SURT
# range from the public CDX files. Repeat --crawl for each collection ID.
uv run ph-catalog import-archive-direct \
  --crawl CC-MAIN-2021-49 --crawl CC-MAIN-2021-43

# Audit redirects, denied responses, and missing-page URL keys independently.
# This does not treat their response bodies as successful captures.
uv run ph-catalog import-archive-direct \
  --include-non-200 --crawl CC-MAIN-2021-49 --crawl CC-MAIN-2021-43

# ArchiveTeam's full-domain Product Hunt crawls. The importer transfers only
# indexed Product Hunt URL ranges, not the multi-terabyte WARC bodies.
uv run ph-catalog import-internet-archive --workers 3

# Separately audit legacy URL keys whose archived response was not HTTP 200.
uv run ph-catalog import-internet-archive --include-non-200 --workers 3

# Internet Archive CDX, resumable every 10,000 collapsed URLs.
uv run ph-catalog import-wayback

# Portugal's independent web archive; one bounded CDX request per path family.
uv run ph-catalog import-arquivo-pt

# Separately audit Arquivo.pt URL keys whose archived response was not HTTP 200.
uv run ph-catalog import-arquivo-pt --include-non-200

# Seven additional national, university, and Archive-It CDX collections that
# independently contain Product Hunt captures.
uv run ph-catalog import-independent-cdx

# Map legacy /posts/{launch-slug} URLs to canonical product IDs.
uv run ph-catalog resolve-posts --batch-size 200 --rps 0.5

# Resolve archive-only /products/{slug} aliases and deleted pages.
uv run ph-catalog resolve-products --batch-size 100 --rps 0.5
```

Unavailable canonical rows can then be recovered from Internet Archive playback.
The recovery commands stream one page at a time through the normal parser and
discard its HTML immediately. Claims, attempts, capture timestamps, parse
outcomes, and canonical launch mappings remain resumable in DuckDB:

```bash
uv run ph-catalog recover-wayback --workers 8 --rps 2
uv run ph-catalog recover-wayback-posts --workers 8 --rps 2

# Scan exact Common Crawl CDX ranges, then stream only matched WARC records.
# Both commands resume from per-collection and per-capture DuckDB state.
uv run ph-catalog recover-common-crawl
uv run ph-catalog recover-common-crawl-posts

# Retry an incomplete page against an older capture date without touching
# successful rows.
uv run ph-catalog requeue-archive-recovery --outcome parse_failed
uv run ph-catalog recover-wayback --timestamp 20230101
```

The product-page recovery audited 1,332 syntactically plausible unavailable rows
and the combined archive recovery table now contains 385 complete products.
The launch-page recovery audited all 212
valid nonnumeric launches whose live resolver was unavailable, mapped 50 to a
single canonical product, and contributed another 27 products absent from
DuckDB. Capture-date fallbacks for 2022 and 2018 added one of those launch-page
products and confirmed the remaining pages were generic, incomplete, or
uncaptured. No archived HTML or media is retained.

A final body-level Common Crawl pass scanned all 127 collections using exact
Product Hunt host and path validation. It inspected 5,091,000 product CDX rows,
staged 21 exact WARC byte ranges, and restored 14 complete deleted products (12
through the resumable command and two during targeted validation). A matching
launch-page pass inspected 5,001,000 CDX rows and 145 captures representing 42
distinct unresolved launches. Only the archived `lawnstarter-2` page contained
an explicit Post -> Product slug plus a Post -> Website relationship and all
required fields; it safely restored canonical product `lawnstarter`. The other
legacy pages were left unresolved rather than assigned invented product slugs.

The completed Internet Archive audit, exhaustive Common Crawl sweep, archived
Product Hunt sitemaps, independent national-web-archive checks, mirror sitemaps,
and public research exports found 674,930 historical URL candidates: 335,343
product-path slugs and 339,587 legacy post/launch slugs. Across the archive and
exhaustive numeric-launch work, 709,643 launch identifiers resolved to 547,793
distinct canonical slugs. Repeat
launches are retained as provenance, never as duplicate catalogue products.
Adding the 709,643 launch identifiers to the 573,622 live Product IDs produces
an apparent total above one million only by counting repeat launches and their
canonical products as separate entities. It does not produce one million
unique product slugs.

Fourteen ArchiveTeam full-domain crawls from 2016 through 2020 exposed Product
Hunt's retired `/tech`, `/games`, `/books`, `/podcasts`, and early `/posts`
routes. Reading their aggregate CDX byte indexes transferred 229.7 MB instead
of the multi-terabyte WARC bodies and added 39,288 unique launch candidates.
Only 9,394 required a new structured lookup: 8,878 resolved to products already
in DuckDB, 484 were non-product posts, and 32 were unavailable. The probe added
historical provenance but zero new canonical product rows.

The same 14 ArchiveTeam indexes were then reread under separate
`--include-non-200` scan IDs. The pass transferred the same 229.7 MB of exact
CDX byte ranges, examined 714,061 legacy-route records, and added 677 launch
slugs. Only 18 lacked an existing resolution: 10 mapped to products already in
DuckDB and eight were unavailable. Together with the Common Crawl all-status
pass, this raised the historical union at that checkpoint to 674,298
candidates: 335,103 product paths and 339,195 launch paths, without adding a
canonical product.

Product Hunt's still-live 2021 `/sitemap.xml.gz` index supplied a separate
official legacy check. Its three S3 post shards contain 124,352 launch URL
captures and 68,084 unique launch slugs. All but one were already archive
candidates. The only new slug, `reviewside`, resolved to product ID 448142,
which was already in DuckDB. The import is recorded as a completed archive scan
and added one provenance row but no canonical product.

Arquivo.pt supplied an independent national-web-archive check outside Common
Crawl and Internet Archive. Its successful HTML index rows contained 21,523
product-path captures and 10,144 post-path captures, deduplicating to 3,648
product slugs and 1,813 launch slugs. The import added 21 product candidates and
65 launch candidates to archive provenance. Live resolution mapped 30 launches
and the `quest-7` alias to products already in DuckDB; archived playback of the
remaining captures recovered no additional canonical product.

A separate all-status Arquivo.pt pass then audited redirects, denials, and
missing pages. It examined 43,098 product-path records and 29,734 launch-path
records, adding 234 product candidates and 354 launch candidates to the global
union. Resolution produced one alias and mapped 12 launches to already known
products; the remaining usable candidates were unavailable or non-product
pages. It added no complete canonical product.

Seven additional public CDX archives from the International Web Archiving
registry were queried directly: the Icelandic, EU, US Federal Depository,
Government of Canada, Columbia University, National Library of Ireland, and
Netherlands Institute for Sound and Vision archives. Fourteen bounded scans
added six product-path candidates and 38 launch-path candidates after global
deduplication. The two genuinely pending product slugs were unavailable, and
the only newly resolved launch mapped to an existing product. A follow-up probe
of the retired `/apps`, `/tech`, `/games`, `/books`, and `/podcasts` routes found
88 unique slugs, all already present in archive provenance.

The two largest public Hugging Face datasets discoverable under Product Hunt
contain only 21,747 and 19,626 rows. The first has 17,520 unique `/products/`
slugs; its 31 slugs absent from the canonical `products` table were already
recorded redirects in `product_aliases`. The second contains product names but
no Product Hunt URLs. Neither contributes a new canonical row or supports a
million-product downloadable corpus.

Product Hunt's archived sitemap generations supplied an additional independent
check. The archived `product_about` manifest contained 183,133 unique slugs in
August 2022, 199,601 in December 2022, 206,467 in March 2023, 219,092 in June
2023, and 229,041 in September 2023. Five quarter-end snapshots unioned to
230,588 slugs. Expanding the audit to every archived numbered shard found 84
distinct parent/child captures and 233,545 unique slugs. Of the final 132 slugs
not already present as products or aliases, 10 resolved to known products and
122 were unavailable with no archived page capture. Earlier archived playback
restored four complete products.

The 2015 sitemap also exposed predecessor launch routes under `/tech`, `/games`,
and `/books`. They added 2,486 launch identifiers missing from the archive queue.
Product Hunt resolved 280 to products already in the catalogue; 23 were
non-product pages. Later archived sitemaps used `/posts`, which is already covered
by the exhaustive launch and archive scans.

The retired S3 sitemap family was also checked directly rather than inferred
from its root index. Nineteen archived numbered shards contain 895,297 URLs, but
870,164 unique entries are user profiles. The remaining routes are 15,625
`/tech` launches, 898 games, 184 books, 1,587 Ask pages, 859 events, 255 topics,
and a handful of site pages. There are zero `/products` URLs in the numbered
family. Its near-million URL count is therefore an all-entity/user count, not a
hidden million-product manifest; its launch routes are the already imported
2015 candidates described above.

A separate public launch export contains 520,038 launch rows but only 430,018
canonical product slugs after normalizing `/products/{slug}/launches/...` URLs.
It contributed 435 complete historical products absent from the live catalogue;
repeat launches were never counted as separate products. Import it with:

```bash
uv run ph-catalog import-external \
  --launches-csv data/external/producthunt_posts.csv \
  --redirects-csv data/external/kaggle/pieter79/producthunt.csv \
  --product-dump-json data/external/kaggle/alanhamlett/product_hunt_dump.json
uv run ph-catalog resolve-products --batch-size 100 --rps 0.5
uv run ph-catalog promote-external
```

The companion 598 MB Product Hunt API dump was then streamed independently.
Its 275,840 canonical Product records were almost entirely subsumed: it added
181 legacy Product Hunt ID relationships, 11 ID-backed historical aliases, and
four complete deleted products. A separate 126,401-row canonical metadata
export had one complete unknown slug, which the live lookup identified as the
existing `coresight` product and recorded as an alias. These imports are
repeatable without loading either source into memory:

```bash
uv run ph-catalog import-product-dump \
  --product-dump-json data/external/kaggle/alanhamlett/product_hunt_dump.json
uv run ph-catalog import-product-metadata \
  --metadata-csv data/external/kaggle/jessysisca/producthunt_web_archive_snapshot_metadata.csv
```

Two maker-relation audits independently paginated the profiles of the 50 most
followed historical users and the 50 most prolific historical makers. Their
1,157 and 3,351 maker launches referenced 475 and 790 distinct products;
neither cohort exposed an unknown product ID. A final 20,000-ID numeric tail
refresh contributed 105 products that had become public since the earlier
scan. At that checkpoint, the live first-party aggregate reported 573,734
products.

Two independently maintained GitHub daily archives were then audited. Ranbot's
166 CSV files contain 3,169 rows and 2,957 canonical slugs. Nbox's 119 Markdown
reports contain 70,311 launch-table rows and 61,176 canonical slugs with complete
archived fields. After exact product-and-alias filtering, 293 slugs required a
live lookup: 24 were historical aliases and 269 were confirmed removed products
that could be restored from the archived Product Hunt records. Two older Kaggle
launches were additionally restored under the documented tagline-as-description
fallback after archived short links supplied their websites. Import the GitHub
sources with:

```bash
uv run ph-catalog import-daily-archive \
  --archive-zip data/external/github/ranbot-ai-product-hunt.zip
uv run ph-catalog import-markdown-archive \
  --archive-zip data/external/github/nbox-producthunt-statistic.zip
```

A second final numeric-tail refresh contributed another 57 products that became
public during the collection and archive audit.

The anonymous `User.stacks` relation was then sampled across the full reported
9,451,772-user connection. A stratified 10,000-user sample returned 2,643 stack
memberships representing 977 distinct product IDs; every ID was already in the
catalogue. The relation is sparse and live-filtered, so an exhaustive scan would
cost roughly 94,518 root requests without evidence of historical yield. Two more
GitHub report archives were also checked: Aymeric Roucher's 76,525-launch study
duplicates an already audited export, while all 1,371 canonical product slugs in
xykong36's 2026 daily HTML reports were already known.

One overlooked Internet Archive URL class did produce a small defensible gain.
An exhaustive resumable sweep of 289,115 query-bearing product-root URLs found
151,007 distinct slugs and 11 absent from the database. Current Product Hunt
resolved three as aliases; archived structured pages restored seven complete
deleted products, and one incomplete shell remained unavailable. A separate
282,986-URL sweep of product subpages found 71,942 distinct slugs. After rejecting
two malformed keys, its 11 plausible unknowns resolved to two aliases, six
complete archived products, and three incomplete pages. These passes added 13
complete products and five aliases without retaining archived HTML.

The evidence-backed discovery result is therefore 574,918 unique public and
historical products, not one million. The copy-quality exclusion described below
quarantines 166 incomplete rows, leaving 574,752 products in the active export.

Common Crawl's interactive CDX service is strictly rate-limited. The importer
uses small pages, global pacing, retries, and crash-safe per-index commits; it
does not rotate IPs after denial. If the service refuses even its collection
manifest, leave the pending archive scans untouched and retry on the low-rate
schedule later. Common Crawl itself recommends its Parquet URL Index for heavy
bulk analysis.

The bulk audit now covers both `/products/` and `/posts/` in all 127 official
Common Crawl collections from 2013 through the current collection. A direct
importer binary-searches each public `cluster.idx`, downloads only the exact
Product Hunt SURT byte range from the underlying CDX gzip members, and commits
each crawl/path pair independently. Exact hostname validation prevents adjacent
SURT domains from entering the candidate queue. This avoided the unreliable
wildcard API and completed 254 path-family scans without downloading WARC page
bodies.

A second, separately resumable `--include-non-200` pass audited redirects,
blocks, and missing-page URL keys across the same collection inventory. It examined
1,472,674 exact-range CDX rows and added 1,328 product paths plus 5,977 launch
paths, raising the historical union to 673,621 candidates: 335,103 product paths
and 338,518 launch paths. Product resolution produced 11 aliases and 1,080
unavailable rows but no new complete canonical product. Launch resolution mapped
1,802 candidates to already catalogued products, classified 22 as non-product
posts, marked 358 unavailable, and quarantined 32 invalid slugs. A subsequent
Wayback recovery attempt respected repeated 429 responses, reduced itself to one
worker at 0.25 requests/second, stopped without IP rotation, and preserved the
remaining claims as retry work.

Filling the previously unscanned 2013–2021 collections added 5,723 historical
URLs to the candidate union. Yield flattened sharply with age: 2019 added 3,361,
2018 added 586, 2017 added 80, 2016 added zero, 2015 added 58, and 2014–2013
added zero. The final four missing 2022–2023 path-family halves added 13 URLs;
resolution produced one additional alias and no catalogue row. An initial
stratified sample of 1,200 numeric Product Hunt post IDs returned 638 surviving
posts and 609 distinct product IDs; all 609 product IDs were already in the
catalogue. The subsequent exhaustive 1.24-million-ID launch audit found only the
26 additions documented above. These independent, now-exhaustive probes do not
support Common Crawl as a source of the remaining 425,082 canonical products.

Hunted.space publishes 103 public sitemap shards containing 52,365 distinct
launch-dashboard identifiers. It contributed 49,888 launch slugs absent from
the existing archive queue. Product Hunt resolved 49,503 of those identifiers
to canonical products (with 22 non-product posts and 27 unavailable), but every
resolved product ID was already present in DuckDB. A CC BY 4.0 Zenodo Product
Hunt graveyard dataset supplied another 2,291 deleted/dead-site slugs; all 2,291
were also already present. A separately archived Data.world research table
supplied 18,129 exact Product Hunt post IDs: 18,081 resolved to 15,829 distinct
products, 45 had no product, and three were unavailable. That table exposed
`coverr`, the one canonical product missed by the direct ID scan. Finally,
Wayback and representative Common Crawl indexes contained no retired v1 Product
Hunt API JSON responses—only API documentation pages.

Product Hunt's newsletter claim of “over one million products” is genuine but
is not an enumerable claim in the current public site. The refreshed 33-root
canonical sitemap inventory deduplicates to 228,252 slugs; every archived
numbered generation unions to 233,545; and the live anonymous product table
exposed 573,622 records representing 573,621 unique slugs at that checkpoint.
Native search adds zero IDs, while free historical sources and the final live
tail refresh raise the verified discovery total to 574,918
rather than exposing another 425,082 usable
product pages. Product IDs extend past 1.3 million, but removed/unpublished IDs
return `null` and provide none of the seven required fields.
The catalogue records measured public entities and never fabricates rows to hit
a marketing total.

A final anonymous GraphQL search audit partitioned every two-letter prefix for
both products and launches. The Product model produced 349,958 ranked rows but
only 45,027 distinct product IDs; the Post model produced 521,908 rows and
127,800 distinct launches. Neither exposed an unknown product ID. Exhaustive
two-letter scans of all 16,911 searchable discussion threads and all 451
editorial anthology stories checked their product-forum and product-mention
relationships: 89 and 222 distinct product references respectively, all
already catalogued. A separate 20,000-row sample of the 105,540 Artificial
Intelligence topic memberships yielded 19,356 distinct IDs and zero unknowns;
topic totals are overlapping memberships, not additional canonical products.

For the primary full backfill, keep proxy credentials in an ignored local file,
one URL per line:

```bash
uv run ph-catalog backfill \
  --workers 4 \
  --rps 2 \
  --batch-size 250 \
  --proxy-file proxies.txt \
  --proxy-rotation 150
```

The global limiter applies across the entire proxy pool. Proxy selection is
sticky for 150 requests by default and rotates only on that fixed schedule or
after a transport failure. HTTP 403/429 does not rotate the endpoint: it reduces
concurrency, honors `Retry-After`, applies shared backoff, and pauses the run
after persistent blocking. No CAPTCHA bypass is implemented.

Every error is recorded in DuckDB and `logs/crawl.jsonl`. A full progress event
is printed and logged every **10,000 processed products**, plus at crawl start
and finish. A Zstd checkpoint snapshot is also exported every 10,000 products;
change it with `--snapshot-every`. Proxy URLs and credentials are never logged.

The 2 RPS default follows the backfill PRD. A live direct-IP sample encountered
intermittent 429 responses at that rate, while 1 RPS completed cleanly, so use
`--rps 1` without a proxy pool. Any 403/429 now also halves the global rate and
the crawler restores it gradually after sustained successful responses.

The crawler increments `attempts` before network I/O and writes results in
100–500 row transactions. A crash therefore leaves unfinished rows as `retry`,
while `fetched` products are excluded from future queue claims. Retry delay is
exponential and capped at one hour.

## Redirects and failures

Redirected slugs move to `product_aliases`; only the canonical slug remains in
`products`. Re-importing the sitemap does not recreate known aliases.

```bash
# Retry parser failures after improving the parser.
uv run ph-catalog requeue --status parse_failed --reset-attempts

# Produce a snapshot at any time.
uv run ph-catalog snapshot
```

`unavailable` is terminal for HTTP 404/410 and other explicit client failures.
Transport errors, 5xx, 403, and 429 remain `retry`; parser failures retain the
distinct `parse_failed` status but stay eligible after exponential delay.
`max_attempts` prevents a tight infinite loop until an operator explicitly
requeues with reset attempts.

## Website-copy enrichment

The fetched catalogue initially contained 195 blank taglines and 11 blank
descriptions across 198 products. Of those rows, 130 point to Product Hunt's
deleted/offline sentinel domains and cannot be recovered from a product website.
The bounded website pass considered the remaining 68 URLs at one page per URL,
with a shared maximum of 2 RPS, public-address validation, manual redirect
validation, and a 2 MB response cap. It stores no HTML.

Candidate extraction is deliberately non-generative: Product/SoftwareApplication
JSON-LD, OpenGraph, Twitter metadata, the HTML meta description, title, and H1
are considered in that order with length and quality guards. Fetch outcomes,
resolved URLs, response hashes, candidates, and extractor labels are retained in
`website_enrichments`. Existing Product Hunt values are never overwritten.
`products.tagline_source` and `products.description_source` distinguish
`producthunt` from reviewed `website:*` values.

The command defaults to candidate-only mode:

```bash
uv run ph-catalog enrich-websites --workers 4 --rps 2

# Apply only candidates that have been reviewed against the product and domain.
uv run ph-catalog apply-website-enrichments --slug example-product
```

The August 30 pass extracted 41 candidates. Manual review rejected parked
domains, error pages, generic third-party metadata, and brand mismatches; 31
taglines and one description were applied. The remaining 166 products (164 blank
taglines, 10 blank descriptions, with eight overlapping) were transactionally
moved out of the active catalogue:

```bash
uv run ph-catalog exclude-incomplete-copy
```

Their complete original rows and one dependent alias remain recoverable in
`excluded_products` and `excluded_product_aliases`; source-ID, archive, and
website-fetch evidence is retained. Repeated sitemap imports skip quarantined
slugs. The 574,752 active fetched products now have non-empty names, taglines,
descriptions, websites, and Product Hunt URLs. Separately, 75,679 active
descriptions match Product Hunt's generic product-page boilerplate. They remain
unchanged because replacing non-empty Product Hunt text is a different cleanup
policy.

The operational timestamps are not product launch dates. Product Hunt's live
frontend exposes `Product.createdAt` and launch-post `Post.createdAt`, but a
product record can be created after an older launch. A future time backfill should
therefore store the minimum verified `Post.createdAt` as
`first_known_producthunt_launch_at` with launch-level provenance, rather than
relabeling sitemap `source_lastmod` or local `first_seen_at`.

## Automated daily refresh

The GitHub Actions workflow in `.github/workflows/daily.yml` refreshes the
manifest every day, fetches at most 500 new products at a global 0.25 RPS, runs
verification, and publishes both a fresh Zstd Parquet catalogue and the compact
full-data archive to the private `catalog-state` release. Product Hunt rejects
GitHub-hosted runner IPs at the public sitemap boundary, so the repository-scoped
workflow runs on the registered M1 Mac with labels `self-hosted`, `macOS`,
`ARM64`, and `ph-catalog`.

All source and automation live in the Fleet workspace. The unattended runner's
mutable state lives at `~/.local/share/ph-catalog/`, because a macOS LaunchAgent
cannot access `~/Desktop` without a manual Full Disk Access grant. The ignored
`runtime` symlink in the Fleet checkout provides a convenient local entry point
to that state. Keeping the database outside the runner's disposable checkout
also prevents Git checkout cleanup from removing it. If the database is
unavailable, the workflow reconstructs fetched state from the private release
snapshot:

```bash
uv run ph-catalog restore-snapshot \
  --input snapshots/producthunt-full.parquet
```

For a smallest practical archival copy, export the complete active catalogue,
Product Hunt IDs, resolved launch relationships, aliases, and field provenance
as a schema-described `tar.zst`. This intentionally omits crawler retries,
errors, cursors, and other operational state:

```bash
uv run ph-catalog --database data/producthunt.duckdb compact-archive \
  --output snapshots/ph-catalog-full.tar.zst \
  --compression-level 22
```

The exports and DuckDB remain generated state and are not committed to Git. The
Parquet release asset contains the seven catalogue fields. The compact archive
also contains Product Hunt IDs, resolved launch relationships, aliases, and
field provenance. Retry state and scan bookkeeping remain only in Fleet's
persistent DuckDB.

## Low-rate daily fallback

When sustained blocking makes the primary backfill impractical, the `daily`
command refreshes the sitemap first, fetches new `pending` products first, and
then drains a bounded retry backlog:

```bash
uv run ph-catalog daily --limit 500 --workers 2 --rps 0.25
```

Generate a launchd definition (this does not install or load it):

```bash
uv run ph-catalog launchd-plist \
  --output launchd/local.ph-catalog.daily.plist \
  --daily-limit 500
```

Review the plist, add `--proxy-file` to its arguments if needed, then install it
manually with your preferred launchd workflow. The project never activates a
paid service, VPN, proxy, or scheduled job on its own.

## Quality checks

```bash
uv run pytest
uv run ruff check .
uv run ph-catalog verify
```

The completed local artifacts are:

```text
data/producthunt.duckdb                 # operational database and provenance
snapshots/producthunt-full.parquet      # 574,752 rows, seven fields, Zstd
```

For the acceptance sample, `reachable_parse_success` should be at least `0.95`
on 1,000 reachable pages. HTTP-unavailable and blocked pages are deliberately
not counted as successful parses. `fetched_completeness` separately checks that
every accepted row still contains the parser's required field set.
