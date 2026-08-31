# Catalogue handoff v1

This immutable handoff contains the complete cleaned catalogue, its standalone
analytics mart, the broad-label outputs and audit evidence, and the completed
GLiNER entity run with its raw resumable shards.

## Verified contents

- 574,752 unique products; zero duplicate slugs.
- Complete name, tagline, and description coverage for every retained product.
- 517,853 external website URLs across 406,208 exact hosts.
- 199,654 unchanged Product Hunt category assignments on 93,184 products.
- 281,228 trusted broad-label assignments on 247,788 products.
- 115,504 filtered entity assignments on 75,713 products.
- 709,323 mapped launch records on 547,763 products; post IDs are relative
  ordering evidence, not calendar dates.
- Frozen-rule entity audit: 97/100 correct types and 100/100 product-relevant
  mentions in a type-stratified sample with rare-tail coverage.
- 14 checksummed data assets totaling 299,803,623 bytes, plus the manifest.

The release was verified by decompressing the analytics database, opening it
read-only, checking row uniqueness and entity referential integrity, unpacking
all 58 raw NER shards, rebuilding precision-filter v3 output, and matching the
rebuilt Parquet SHA-256 to the released entity file.

See `docs/machine-handoff.md` for download and verification commands. Votes,
comment counts, and trustworthy calendar launch dates are not included.
