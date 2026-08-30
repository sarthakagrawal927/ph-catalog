"""Conservative, fetch-free extraction of descriptive website metadata.

The crawler intentionally lives elsewhere: this module only evaluates HTML that a
caller has already obtained.  It favours publisher supplied structured metadata
over visible page chrome, and returns both the selected value and its provenance.
"""

from __future__ import annotations

import html as html_module
import re
from collections.abc import Iterable
from dataclasses import dataclass

import orjson
from selectolax.parser import HTMLParser

_WHITESPACE_RE = re.compile(r"\s+")
_TITLE_SEPARATORS = (" | ", " — ", " - ", ": ")
_WORD_RE = re.compile(r"[\w][\w'’.-]*", re.UNICODE)
_REJECTED_TEXT = {
    "404",
    "404 not found",
    "access denied",
    "coming soon",
    "forbidden",
    "home",
    "homepage",
    "loading",
    "not found",
    "official website",
    "page not found",
    "untitled",
    "welcome",
}
_REJECTED_PREFIXES = (
    "cookie ",
    "privacy ",
    "terms of ",
    "welcome to ",
)
_JSON_LD_TYPES = {"product", "softwareapplication", "webapplication"}


@dataclass(frozen=True, slots=True)
class MetadataCandidate:
    """A normalized value and the HTML metadata source that supplied it."""

    value: str
    source: str


@dataclass(frozen=True, slots=True)
class WebsiteMetadata:
    """Selected conservative candidates plus all accepted alternatives.

    Candidate tuples are ordered by source preference.  A caller can persist the
    selected values while retaining the source labels for auditability.
    """

    tagline: MetadataCandidate | None
    description: MetadataCandidate | None
    tagline_candidates: tuple[MetadataCandidate, ...]
    description_candidates: tuple[MetadataCandidate, ...]


def extract_website_metadata(
    html: str | bytes, *, product_name: str | None = None
) -> WebsiteMetadata:
    """Extract high-confidence tagline and description candidates from HTML.

    This does not make requests and does not attempt to synthesize copy.  The
    optional product name helps discard a page title or H1 that contains only the
    product's name.
    """

    tree = HTMLParser(html)
    normalized_name = _normalize_name(product_name)
    tagline_candidates: list[MetadataCandidate] = []
    description_candidates: list[MetadataCandidate] = []

    for item in _json_ld_items(tree):
        slogan = _clean(item.get("slogan"))
        if _is_usable(slogan, "tagline", normalized_name):
            tagline_candidates.append(MetadataCandidate(slogan, "json_ld_slogan"))

        description = _clean(item.get("description"))
        if _is_usable(description, "description", normalized_name):
            description_candidates.append(MetadataCandidate(description, "json_ld_description"))
        if _is_usable(description, "tagline", normalized_name):
            tagline_candidates.append(MetadataCandidate(description, "json_ld_description"))

    for source, description in _description_meta_values(tree):
        if _is_usable(description, "description", normalized_name):
            description_candidates.append(MetadataCandidate(description, source))
        if _is_usable(description, "tagline", normalized_name):
            tagline_candidates.append(MetadataCandidate(description, source))

    title = _title_tagline(tree, normalized_name)
    if _is_usable(title, "tagline", normalized_name):
        tagline_candidates.append(MetadataCandidate(title, "title"))

    h1 = _first_text(tree, "h1")
    if _is_usable(h1, "tagline", normalized_name):
        tagline_candidates.append(MetadataCandidate(h1, "h1"))

    unique_taglines = _deduplicate(tagline_candidates)
    unique_descriptions = _deduplicate(description_candidates)
    return WebsiteMetadata(
        tagline=unique_taglines[0] if unique_taglines else None,
        description=unique_descriptions[0] if unique_descriptions else None,
        tagline_candidates=unique_taglines,
        description_candidates=unique_descriptions,
    )


def _clean(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = _WHITESPACE_RE.sub(" ", html_module.unescape(value)).strip()
    return cleaned or None


def _normalize_name(value: str | None) -> str | None:
    cleaned = _clean(value)
    return cleaned.casefold() if cleaned else None


def _is_usable(value: str | None, kind: str, product_name: str | None) -> bool:
    if not value:
        return False
    length = len(value)
    lower_value = value.casefold()
    words = _WORD_RE.findall(value)
    if kind == "tagline":
        if not 12 <= length <= 160 or len(words) < 2:
            return False
    elif not 24 <= length <= 2_000 or len(words) < 4:
        return False
    if lower_value == product_name or lower_value in _REJECTED_TEXT:
        return False
    if lower_value.startswith(_REJECTED_PREFIXES):
        return False
    alpha_characters = sum(character.isalpha() for character in value)
    return alpha_characters >= 3 and alpha_characters / length >= 0.2


def _json_ld_items(tree: HTMLParser) -> Iterable[dict[str, object]]:
    for script in tree.css('script[type="application/ld+json"]'):
        raw = script.text().strip()
        if raw.startswith("<![CDATA[") and raw.endswith("]]>"):
            raw = raw[9:-3]
        try:
            parsed = orjson.loads(raw)
        except orjson.JSONDecodeError:
            continue
        yield from _walk_json_ld(parsed)


def _walk_json_ld(value: object) -> Iterable[dict[str, object]]:
    if isinstance(value, dict):
        type_value = value.get("@type")
        types = type_value if isinstance(type_value, list) else [type_value]
        if any(isinstance(item, str) and item.casefold() in _JSON_LD_TYPES for item in types):
            yield value
        for child in value.values():
            yield from _walk_json_ld(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_json_ld(child)


def _description_meta_values(tree: HTMLParser) -> Iterable[tuple[str, str | None]]:
    selectors = (
        ("og_description", 'meta[property="og:description"]'),
        ("twitter_description", 'meta[name="twitter:description"]'),
        ("twitter_description", 'meta[property="twitter:description"]'),
        ("meta_description", 'meta[name="description"]'),
    )
    for source, selector in selectors:
        for node in tree.css(selector):
            yield source, _clean(node.attributes.get("content"))


def _title_tagline(tree: HTMLParser, product_name: str | None) -> str | None:
    title = _first_text(tree, "title")
    if not title or not product_name:
        return title
    lower_title = title.casefold()
    for separator in _TITLE_SEPARATORS:
        left, found, right = lower_title.partition(separator)
        if found and left.strip() == product_name:
            return title[len(left) + len(separator) :].strip()
        if found and right.strip() == product_name:
            return title[: len(left)].strip()
    return title


def _first_text(tree: HTMLParser, selector: str) -> str | None:
    node = tree.css_first(selector)
    return _clean(node.text()) if node else None


def _deduplicate(candidates: list[MetadataCandidate]) -> tuple[MetadataCandidate, ...]:
    seen: set[str] = set()
    unique: list[MetadataCandidate] = []
    for candidate in candidates:
        key = candidate.value.casefold()
        if key not in seen:
            seen.add(key)
            unique.append(candidate)
    return tuple(unique)
