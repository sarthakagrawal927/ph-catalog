from __future__ import annotations

import hashlib
import html as html_module
import re
from collections.abc import Iterable
from dataclasses import asdict
from urllib.parse import urlparse

import orjson
from selectolax.parser import HTMLParser, Node

from ph_catalog.models import ProductData

WHITESPACE_RE = re.compile(r"\s+")
NEXT_PAYLOAD_RE = re.compile(r'self\.__next_f\.push\(\[\d+,("(?:\\.|[^"\\])*")\]\)')


def _clean(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = WHITESPACE_RE.sub(" ", html_module.unescape(value)).strip()
    return cleaned or None


def _canonical_slug(url: str, fallback: str) -> str:
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    if (
        parsed.hostname in {"producthunt.com", "www.producthunt.com"}
        and len(parts) == 2
        and parts[0] == "products"
    ):
        return parts[1].lower()
    return fallback


def _walk_json(value: object) -> Iterable[dict[str, object]]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_json(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_json(child)


def _json_loads(value: str | bytes) -> object | None:
    try:
        return orjson.loads(value)
    except orjson.JSONDecodeError:
        return None


def _ld_product(tree: HTMLParser, canonical_url: str) -> dict[str, object]:
    best: tuple[int, dict[str, object]] | None = None
    for script in tree.css('script[type="application/ld+json"]'):
        raw = script.text().strip()
        if raw.startswith("<![CDATA[") and raw.endswith("]]>"):
            raw = raw[9:-3]
        parsed = _json_loads(raw)
        if parsed is None:
            continue
        for item in _walk_json(parsed):
            item_type = item.get("@type")
            types = item_type if isinstance(item_type, list) else [item_type]
            if "Product" not in types and "WebApplication" not in types:
                continue
            score = 0
            if item.get("@id") == canonical_url or item.get("url") == canonical_url:
                score += 10
            score += sum(
                bool(item.get(key)) for key in ("name", "description", "applicationCategory")
            )
            if best is None or score > best[0]:
                best = (score, item)
    return best[1] if best else {}


def _balanced_object(text: str, start: int) -> str | None:
    depth = 0
    in_string = False
    escaped = False
    for index in range(start, len(text)):
        character = text[index]
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            continue
        if character == '"':
            in_string = True
        elif character == "{":
            depth += 1
        elif character == "}":
            depth -= 1
            if depth == 0:
                return text[start : index + 1]
    return None


def _object_starts_containing(text: str, positions: list[int]) -> list[int]:
    """Find the currently open JSON object at each target position in one pass."""
    if not positions:
        return []
    targets = iter(sorted(positions))
    target = next(targets, None)
    starts: list[int] = []
    stack: list[int] = []
    in_string = False
    escaped = False
    for index, character in enumerate(text):
        while target is not None and index == target:
            if stack:
                starts.append(stack[-1])
            target = next(targets, None)
        if target is None:
            break
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            continue
        if character == '"':
            in_string = True
        elif character == "{":
            stack.append(index)
        elif character == "}" and stack:
            stack.pop()
    return starts


def _script_payloads(tree: HTMLParser) -> Iterable[str]:
    for script in tree.css("script:not([src])"):
        raw = script.text()
        if not raw:
            continue
        yield raw
        for match in NEXT_PAYLOAD_RE.finditer(raw):
            decoded = _json_loads(match.group(1))
            if isinstance(decoded, str):
                yield decoded


def _embedded_product(tree: HTMLParser, slug: str) -> dict[str, object]:
    slug_token = orjson.dumps(slug).decode()
    slug_re = re.compile(r'"slug"\s*:\s*' + re.escape(slug_token))
    best: tuple[int, dict[str, object]] | None = None
    typed_fallback: tuple[int, dict[str, object]] | None = None
    for payload in _script_payloads(tree):
        parsed = _json_loads(payload)
        if parsed is not None:
            for item in _walk_json(parsed):
                if item.get("slug") == slug:
                    score = _candidate_score(item)
                    if best is None or score > best[0]:
                        best = (score, item)
                elif item.get("__typename") == "Product" and item.get("websiteUrl"):
                    # Older Apollo caches keyed the page record as `Product123`
                    # and did not repeat its slug inside the normalized object.
                    # Require a direct website plus descriptive product fields so
                    # generic shell state and related navigation items cannot win.
                    score = _candidate_score(item)
                    if (
                        (item.get("tagline") or item.get("description"))
                        and (typed_fallback is None or score > typed_fallback[0])
                    ):
                        typed_fallback = (score, item)
        match_positions = [match.start() for match in slug_re.finditer(payload)]
        for start in set(_object_starts_containing(payload, match_positions)):
            fragment = _balanced_object(payload, start)
            item = _json_loads(fragment) if fragment else None
            if not isinstance(item, dict) or item.get("slug") != slug:
                continue
            score = _candidate_score(item)
            if best is None or score > best[0]:
                best = (score, item)
    if best:
        return best[1]
    return typed_fallback[1] if typed_fallback else {}


def _candidate_score(item: dict[str, object]) -> int:
    score = 5 if item.get("__typename") == "Product" else 0
    score += 2 * sum(
        bool(item.get(key))
        for key in ("name", "tagline", "description", "websiteUrl", "categories")
    )
    return score


def _categories(value: object) -> list[str]:
    if isinstance(value, str):
        cleaned = _clean(value)
        return [cleaned] if cleaned else []
    if not isinstance(value, list):
        return []
    categories: list[str] = []
    for item in value:
        name = item.get("name") if isinstance(item, dict) else item
        cleaned = _clean(name)
        if cleaned and cleaned not in categories:
            categories.append(cleaned)
    return categories


def _meta(tree: HTMLParser, *selectors: str) -> str | None:
    for selector in selectors:
        node = tree.css_first(selector)
        if node:
            value = _clean(node.attributes.get("content"))
            if value:
                return value
    return None


def _semantic_website(tree: HTMLParser) -> str | None:
    selectors = (
        'a[data-test*="visit"]',
        'a[data-testid*="website"]',
        'a[aria-label*="website" i]',
    )
    for selector in selectors:
        try:
            nodes = tree.css(selector)
        except ValueError:
            continue
        for node in nodes:
            href = _clean(node.attributes.get("href"))
            if href and urlparse(href).scheme in {"http", "https"}:
                return href
    return None


def _tagline_from_title(title: str | None, name: str | None) -> str | None:
    if not title:
        return None
    title = re.sub(r"\s*\|\s*Product Hunt\s*$", "", title).strip()
    if name and title.startswith(f"{name}:"):
        return _clean(title[len(name) + 1 :])
    return None


def parse_product_page(html: bytes, requested_slug: str, final_url: str) -> ProductData:
    tree = HTMLParser(html)
    canonical_slug = _canonical_slug(final_url, requested_slug)
    canonical_url = f"https://www.producthunt.com/products/{canonical_slug}"

    embedded = _embedded_product(tree, canonical_slug)
    structured = _ld_product(tree, canonical_url)

    name = _clean(embedded.get("name")) or _clean(structured.get("name"))
    description = _clean(embedded.get("description")) or _clean(structured.get("description"))
    description = description or _meta(
        tree, 'meta[property="og:description"]', 'meta[name="description"]'
    )
    website_url = (
        _clean(embedded.get("websiteUrl"))
        or _clean(embedded.get("website_url"))
        or _semantic_website(tree)
    )
    categories = _categories(embedded.get("categories"))
    if not categories:
        categories = _categories(structured.get("applicationCategory"))

    if not name:
        heading: Node | None = tree.css_first("main h1") or tree.css_first("h1")
        name = _clean(heading.text()) if heading else None

    tagline = _clean(embedded.get("tagline"))
    title = _meta(tree, 'meta[property="og:title"]', 'meta[name="twitter:title"]')
    tagline = tagline or _tagline_from_title(title, name)

    return ProductData(
        slug=canonical_slug,
        producthunt_url=canonical_url,
        name=name,
        tagline=tagline,
        description=description,
        website_url=website_url,
        categories=categories,
    )


def content_hash(data: ProductData) -> str:
    payload = orjson.dumps(asdict(data), option=orjson.OPT_SORT_KEYS)
    return hashlib.sha256(payload).hexdigest()
