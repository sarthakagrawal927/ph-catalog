from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum


class ProductStatus(StrEnum):
    PENDING = "pending"
    FETCHED = "fetched"
    RETRY = "retry"
    UNAVAILABLE = "unavailable"
    PARSE_FAILED = "parse_failed"


@dataclass(slots=True, frozen=True)
class SitemapProduct:
    slug: str
    producthunt_url: str
    lastmod: datetime | None = None


@dataclass(slots=True, frozen=True)
class PendingProduct:
    slug: str
    producthunt_url: str
    attempt: int


@dataclass(slots=True)
class ProductData:
    slug: str
    producthunt_url: str
    name: str | None = None
    tagline: str | None = None
    description: str | None = None
    website_url: str | None = None
    categories: list[str] = field(default_factory=list)

    @property
    def is_complete_enough(self) -> bool:
        return bool(self.name and self.description and (self.tagline or self.website_url))


@dataclass(slots=True, frozen=True)
class CatalogRecord:
    product_id: int
    data: ProductData
    content_hash: str


@dataclass(slots=True)
class FetchResult:
    requested_slug: str
    canonical_slug: str
    requested_url: str
    canonical_url: str
    attempt: int
    status: ProductStatus
    http_status: int | None
    proxy_label: str
    elapsed_ms: int
    data: ProductData | None = None
    error: str | None = None
    retry_at: datetime | None = None
    content_hash: str | None = None
    fixture_html: bytes | None = None

    @property
    def is_alias(self) -> bool:
        return self.requested_slug != self.canonical_slug
