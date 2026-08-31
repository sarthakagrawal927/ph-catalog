"""Deterministic precision filters for zero-shot product entity extraction."""

from __future__ import annotations

import re

ENTITY_LABELS = (
    "professional role",
    "industry sector",
    "technical method or standard",
    "programming language or framework",
    "operating system",
    "external software service",
    "file format",
    "hardware device",
)

OS_ALIASES = {
    "android": "android",
    "ios": "ios",
    "ipados": "ipados",
    "linux": "linux",
    "mac os": "macos",
    "macos": "macos",
    "tvos": "tvos",
    "ubuntu": "ubuntu",
    "watchos": "watchos",
    "windows": "windows",
    "windows 10": "windows",
    "windows 11": "windows",
}

FILE_FORMATS = {
    "apk",
    "avif",
    "csv",
    "doc",
    "docx",
    "epub",
    "gif",
    "heic",
    "html",
    "ics",
    "jpeg",
    "jpg",
    "json",
    "markdown",
    "md",
    "mobi",
    "mov",
    "mp3",
    "mp4",
    "odp",
    "ods",
    "odt",
    "parquet",
    "pdf",
    "png",
    "ppt",
    "pptx",
    "psd",
    "rtf",
    "svg",
    "tiff",
    "tsv",
    "txt",
    "wav",
    "webm",
    "webp",
    "xls",
    "xlsx",
    "xml",
    "yaml",
    "yml",
    "zip",
}

TECHNOLOGIES = {
    "ai",
    "angular",
    "artificial intelligence",
    "asp.net",
    "aws",
    "azure",
    "bootstrap",
    "c",
    "c#",
    "c++",
    "chatgpt",
    "claude code",
    "cloudflare",
    "codex",
    "css",
    "dart",
    "deep learning",
    "django",
    "docker",
    "electron",
    "firebase",
    "flutter",
    "git",
    "github",
    "go",
    "graphql",
    "java",
    "javascript",
    "jquery",
    "kotlin",
    "kubernetes",
    "laravel",
    "machine learning",
    "mongodb",
    "mysql",
    "next.js",
    "node.js",
    "notion",
    "openai",
    "php",
    "postgresql",
    "python",
    "pytorch",
    "react",
    "react native",
    "redis",
    "ruby",
    "rust",
    "salesforce",
    "shopify",
    "slack",
    "sql",
    "stripe",
    "supabase",
    "swift",
    "tailwind",
    "tensorflow",
    "typescript",
    "vue",
    "web3",
    "wordpress",
    "zapier",
}

INDUSTRIES = {
    "agriculture",
    "automotive",
    "banking",
    "construction",
    "cybersecurity",
    "e-commerce",
    "education",
    "energy",
    "finance",
    "fintech",
    "food",
    "government",
    "healthcare",
    "hospitality",
    "insurance",
    "legal",
    "logistics",
    "manufacturing",
    "marketing",
    "media",
    "nonprofit",
    "pharmaceutical",
    "real estate",
    "restaurants",
    "retail",
    "telemedicine",
    "telecommunications",
    "travel",
}

GENERIC_EXTERNAL = {
    "app",
    "application",
    "online service",
    "online tool",
    "platform",
    "pwa",
    "saas",
    "service",
    "software",
    "tool",
    "website",
}

GENERIC_ROLES = {
    "business",
    "businesses",
    "companies",
    "company",
    "customer",
    "customers",
    "enterprise",
    "enterprises",
    "owner",
    "owners",
    "people",
    "professional",
    "professionals",
    "student",
    "students",
    "user",
    "users",
}

GENERIC_HARDWARE = {"board", "boards", "device"}


def clean_entity(value: str) -> str:
    value = value.casefold().strip()
    value = re.sub(r"[\s_/.-]+", " ", value)
    return re.sub(r"\s+", " ", value)


def compact_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.casefold())


def is_self_name(entity_text: str, product_name: str) -> bool:
    entity = compact_name(entity_text)
    product = compact_name(product_name)
    return bool(
        entity and product and (entity == product or (len(entity) >= 5 and entity in product))
    )


def find_file_format(value: str) -> str | None:
    tokens = re.findall(r"[a-z0-9]+", clean_entity(value))
    for token in tokens:
        token = {"gifs": "gif", "jpegs": "jpeg", "pdfs": "pdf"}.get(token, token)
        if token in FILE_FORMATS:
            return {"md": "markdown", "yml": "yaml"}.get(token, token)
    return None


def classify_entity(
    label: str,
    entity_text: str,
    support_products: int,
    score: float,
) -> tuple[str, str] | None:
    """Return a normalized type/value only when the v1 precision policy accepts it."""
    value = clean_entity(entity_text)
    if label == "operating system":
        for alias, canonical in OS_ALIASES.items():
            if value == alias or value.startswith(f"{alias} "):
                return "operating_system", canonical
        return None
    if label == "file format":
        canonical = find_file_format(value)
        return ("file_format", canonical) if canonical else None
    if label in {"programming language or framework", "technical method or standard"}:
        return ("technology_or_tool", value) if value in TECHNOLOGIES else None
    if label == "external software service":
        if support_products >= 3 and score >= 0.70 and value not in GENERIC_EXTERNAL:
            return "external_platform_mentioned", value
        return None
    if label == "professional role":
        if support_products >= 2 and score >= 0.72 and value not in GENERIC_ROLES:
            return "audience_role", value
        return None
    if label == "industry sector":
        singular = value.removesuffix("s")
        if value in INDUSTRIES or singular in INDUSTRIES:
            return "industry", singular
        return None
    if label == "hardware device":
        if support_products >= 2 and score >= 0.75 and value not in GENERIC_HARDWARE:
            return "hardware", value
        return None
    return None
