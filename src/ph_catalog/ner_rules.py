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

PRECISION_FILTER_VERSION = "v3"

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

TECHNOLOGY_ALIASES = {
    "asp net": "asp.net",
    "next js": "next.js",
    "node js": "node.js",
}

AMBIGUOUS_TECHNOLOGY_MINIMUM_SCORE = {
    "c": 0.90,
    "go": 0.68,
    "java": 0.90,
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
    "ai",
    "api",
    "app",
    "application",
    "ats",
    "cdn",
    "chrome extension",
    "crm",
    "ease tools",
    "extension",
    "hosting provider",
    "mcp server",
    "online service",
    "online tool",
    "our service",
    "platform",
    "pwa",
    "rest api",
    "saas",
    "saas platform",
    "service",
    "software",
    "sftp",
    "telegram bot",
    "tool",
    "vpn",
    "we",
    "webdav",
    "webhooks",
    "website",
}

# A precision-first list of established third-party services. Unknown brands are
# deliberately dropped: recurrence alone allowed spam brands and generic service
# phrases to leak into the v1 output.
KNOWN_EXTERNAL_PLATFORMS = {
    "airbnb",
    "airtable",
    "amazon",
    "app store",
    "app store connect",
    "apple music",
    "asana",
    "aws",
    "aws lambda",
    "azure",
    "bitbucket",
    "box",
    "canva",
    "chatgpt",
    "claude",
    "claude ai",
    "claude code",
    "cloudflare",
    "deepseek",
    "discord",
    "docusign",
    "docker",
    "dropbox",
    "ebay",
    "etsy",
    "facebook",
    "facebook messenger",
    "figma",
    "firebase",
    "gemini",
    "github",
    "github actions",
    "github pages",
    "gitlab",
    "gmail",
    "google",
    "google ads",
    "google analytics",
    "google calendar",
    "google cloud",
    "google docs",
    "google drive",
    "google maps",
    "google meet",
    "google play",
    "google sheets",
    "google translate",
    "hacker news",
    "heroku",
    "hubspot",
    "icloud",
    "indeed",
    "instagram",
    "intercom",
    "itunes",
    "jira",
    "linkedin",
    "mailchimp",
    "make",
    "n8n",
    "netflix",
    "netlify",
    "notion",
    "obs",
    "onedrive",
    "openai",
    "patreon",
    "paypal",
    "pinterest",
    "play store",
    "reddit",
    "salesforce",
    "shopify",
    "slack",
    "soundcloud",
    "spotify",
    "steam",
    "strava",
    "stripe",
    "supabase",
    "telegram",
    "tiktok",
    "trello",
    "twitch",
    "twitter",
    "unsplash",
    "vercel",
    "vimeo",
    "webflow",
    "wetransfer",
    "whatsapp",
    "woocommerce",
    "wordpress",
    "youtube",
    "zapier",
    "zendesk",
    "zoom",
}

GENERIC_ROLES = {
    "agents",
    "business",
    "businesses",
    "companies",
    "company",
    "customer",
    "customers",
    "enterprise",
    "enterprises",
    "expert team",
    "freelance",
    "ghost writer",
    "owner",
    "owners",
    "people",
    "professional",
    "professionals",
    "pro",
    "pros",
    "role",
    "roles",
    "sales",
    "student",
    "students",
    "user",
    "users",
}

ROLE_NOUNS = {
    "accountant",
    "accountants",
    "admin",
    "admins",
    "advisor",
    "advisors",
    "analyst",
    "analysts",
    "animator",
    "animators",
    "architect",
    "architects",
    "artist",
    "artists",
    "bookkeeper",
    "bookkeepers",
    "cfo",
    "cmo",
    "coach",
    "coaches",
    "consultant",
    "consultants",
    "contractor",
    "contractors",
    "copywriter",
    "copywriters",
    "creator",
    "creators",
    "cto",
    "designer",
    "designers",
    "developer",
    "developers",
    "devs",
    "director",
    "drivers",
    "editor",
    "editors",
    "employee",
    "employees",
    "engineer",
    "engineers",
    "entrepreneur",
    "entrepreneurs",
    "founder",
    "founders",
    "freelancer",
    "freelancers",
    "manager",
    "managers",
    "marketer",
    "marketers",
    "mentor",
    "mentors",
    "operator",
    "operators",
    "photographer",
    "photographers",
    "recruiter",
    "recruiters",
    "researcher",
    "researchers",
    "scientist",
    "scientists",
    "specialist",
    "specialists",
    "teacher",
    "teachers",
    "technician",
    "technicians",
    "tester",
    "testers",
    "tutor",
    "tutors",
    "writer",
    "writers",
}

ROLE_ABBREVIATIONS = {"cfo", "cmo", "cto", "pm", "pms", "qa"}
PROFESSIONAL_ROLE_QUALIFIERS = {"creative", "hr", "it", "sales", "seo"}

ROLE_SINGULARS = {
    "accountants": "accountant",
    "admins": "admin",
    "advisors": "advisor",
    "analysts": "analyst",
    "animators": "animator",
    "architects": "architect",
    "artists": "artist",
    "bookkeepers": "bookkeeper",
    "coaches": "coach",
    "consultants": "consultant",
    "contractors": "contractor",
    "copywriters": "copywriter",
    "creators": "creator",
    "designers": "designer",
    "developers": "developer",
    "devs": "developer",
    "drivers": "driver",
    "editors": "editor",
    "employees": "employee",
    "engineers": "engineer",
    "entrepreneurs": "entrepreneur",
    "founders": "founder",
    "freelancers": "freelancer",
    "managers": "manager",
    "marketers": "marketer",
    "mentors": "mentor",
    "operators": "operator",
    "photographers": "photographer",
    "professionals": "professional",
    "recruiters": "recruiter",
    "researchers": "researcher",
    "scientists": "scientist",
    "specialists": "specialist",
    "teachers": "teacher",
    "technicians": "technician",
    "testers": "tester",
    "tutors": "tutor",
    "writers": "writer",
}

GENERIC_HARDWARE = {
    "ac",
    "board",
    "boards",
    "browser",
    "camera roll",
    "device",
    "devices",
    "dock",
    "hardware",
    "memory",
    "mobile",
    "notch",
    "samsung",
    "switch",
    "terminal",
    "vivo",
    "watch",
    "your phone",
}

INDUSTRY_CANONICAL_VALUES = {
    "e commerce": "e-commerce",
    "logistics": "logistics",
    "restaurants": "restaurant",
    "telecommunications": "telecommunications",
}

HARDWARE_ALIASES = {
    "applewatch": "apple watch",
    "cameras": "camera",
    "chromebooks": "chromebook",
    "computers": "computer",
    "cpus": "cpu",
    "desktops": "desktop",
    "gpus": "gpu",
    "iphones": "iphone",
    "ipads": "ipad",
    "laptops": "laptop",
    "macs": "mac",
    "mobile devices": "mobile device",
    "mobile phones": "mobile phone",
    "pcs": "pc",
    "phones": "phone",
    "smartphones": "smartphone",
    "smartwatches": "smartwatch",
    "ssds": "ssd",
    "tablets": "tablet",
    "tvs": "tv",
}


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
    """Return a normalized type/value only when the current precision policy accepts it."""
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
        canonical = TECHNOLOGY_ALIASES.get(value, value)
        minimum_score = AMBIGUOUS_TECHNOLOGY_MINIMUM_SCORE.get(canonical, 0.68)
        if canonical in TECHNOLOGIES and score >= minimum_score:
            return "technology_or_tool", canonical
        return None
    if label == "external software service":
        if (
            support_products >= 3
            and score >= 0.70
            and value not in GENERIC_EXTERNAL
            and value in KNOWN_EXTERNAL_PLATFORMS
        ):
            return "external_platform_mentioned", value
        return None
    if label == "professional role":
        tokens = set(value.split())
        is_professional_group = bool(
            tokens & PROFESSIONAL_ROLE_QUALIFIERS
            and tokens & {"professional", "professionals"}
        )
        is_role = bool(tokens & ROLE_NOUNS or value in ROLE_ABBREVIATIONS)
        is_software_persona = value.startswith("ai ")
        if (
            support_products >= 2
            and score >= 0.72
            and value not in GENERIC_ROLES
            and (is_role or is_professional_group)
            and not is_software_persona
            and " and " not in value
        ):
            words = value.split()
            words[-1] = ROLE_SINGULARS.get(words[-1], words[-1])
            return "audience_role", " ".join(words)
        return None
    if label == "industry sector":
        canonical = INDUSTRY_CANONICAL_VALUES.get(value, value.removesuffix("s"))
        if value in INDUSTRIES or canonical in INDUSTRIES or value == "e commerce":
            return "industry", canonical
        return None
    if label == "hardware device":
        if support_products >= 2 and score >= 0.75 and value not in GENERIC_HARDWARE:
            return "hardware", HARDWARE_ALIASES.get(value, value)
        return None
    return None
