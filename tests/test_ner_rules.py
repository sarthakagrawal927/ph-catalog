from ph_catalog.ner_rules import classify_entity, find_file_format, is_self_name


def test_self_name_filter() -> None:
    assert is_self_name("Creatorlad", "Creatorlad")
    assert is_self_name("MultiPost", "multipost.social")
    assert not is_self_name("YouTube", "Playlist Search")


def test_file_format_normalization() -> None:
    assert find_file_format("PDF reports") == "pdf"
    assert find_file_format("Markdown format") == "markdown"
    assert find_file_format("cookies") is None


def test_operating_system_filter_rejects_browsers() -> None:
    assert classify_entity("operating system", "macOS High Sierra", 1, 0.9) == (
        "operating_system",
        "macos",
    )
    assert classify_entity("operating system", "Chrome", 90, 0.99) is None


def test_recurring_external_platform_policy() -> None:
    assert classify_entity("external software service", "Spotify", 5, 0.8) == (
        "external_platform_mentioned",
        "spotify",
    )
    assert classify_entity("external software service", "PWA", 12, 0.9) is None
    assert classify_entity("external software service", "hosting provider", 12, 0.9) is None
    assert classify_entity("external software service", "SMMSEOService", 367, 0.9) is None
    assert classify_entity("external software service", "one-off brand", 1, 0.99) is None


def test_professional_role_requires_a_person_role() -> None:
    assert classify_entity("professional role", "technical writers", 5, 0.8) == (
        "audience_role",
        "technical writer",
    )
    assert classify_entity("professional role", "SEO professionals", 5, 0.8) == (
        "audience_role",
        "seo professional",
    )
    assert classify_entity("professional role", "copywriting", 5, 0.8) is None
    assert classify_entity("professional role", "AI agents", 20, 0.9) is None
    assert classify_entity("professional role", "AI data analyst", 20, 0.9) is None
    assert classify_entity("professional role", "ghost writer", 20, 0.9) is None


def test_hardware_filter_rejects_interface_terms() -> None:
    assert classify_entity("hardware device", "GPU", 5, 0.8) == ("hardware", "gpu")
    assert classify_entity("hardware device", "browser", 20, 0.9) is None
    assert classify_entity("hardware device", "Dock", 20, 0.9) is None
    assert classify_entity("hardware device", "your phone", 20, 0.9) is None
    assert classify_entity("hardware device", "AppleWatch", 20, 0.9) == (
        "hardware",
        "apple watch",
    )
    assert classify_entity("hardware device", "Vivo", 20, 0.9) is None


def test_technology_filter_rejects_weak_short_word_matches() -> None:
    assert classify_entity("programming language or framework", "Go", 20, 0.67) is None
    assert classify_entity("programming language or framework", "Go", 20, 0.8) == (
        "technology_or_tool",
        "go",
    )
    assert classify_entity("programming language or framework", "Java", 20, 0.88) is None
    assert classify_entity("programming language or framework", "Next.js", 20, 0.8) == (
        "technology_or_tool",
        "next.js",
    )


def test_industry_canonicalization_preserves_non_plural_s() -> None:
    assert classify_entity("industry sector", "logistics", 20, 0.8) == (
        "industry",
        "logistics",
    )
    assert classify_entity("industry sector", "telecommunications", 20, 0.8) == (
        "industry",
        "telecommunications",
    )
    assert classify_entity("industry sector", "restaurants", 20, 0.8) == (
        "industry",
        "restaurant",
    )
