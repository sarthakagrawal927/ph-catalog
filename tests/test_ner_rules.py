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
    assert classify_entity("external software service", "one-off brand", 1, 0.99) is None
