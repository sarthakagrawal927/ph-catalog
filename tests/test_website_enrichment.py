from __future__ import annotations

from ph_catalog.website_enrichment import extract_website_metadata


def test_prefers_json_ld_and_retains_candidate_sources() -> None:
    html = """
    <script type="application/ld+json">
      {"@context":"https://schema.org", "@type":"SoftwareApplication",
       "slogan":"Plan calm, productive workdays", 
       "description":"A focused planning workspace for teams that need to coordinate work."}
    </script>
    <meta property="og:description" content="Open Graph fallback description for the same app.">
    <title>Focusboard | Calm planning for fast-moving teams</title>
    <h1>Make planning feel easy</h1>
    """

    result = extract_website_metadata(html, product_name="Focusboard")

    assert result.tagline is not None
    assert result.tagline.value == "Plan calm, productive workdays"
    assert result.tagline.source == "json_ld_slogan"
    assert result.description is not None
    assert result.description.source == "json_ld_description"
    assert [candidate.source for candidate in result.tagline_candidates] == [
        "json_ld_slogan",
        "json_ld_description",
        "og_description",
        "title",
        "h1",
    ]


def test_meta_priority_and_title_name_prefix_are_handled() -> None:
    html = """
    <meta name="description"
          content="A detailed fallback that should rank below Twitter and Open Graph.">
    <meta name="twitter:description"
          content="A useful Twitter summary for people who make websites.">
    <meta property="og:description"
          content="A useful Open Graph summary for people who make websites.">
    <title>Acme — Websites that explain your product clearly</title>
    """

    result = extract_website_metadata(html, product_name="Acme")

    assert result.description is not None
    assert result.description.source == "og_description"
    assert result.tagline is not None
    assert result.tagline.value == "A useful Open Graph summary for people who make websites."
    assert result.tagline_candidates[-1].value == "Websites that explain your product clearly"
    assert result.tagline_candidates[-1].source == "title"


def test_rejects_navigation_and_low_quality_copy() -> None:
    html = """
    <script type="application/ld+json">
      {"@type":"Product", "slogan":"Coming soon", "description":"Home"}
    </script>
    <meta property="og:description" content="Welcome to Example">
    <meta name="description" content="Short copy">
    <title>Example | Official Website</title>
    <h1>Example</h1>
    """

    result = extract_website_metadata(html, product_name="Example")

    assert result.tagline is None
    assert result.description is None
    assert result.tagline_candidates == ()
    assert result.description_candidates == ()


def test_ignores_non_product_json_ld_and_deduplicates_values() -> None:
    html = """
    <script type="application/ld+json">
      {"@type":"WebSite", "description":"A generic site-wide description that is not product copy."}
    </script>
    <script type="application/ld+json">
      [{"@type":["Thing", "WebApplication"],
        "description":"Build privacy-friendly forms without writing backend code."}]
    </script>
    <meta property="og:description"
          content="Build privacy-friendly forms without writing backend code.">
    """

    result = extract_website_metadata(html)

    assert result.description is not None
    assert result.description.value == "Build privacy-friendly forms without writing backend code."
    assert result.description.source == "json_ld_description"
    assert len(result.description_candidates) == 1
    assert result.tagline is not None
    assert result.tagline.source == "json_ld_description"
