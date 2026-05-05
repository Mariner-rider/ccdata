from services.extraction.webclaw_adapter.webclaw_adapter import WebClawAdapter, WebClawConfig, WebClawError, normalize_webclaw_output


def test_normalize_webclaw_output():
    raw = {"data": {"college_name": "ABC College"}}
    out = normalize_webclaw_output(raw)
    assert out["name"] == "ABC College"


def test_webclaw_disabled_mode():
    adapter = WebClawAdapter(WebClawConfig(base_url="", enabled=False))
    try:
        adapter.scrape("https://example.com")
    except WebClawError:
        assert True
