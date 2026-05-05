import httpx
import pytest

from services.extraction.webclaw_adapter.fallback_extractor import extract_fallback
from services.extraction.webclaw_adapter.webclaw_adapter import WebClawAdapter, WebClawConfig, WebClawError, normalize_webclaw_output


def test_normalize_webclaw_output():
    raw = {"data": {"college_name": "ABC College", "courses": ["B.Tech"], "fees": ["$1000"]}}
    out = normalize_webclaw_output(raw)
    assert out["name"] == "ABC College"
    assert out["courses"] == ["B.Tech"]


def test_webclaw_success(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": {"name": "XYZ"}})

    transport = httpx.MockTransport(handler)

    class MockClient(httpx.Client):
        def __init__(self, *args, **kwargs):
            super().__init__(transport=transport, *args, **kwargs)

    monkeypatch.setattr(httpx, "Client", MockClient)
    adapter = WebClawAdapter(WebClawConfig(base_url="http://mock", enabled=True))
    out = adapter.scrape("https://example.com")
    assert out["data"]["name"] == "XYZ"


def test_webclaw_timeout(monkeypatch):
    def handler(request: httpx.Request):
        raise httpx.ReadTimeout("timeout")

    transport = httpx.MockTransport(handler)

    class MockClient(httpx.Client):
        def __init__(self, *args, **kwargs):
            super().__init__(transport=transport, *args, **kwargs)

    monkeypatch.setattr(httpx, "Client", MockClient)
    adapter = WebClawAdapter(WebClawConfig(base_url="http://mock", enabled=True))
    with pytest.raises(WebClawError):
        adapter.scrape("https://example.com")


def test_webclaw_malformed(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[1, 2, 3])

    transport = httpx.MockTransport(handler)

    class MockClient(httpx.Client):
        def __init__(self, *args, **kwargs):
            super().__init__(transport=transport, *args, **kwargs)

    monkeypatch.setattr(httpx, "Client", MockClient)
    adapter = WebClawAdapter(WebClawConfig(base_url="http://mock", enabled=True))
    with pytest.raises(WebClawError):
        adapter.scrape("https://example.com")


def test_fallback_extractor(monkeypatch):
    html = """
    <html><head><title>Sample College</title><meta name='description' content='Great college'></head>
    <body><h1>Sample College</h1><a href='/admission/apply'>Apply</a><p>Tuition is $12,000</p></body></html>
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=html)

    transport = httpx.MockTransport(handler)

    class MockClient(httpx.Client):
        def __init__(self, *args, **kwargs):
            super().__init__(transport=transport, *args, **kwargs)

    monkeypatch.setattr(httpx, "Client", MockClient)
    out = extract_fallback("https://example.com")
    assert out["name"] == "Sample College"
    assert out["meta"]["meta_description"] == "Great college"
    assert out["admission_link"]
