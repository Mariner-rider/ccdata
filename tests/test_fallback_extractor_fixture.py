from pathlib import Path

import httpx

from services.extraction.webclaw_adapter.fallback_extractor import extract_fallback


def test_fixture_college_extraction(monkeypatch):
    html = Path("tests/fixtures/college_sample.html").read_text(encoding="utf-8")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=html)

    transport = httpx.MockTransport(handler)

    class MockClient(httpx.Client):
        def __init__(self, *args, **kwargs):
            super().__init__(transport=transport, *args, **kwargs)

    monkeypatch.setattr(httpx, "Client", MockClient)
    out = extract_fallback("https://sit.example.edu")
    assert "Springfield Institute of Technology" in out["name"]
    assert out["fees"]
    assert out["admission_link"]
