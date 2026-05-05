import pytest
from services.lite_pipeline.main import crawl_single


class DummyAdapter:
    def extract(self, url, schema):
        return {"data": {"name": "Test College", "location": "Test City", "official_website": url, "courses": ["B.Tech"], "fees": ["$1000"], "admission_links": [url + "/admissions"], "placements": ["90%"], "faculty": ["CS Dept"], "hostel": ["Boys Hostel"], "images": [url + "/img.jpg"]}}


@pytest.mark.integration
def test_crawl_to_record_flow(monkeypatch):
    from services import lite_pipeline as lp
    from services.lite_pipeline import main as mod

    monkeypatch.setattr(mod, "_robots_allowed", lambda url: True)
    monkeypatch.setattr(mod, "WebClawAdapter", lambda: DummyAdapter())

    captured = {}

    def fake_store(url, data):
        captured["url"] = url
        captured["data"] = data

    monkeypatch.setattr(mod, "_store_record", fake_store)

    out = crawl_single("https://example.com")
    assert captured["url"] == "https://example.com"
    assert out["source_url"] == "https://example.com"
    assert out["content_hash"]
    assert out["last_crawled_at"]
    assert out["confidence_score"] >= 0
    assert out["extraction_method"] in {"webclaw", "fallback"}
    assert out["freshness_status"] == "fresh"
