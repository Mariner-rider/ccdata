import asyncio
from types import SimpleNamespace

from services.deep_crawler.crawler import DeepCrawler


COURSE_HTML = """
<html><body>
<section><p>Example Institute is a multidisciplinary campus with strong academics, research,
industry collaboration and student support services across several schools.</p></section>
<table>
<tr><th>Course</th><th>Duration</th><th>Seats</th></tr>
<tr><td>B.Tech Computer Science</td><td>4 years</td><td>120</td></tr>
<tr><td>MBA</td><td>2 years</td><td>60</td></tr>
</table>
<table>
<tr><th>Program</th><th>Fee</th></tr>
<tr><td>B.Tech</td><td>₹ 1,20,000</td></tr>
</table>
<div class="faculty">Dr. Asha Sharma, Professor of Computer Science</div>
<img src="/images/campus-library.jpg" alt="Campus library">
<p>Contact admissions@example.edu +91 9876543210</p>
<iframe src="https://maps.example.com/embed"></iframe>
<p>Placement reached 92% with average package 8.5 LPA.</p>
</body></html>
"""


def test_extract_structured_finds_course_tables():
    data = DeepCrawler()._extract_structured(COURSE_HTML, "https://example.edu/courses")
    assert {course["name"] for course in data["courses"]} >= {"B.Tech Computer Science", "MBA"}


def test_extract_structured_finds_fee_amounts():
    data = DeepCrawler()._extract_structured(COURSE_HTML, "https://example.edu/fees")
    assert any("₹ 1,20,000" in fee for fee in data["fees"])


def test_extract_structured_returns_empty_for_nav_only_pages():
    html = "<html><body><nav><a>Home</a><a>About</a></nav><footer>Links</footer></body></html>"
    assert DeepCrawler()._extract_structured(html, "https://example.edu") == {}


def test_merge_pages_deduplicates_course_lists():
    crawler = DeepCrawler()
    merged = crawler._merge_pages([
        {"courses": [{"name": "MBA", "details": "2 years"}]},
        {"courses": [{"name": "MBA", "details": "2 years"}, {"name": "B.Tech", "details": "4 years"}]},
    ])
    assert merged["courses"] == [
        {"name": "MBA", "details": "2 years"},
        {"name": "B.Tech", "details": "4 years"},
    ]


def test_merge_pages_keeps_longer_about_text():
    merged = DeepCrawler()._merge_pages([
        {"about": "Short campus summary."},
        {"about": "A much longer campus summary with more complete institutional details."},
    ])
    assert merged["about"].startswith("A much longer")


def test_crawl_page_uses_httpx_fallback(monkeypatch):
    class FakeResponse:
        text = COURSE_HTML

        def raise_for_status(self):
            return None

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            self.kwargs = kwargs

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url, headers):
            assert url == "https://example.edu/courses"
            assert "User-Agent" in headers
            return FakeResponse()

    import services.deep_crawler.crawler as module

    monkeypatch.setattr(module.importlib.util, "find_spec", lambda name: None if name == "crawl4ai" else object())
    async def fake_sleep(delay):
        return None

    monkeypatch.setattr(module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(module, "httpx", SimpleNamespace(AsyncClient=FakeAsyncClient))
    assert "B.Tech" in asyncio.run(DeepCrawler()._crawl_page("https://example.edu/courses"))
