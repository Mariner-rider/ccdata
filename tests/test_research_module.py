import asyncio
import importlib.util

import pytest

from services.research.crawler import (
    ResearchCrawler,
    ResearchItem,
    ResearchRepository,
    fallback_enrich,
    parse_arxiv_feed,
    title_author_hash,
    truncate_words,
)


def arxiv_fixture():
    return b'''<?xml version="1.0" encoding="UTF-8"?>
    <feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
      <entry>
        <id>http://arxiv.org/abs/2501.12345v1</id>
        <updated>2025-01-02T00:00:00Z</updated>
        <published>2025-01-01T00:00:00Z</published>
        <title>Machine Learning for Indian Healthcare Systems</title>
        <summary>This paper studies machine learning models for public health and medicine in India.</summary>
        <author><name>Asha Rao</name></author>
        <author><name>Vikram Singh</name></author>
        <link href="http://arxiv.org/abs/2501.12345v1" rel="alternate" type="text/html"/>
        <link title="pdf" href="http://arxiv.org/pdf/2501.12345v1" rel="related" type="application/pdf"/>
        <arxiv:doi>10.1234/example.doi</arxiv:doi>
      </entry>
    </feed>'''


def write_research_page(tmp_path):
    page = tmp_path / "research_project.html"
    page.write_text(
        """
        <html><head><title>IIT Delhi Robotics Research Project</title>
        <meta name="citation_author" content="Meera Kumar" />
        <meta name="citation_author" content="Rahul Jain" />
        <meta name="citation_publication_date" content="2025-03-01" />
        <meta name="citation_pdf_url" content="paper.pdf" />
        </head><body>
        <h1>IIT Delhi Robotics Research Project</h1>
        <section class="abstract">Abstract: This ongoing project develops robotics and machine learning systems for engineering labs at IIT Delhi.</section>
        <p>DOI: 10.5555/robotics.2025 Citations: 12</p>
        </body></html>
        """,
        encoding="utf-8",
    )
    return page.as_uri()


def test_arxiv_parser_and_fallback_enrichment():
    items = parse_arxiv_feed(arxiv_fixture())
    assert len(items) == 1
    item = items[0]
    assert item.arxiv_id == "2501.12345v1"
    assert item.doi == "10.1234/example.doi"
    assert item.status == "preprint"
    assert item.pdf_url.endswith("2501.12345v1")
    abstract, field, subfield, keywords = fallback_enrich(item.title, item.abstract, item.source_url)
    assert len(abstract.split()) <= 500
    assert field in {"engineering", "medicine"}
    assert keywords
    assert title_author_hash(item.title, item.authors) == title_author_hash(item.title.upper(), list(reversed(item.authors)))
    assert len(truncate_words("word " * 600, 500).split()) == 500


def test_research_crawler_extracts_portal_page_and_institution(tmp_path):
    db = tmp_path / "research.db"
    repo = ResearchRepository(f"sqlite:///{db}")
    repo.init_sqlite()
    import sqlite3

    with sqlite3.connect(db) as conn:
        conn.execute("INSERT INTO institutions(id,name,source_url) VALUES(42,'IIT Delhi','https://home.iitd.ac.in')")
        conn.commit()

    crawler = ResearchCrawler(max_pages=5, rate_limit_seconds=0)
    payload = asyncio.run(crawler._fetch(write_research_page(tmp_path)))
    item = asyncio.run(crawler.extract_item(payload, write_research_page(tmp_path), repository=repo))

    assert item.title == "IIT Delhi Robotics Research Project"
    assert item.institution_id == 42
    assert item.institution_name == "IIT Delhi"
    assert item.type == "ongoing_project"
    assert item.status == "ongoing"
    assert item.field == "engineering"
    assert item.doi == "10.5555/robotics.2025"
    assert item.citation_count == 12
    assert item.pdf_url.endswith("paper.pdf")


def test_research_repository_upsert_filters_and_search(tmp_path):
    repo = ResearchRepository(f"sqlite:///{tmp_path / 'research.db'}")
    item = ResearchItem(
        title="Machine Learning in IIT Labs",
        authors=["A", "B"],
        abstract="Machine learning research in engineering labs.",
        type="paper",
        field="engineering",
        subfield="machine learning",
        keywords=["machine learning", "IIT"],
        institution_id=42,
        institution_name="IIT Delhi",
        published_date="2025-01-01",
        doi="10.1/test",
        source_url="https://example.com/paper",
        status="published",
    )
    assert repo.upsert_many([item, item]) == 2
    assert len(repo.list(field="engineering", item_type="paper", year=2025)) == 1
    assert repo.list(institution_id=42)[0]["institution_name"] == "IIT Delhi"
    assert repo.search("machine learning IIT")[0]["title"] == "Machine Learning in IIT Labs"
    assert repo.get(1)["authors"] == ["A", "B"]


@pytest.mark.skipif(importlib.util.find_spec("fastapi") is None, reason="fastapi is not installed")
def test_research_api_endpoints(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from services.lite_pipeline import api

    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{tmp_path / 'api_research.db'}")
    repo = ResearchRepository(f"sqlite:///{tmp_path / 'api_research.db'}")
    repo.upsert_many([
        ResearchItem(
            title="AIIMS Medicine Study",
            authors=["Doctor A"],
            abstract="Medicine research using AI.",
            type="paper",
            field="medicine",
            keywords=["AIIMS", "medicine"],
            institution_id=7,
            institution_name="AIIMS Delhi",
            published_date="2025-02-01",
            source_url="https://example.com/aiims",
            status="published",
        )
    ])
    client = TestClient(api.app)
    listing = client.get("/research", params={"field": "medicine", "type": "paper", "year": 2025})
    assert listing.status_code == 200
    item_id = listing.json()["results"][0]["id"]
    assert client.get(f"/research/{item_id}").json()["title"] == "AIIMS Medicine Study"
    assert client.get("/research/search", params={"q": "AIIMS medicine"}).json()["results"][0]["field"] == "medicine"
