import asyncio
import importlib.util

import pytest

from services.news.crawler import NewsArticle, NewsCrawler, NewsRepository, fallback_classify, truncate_words


def write_news_site(tmp_path):
    image = tmp_path / "thumb.jpg"
    image.write_bytes(b"jpg")
    article = tmp_path / "exam_notification_news.html"
    article.write_text(
        f"""
        <html><head>
          <meta property="article:published_time" content="2026-05-16" />
          <meta property="og:image" content="{image.as_uri()}" />
          <title>NEET UG exam notification issued by NTA for Delhi University aspirants</title>
        </head><body>
          <article>
            <h1>NEET UG exam notification issued by NTA for Delhi University aspirants</h1>
            <p>NTA has released an important education exam notification for NEET UG 2026. Delhi University aspirants and medical college applicants should check the exam schedule and application process.</p>
            <p>The notification includes exam dates, eligibility, state-wise centres and policy updates for students.</p>
          </article>
        </body></html>
        """,
        encoding="utf-8",
    )
    index = tmp_path / "education.html"
    index.write_text(f'<html><body><a href="{article.as_uri()}">Education News</a></body></html>', encoding="utf-8")
    return index.as_uri()


def test_fallback_classification_and_summary_limit():
    text = "NEET exam notification " + "word " * 260
    category, tags, summary = fallback_classify(text)
    assert category == "exam_notification"
    assert "NEET" in [tag.upper() for tag in tags]
    assert len(summary.split()) <= 200
    assert len(truncate_words(text, 20).split()) == 20


def test_news_crawler_extracts_article_thumbnail_and_related_entity(tmp_path, monkeypatch):
    monkeypatch.setenv("NEWS_S3_ENABLED", "false")
    db = tmp_path / "news.db"
    repo = NewsRepository(f"sqlite:///{db}")
    repo.init_sqlite()
    import sqlite3

    with sqlite3.connect(db) as conn:
        conn.execute("INSERT INTO institutions(id,name,source_url) VALUES(42,'Delhi University','https://du.ac.in')")
        conn.commit()

    crawler = NewsCrawler(max_articles_per_source=5, rate_limit_seconds=0)
    articles = asyncio.run(crawler.crawl(sources=[write_news_site(tmp_path)], repository=repo))

    assert len(articles) == 1
    article = articles[0]
    assert article.category == "exam_notification"
    assert article.published_at == "2026-05-16"
    assert article.related_entity_ids == [42]
    assert article.image_url.startswith("disabled://news-thumbnails/")
    assert len(article.summary.split()) <= 200


def test_news_repository_upsert_filters_featured_and_entity(tmp_path):
    repo = NewsRepository(f"sqlite:///{tmp_path / 'news.db'}")
    article = NewsArticle(
        title="CUET result announced",
        summary="CUET result announced by NTA.",
        content_url="https://example.com/cuet-result",
        source_name="example.com",
        category="result",
        tags=["CUET", "NTA"],
        published_at="2026-05-15",
        scraped_at="2026-05-17T00:00:00+00:00",
        related_entity_ids=[7],
        image_url="",
        is_featured=True,
    )
    assert repo.upsert_many([article, article]) == 2
    assert len(repo.list(category="result", days=30)) == 1
    assert repo.list(entity_id=7)[0]["title"] == "CUET result announced"
    assert repo.list(featured=True)[0]["category"] == "result"
    assert repo.get(1)["tags"] == ["CUET", "NTA"]


@pytest.mark.skipif(importlib.util.find_spec("fastapi") is None, reason="fastapi is not installed")
def test_news_api_endpoints(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from services.lite_pipeline import api

    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{tmp_path / 'api_news.db'}")
    repo = NewsRepository(f"sqlite:///{tmp_path / 'api_news.db'}")
    repo.upsert_many([
        NewsArticle(
            title="Scholarship scheme launched",
            summary="A scholarship scheme was launched for students.",
            content_url="https://example.com/scholarship",
            source_name="example.com",
            category="scholarship",
            tags=["Scholarship"],
            published_at="2026-05-16",
            scraped_at="2026-05-17T00:00:00+00:00",
            related_entity_ids=[42],
            is_featured=True,
        )
    ])
    client = TestClient(api.app)
    listing = client.get("/news", params={"category": "scholarship", "entity_id": 42})
    assert listing.status_code == 200
    article_id = listing.json()["results"][0]["id"]
    assert client.get(f"/news/{article_id}").json()["title"] == "Scholarship scheme launched"
    assert client.get("/news/featured").json()["results"][0]["category"] == "scholarship"
