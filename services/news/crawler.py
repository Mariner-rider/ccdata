from __future__ import annotations

import asyncio
import hashlib
import importlib
import importlib.util
import json
import logging
import os
import re
import sqlite3
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib import robotparser
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

from services.common.user_agents import add_jitter, get_headers, get_random_ua

LOGGER = logging.getLogger(__name__)

if importlib.util.find_spec("firecrawl"):
    _firecrawl = importlib.import_module("firecrawl")
    FirecrawlApp = getattr(_firecrawl, "FirecrawlApp", None)
    if FirecrawlApp is None and importlib.util.find_spec("firecrawl.firecrawl"):
        _firecrawl = importlib.import_module("firecrawl.firecrawl")
        FirecrawlApp = getattr(_firecrawl, "FirecrawlApp", None)
else:
    FirecrawlApp = None

litellm = importlib.import_module("litellm") if importlib.util.find_spec("litellm") else None
boto3 = importlib.import_module("boto3") if importlib.util.find_spec("boto3") else None
BotoConfig = importlib.import_module("botocore.client").Config if importlib.util.find_spec("botocore") else None
psycopg = importlib.import_module("psycopg") if importlib.util.find_spec("psycopg") else None

NEWS_SOURCES = (
    "https://timesofindia.indiatimes.com/education",
    "https://www.hindustantimes.com/education",
    "https://www.ndtv.com/education",
    "https://www.thehindu.com/education",
    "https://www.indiatoday.in/education-today",
    "https://educationpost.in",
    "https://www.shiksha.com/news",
    "https://news.careers360.com",
    "https://www.jagranjosh.com/articles-education-1294821304-1",
    "https://www.amarujala.com/education",
    "https://www.ugc.ac.in/news/",
    "https://www.aicte-india.org/news",
    "https://www.moe.gov.in/news",
    "https://nta.ac.in/News",
)
CATEGORIES = {"admission_update", "exam_notification", "result", "scholarship", "welfare_scheme", "policy", "campus_news", "ranking", "abroad"}
NEWS_HINTS = ("education", "exam", "admission", "result", "scholarship", "college", "university", "ugc", "nta", "aicte", "school", "rank")
THUMB_BUCKET = "news-thumbnails"


@dataclass
class NewsArticle:
    title: str
    summary: str
    content_url: str
    source_name: str
    category: str
    tags: list[str]
    published_at: str | None = None
    scraped_at: str = ""
    related_entity_ids: list[int] | None = None
    image_url: str = ""
    is_featured: bool = False
    raw_payload: dict[str, Any] | None = None

    def to_row(self) -> dict[str, Any]:
        row = asdict(self)
        row["tags"] = json.dumps(row.get("tags") or [])
        row["related_entity_ids"] = json.dumps(row.get("related_entity_ids") or [])
        row["raw_payload"] = json.dumps(row.get("raw_payload") or {}, sort_keys=True)
        row["is_featured"] = 1 if row.get("is_featured") else 0
        return row


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_date(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d %B %Y", "%d %b %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    match = re.search(r"(20\d{2})[-/](\d{1,2})[-/](\d{1,2})", value)
    if match:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3))).isoformat()
    return None


def truncate_words(text: str, limit: int = 200) -> str:
    words = re.sub(r"\s+", " ", text or "").strip().split()
    return " ".join(words[:limit])


def fallback_classify(text: str) -> tuple[str, list[str], str]:
    lower = text.lower()
    if "admission" in lower or "counselling" in lower:
        category = "admission_update"
    elif "result" in lower or "scorecard" in lower:
        category = "result"
    elif "scholarship" in lower or "fellowship" in lower:
        category = "scholarship"
    elif "scheme" in lower or "welfare" in lower:
        category = "welfare_scheme"
    elif "exam" in lower or "neet" in lower or "jee" in lower or "cuet" in lower or "nta" in lower:
        category = "exam_notification"
    elif "policy" in lower or "regulation" in lower or "ugc" in lower or "aicte" in lower:
        category = "policy"
    elif "ranking" in lower or "rankings" in lower or "nirf" in lower:
        category = "ranking"
    elif "abroad" in lower or "visa" in lower or "international" in lower:
        category = "abroad"
    else:
        category = "campus_news"
    tags = sorted(set(re.findall(r"\b(?:NEET|JEE|CUET|NTA|UGC|AICTE|CBSE|UPSC|NIRF|IIT|IIM|DU|Delhi|UP|Maharashtra|Karnataka)\b", text, re.I)), key=str.lower)[:12]
    return category, tags, truncate_words(text, 200)


def source_name(url: str) -> str:
    return urlparse(url).netloc.removeprefix("www.") or "local"


class FirecrawlClient:
    def __init__(self) -> None:
        self.enabled = FirecrawlApp is not None and bool(os.getenv("FIRECRAWL_API_KEY"))
        self.app = FirecrawlApp(api_key=os.getenv("FIRECRAWL_API_KEY")) if self.enabled else None

    def scrape(self, url: str) -> dict[str, Any]:
        if not self.app:
            return {}
        return self.app.scrape_url(url, formats=["markdown", "html", "links"])


class ThumbnailStore:
    def __init__(self) -> None:
        self.bucket = os.getenv("NEWS_THUMB_BUCKET", THUMB_BUCKET)
        self.enabled = os.getenv("NEWS_S3_ENABLED", "true").lower() != "false" and boto3 is not None
        self.client = None
        if self.enabled:
            self.client = boto3.client(
                "s3",
                endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://minio:9000"),
                aws_access_key_id=os.getenv("S3_ACCESS_KEY", "minio"),
                aws_secret_access_key=os.getenv("S3_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "")),
                config=BotoConfig(signature_version="s3v4") if BotoConfig else None,
            )

    def store(self, image_url: str) -> str:
        if not image_url:
            return ""
        ext = Path(urlparse(image_url).path).suffix or ".img"
        key = f"news/thumbnails/{hashlib.sha256(image_url.encode()).hexdigest()}{ext}"
        if self.client is None:
            return f"disabled://{self.bucket}/{key}"
        if image_url.startswith("file://"):
            data = Path(urlparse(image_url).path).read_bytes()
            content_type = "image/jpeg"
        else:
            with urlopen(Request(image_url, headers=get_headers(image_url)), timeout=20) as response:  # noqa: S310
                data = response.read()
                content_type = response.headers.get_content_type() or "application/octet-stream"
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)
        return f"s3://{self.bucket}/{key}"


class NewsCrawler:
    def __init__(self, *, firecrawl: FirecrawlClient | None = None, thumbnail_store: ThumbnailStore | None = None, max_articles_per_source: int | None = None, rate_limit_seconds: float = 1.0) -> None:
        self.firecrawl = firecrawl or FirecrawlClient()
        self.thumbnail_store = thumbnail_store or ThumbnailStore()
        self.max_articles_per_source = max_articles_per_source or int(os.getenv("NEWS_MAX_ARTICLES_PER_SOURCE", "25"))
        self.rate_limit_seconds = rate_limit_seconds
        self._last_domain_fetch: dict[str, float] = {}

    async def crawl(self, sources: list[str] | None = None, repository: "NewsRepository | None" = None) -> list[NewsArticle]:
        articles: list[NewsArticle] = []
        for source in sources or list(NEWS_SOURCES):
            if not self._robots_allowed(source):
                continue
            try:
                listing = await self._fetch(source)
                for url in self._discover_article_urls(listing, source):
                    if not self._robots_allowed(url):
                        continue
                    payload = await self._fetch(url)
                    article = await self.extract_article(payload, url, repository=repository)
                    if article:
                        articles.append(article)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("News crawl failed for %s: %s", source, exc)
        return dedupe_articles(articles)

    async def extract_article(self, payload: dict[str, Any], url: str, repository: "NewsRepository | None" = None) -> NewsArticle | None:
        html = payload.get("html") or ""
        markdown = payload.get("markdown") or payload.get("content") or ""
        soup = BeautifulSoup(html or markdown, "lxml")
        text = markdown or soup.get_text("\n", strip=True)
        if not text or not any(hint in text.lower() for hint in NEWS_HINTS):
            return None
        title = self._title(soup, text)
        published_at = self._published_at(soup, text)
        image = self._image(soup, url)
        category, tags, summary = await self._classify(text, title)
        related_ids = repository.resolve_entity_ids(tags + [title]) if repository else []
        stored_image = self.thumbnail_store.store(image) if image else ""
        return NewsArticle(
            title=title,
            summary=summary,
            content_url=url,
            source_name=source_name(url),
            category=category,
            tags=tags,
            published_at=published_at,
            scraped_at=utc_now(),
            related_entity_ids=related_ids,
            image_url=stored_image or image,
            is_featured=self._is_featured(category, text),
            raw_payload={"image_source_url": image, "text_preview": text[:1000]},
        )

    async def _classify(self, text: str, title: str) -> tuple[str, list[str], str]:
        if litellm is None or os.getenv("NEWS_LLM_ENABLED", "false").lower() != "true":
            return fallback_classify(f"{title}\n{text}")
        prompt = (
            "Summarize and classify this education news article for CollegeCue. "
            "Return only JSON with keys summary, category, tags. "
            "summary must be <=200 words; category must be one of " + ", ".join(sorted(CATEGORIES)) + ".\n\n" + f"TITLE: {title}\nARTICLE:\n{text[:50000]}"
        )
        for attempt in range(3):
            try:
                response = await litellm.acompletion(
                    model=os.getenv("NEWS_LLM_MODEL", "claude-sonnet-4-20250514"),
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0,
                    response_format={"type": "json_object"},
                )
                data = json.loads(response["choices"][0]["message"]["content"])
                category = data.get("category") if data.get("category") in CATEGORIES else fallback_classify(text)[0]
                return category, [str(t) for t in data.get("tags", [])][:20], truncate_words(data.get("summary", text), 200)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("News LLM classification failed for %s attempt=%s: %s", title, attempt + 1, exc)
                await asyncio.sleep(max(0.0, add_jitter(2**attempt)))
        return fallback_classify(f"{title}\n{text}")

    async def _fetch(self, url: str) -> dict[str, Any]:
        await self._rate_limit(url)
        scraped = self.firecrawl.scrape(url)
        if scraped:
            return scraped
        if url.startswith("file://"):
            html = Path(urlparse(url).path).read_text(encoding="utf-8")
            return {"html": html, "markdown": BeautifulSoup(html, "lxml").get_text("\n", strip=True)}
        with urlopen(Request(url, headers=get_headers(url)), timeout=30) as response:  # noqa: S310
            html = response.read().decode(response.headers.get_content_charset() or "utf-8", errors="replace")
            return {"html": html, "markdown": BeautifulSoup(html, "lxml").get_text("\n", strip=True)}

    def _discover_article_urls(self, payload: dict[str, Any], source_url: str) -> list[str]:
        html = payload.get("html") or ""
        links = payload.get("links") or []
        urls: list[str] = []
        if isinstance(links, list):
            for link in links:
                if isinstance(link, dict):
                    href = link.get("url") or link.get("href")
                else:
                    href = str(link)
                if href:
                    urls.append(urljoin(source_url, href))
        markdown = payload.get("markdown") or ""
        urls.extend(urljoin(source_url, href) for href in re.findall(r"\[[^\]]+\]\(([^)]+)\)", markdown))
        soup = BeautifulSoup(html or markdown or "", "lxml")
        urls.extend(urljoin(source_url, a.get("href")) for a in soup.select("a[href]") if a.get("href"))
        filtered = [u.split("#", 1)[0] for u in urls if self._looks_like_article(u)]
        if source_url.startswith("file://") and not filtered:
            filtered = [source_url]
        return list(dict.fromkeys(filtered))[: self.max_articles_per_source]

    @staticmethod
    def _looks_like_article(url: str) -> bool:
        lower = url.lower()
        if any(ext in lower for ext in (".jpg", ".png", ".pdf", ".zip")):
            return False
        return any(token in lower for token in ("education", "news", "exam", "admission", "result", "scholarship", "college", "university", "article"))

    async def _rate_limit(self, url: str) -> None:
        domain = urlparse(url).netloc or "file"
        elapsed = time.monotonic() - self._last_domain_fetch.get(domain, 0)
        if elapsed < self.rate_limit_seconds:
            await asyncio.sleep(max(0.0, add_jitter(self.rate_limit_seconds - elapsed)))
        self._last_domain_fetch[domain] = time.monotonic()

    def _robots_allowed(self, url: str) -> bool:
        if url.startswith("file://"):
            return True
        parsed = urlparse(url)
        rp = robotparser.RobotFileParser(f"{parsed.scheme}://{parsed.netloc}/robots.txt")
        try:
            rp.read()
            return rp.can_fetch(get_random_ua(), url)
        except Exception:
            return True

    @staticmethod
    def _title(soup: BeautifulSoup, text: str) -> str:
        node = soup.find(["h1", "title", "h2"])
        if node and node.get_text(" ", strip=True):
            return node.get_text(" ", strip=True)[:300]
        return re.split(r"[.\n]", text.strip())[0][:300] or "Untitled News"

    @staticmethod
    def _published_at(soup: BeautifulSoup, text: str) -> str | None:
        node = soup.select_one("time[datetime], meta[property='article:published_time'], meta[name='date'], meta[name='publish-date']")
        value = node.get("datetime") or node.get("content") if node else None
        return parse_date(value) or parse_date(text[:500])

    @staticmethod
    def _image(soup: BeautifulSoup, url: str) -> str:
        node = soup.select_one("meta[property='og:image'], img[src]")
        value = node.get("content") or node.get("src") if node else ""
        return urljoin(url, value) if value else ""

    @staticmethod
    def _is_featured(category: str, text: str) -> bool:
        return category in {"exam_notification", "result", "admission_update", "policy"} or any(k in text.lower() for k in ("breaking", "important", "major"))


def dedupe_articles(articles: list[NewsArticle]) -> list[NewsArticle]:
    out: dict[tuple[str, str, str], NewsArticle] = {}
    seen_urls: set[str] = set()
    for article in articles:
        if article.content_url in seen_urls:
            continue
        seen_urls.add(article.content_url)
        key = (article.title.lower(), article.source_name.lower(), article.published_at or "")
        out.setdefault(key, article)
    return list(out.values())

SQLITE_CREATE_INSTITUTIONS = """
CREATE TABLE IF NOT EXISTS institutions(
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    source_url TEXT UNIQUE,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
)
"""

SQLITE_CREATE_NEWS = """
CREATE TABLE IF NOT EXISTS news_articles(
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    content_url TEXT NOT NULL UNIQUE,
    source_name TEXT NOT NULL,
    category TEXT NOT NULL,
    tags TEXT,
    published_at TEXT,
    scraped_at TEXT,
    related_entity_ids TEXT,
    image_url TEXT,
    is_featured INTEGER DEFAULT 0,
    raw_payload TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
)
"""

SQLITE_UPSERT_NEWS = """
INSERT INTO news_articles(title,summary,content_url,source_name,category,tags,published_at,scraped_at,related_entity_ids,image_url,is_featured,raw_payload)
VALUES(:title,:summary,:content_url,:source_name,:category,:tags,:published_at,:scraped_at,:related_entity_ids,:image_url,:is_featured,:raw_payload)
ON CONFLICT(content_url) DO UPDATE SET
    title=excluded.title,
    summary=excluded.summary,
    source_name=excluded.source_name,
    category=excluded.category,
    tags=excluded.tags,
    published_at=excluded.published_at,
    scraped_at=excluded.scraped_at,
    related_entity_ids=excluded.related_entity_ids,
    image_url=excluded.image_url,
    is_featured=excluded.is_featured,
    raw_payload=excluded.raw_payload
"""

POSTGRES_UPSERT_NEWS = """
INSERT INTO news_articles(title,summary,content_url,source_name,category,tags,published_at,scraped_at,related_entity_ids,image_url,is_featured,raw_payload)
VALUES(%(title)s,%(summary)s,%(content_url)s,%(source_name)s,%(category)s,%(tags)s::text[],%(published_at)s,%(scraped_at)s,%(related_entity_ids)s::int[],%(image_url)s,%(is_featured)s,%(raw_payload)s::jsonb)
ON CONFLICT(content_url) DO UPDATE SET
    title=EXCLUDED.title,
    summary=EXCLUDED.summary,
    source_name=EXCLUDED.source_name,
    category=EXCLUDED.category,
    tags=EXCLUDED.tags,
    published_at=EXCLUDED.published_at,
    scraped_at=EXCLUDED.scraped_at,
    related_entity_ids=EXCLUDED.related_entity_ids,
    image_url=EXCLUDED.image_url,
    is_featured=EXCLUDED.is_featured,
    raw_payload=EXCLUDED.raw_payload
"""


class NewsRepository:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self.sqlite_path = database_url.replace("sqlite:///", "") if database_url.startswith("sqlite:///") else ""

    def init_sqlite(self) -> None:
        if not self.sqlite_path:
            return
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.execute(SQLITE_CREATE_INSTITUTIONS)
            conn.execute(SQLITE_CREATE_NEWS)
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_news_title_source_published ON news_articles(title, source_name, published_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_news_category_published ON news_articles(category, published_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_news_featured ON news_articles(is_featured, published_at)")
            conn.commit()

    def upsert_many(self, articles: list[NewsArticle]) -> int:
        if self.database_url.startswith("postgresql://"):
            return self._upsert_many_pg(articles)
        self.init_sqlite()
        with sqlite3.connect(self.sqlite_path) as conn:
            for article in articles:
                conn.execute(SQLITE_UPSERT_NEWS, article.to_row())
            conn.commit()
        return len(articles)

    def _upsert_many_pg(self, articles: list[NewsArticle]) -> int:
        if psycopg is None:
            raise RuntimeError("psycopg not installed for PostgreSQL news repository")
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                for article in articles:
                    row = asdict(article)
                    row["raw_payload"] = json.dumps(row.get("raw_payload") or {}, sort_keys=True)
                    cur.execute(POSTGRES_UPSERT_NEWS, row)
                    for entity_id in article.related_entity_ids or []:
                        cur.execute(
                            "INSERT INTO news_article_entities(article_id, entity_id) SELECT id, %s FROM news_articles WHERE content_url=%s ON CONFLICT DO NOTHING",
                            (entity_id, article.content_url),
                        )
            conn.commit()
        return len(articles)

    def list(self, *, category: str | None = None, days: int | None = None, entity_id: int | None = None, featured: bool | None = None, limit: int = 100) -> list[dict[str, Any]]:
        if self.database_url.startswith("postgresql://"):
            return self._list_pg(category=category, days=days, entity_id=entity_id, featured=featured, limit=limit)
        self.init_sqlite()
        where: list[str] = []
        params: list[Any] = []
        if category:
            where.append("category=?")
            params.append(category)
        if days is not None:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
            where.append("COALESCE(published_at, scraped_at) >= ?")
            params.append(cutoff)
        if entity_id is not None:
            where.append("related_entity_ids LIKE ?")
            params.append(f"%{entity_id}%")
        if featured is not None:
            where.append("is_featured=?")
            params.append(1 if featured else 0)
        sql = "SELECT * FROM news_articles" + (" WHERE " + " AND ".join(where) if where else "") + " ORDER BY COALESCE(published_at, scraped_at) DESC, id DESC LIMIT ?"
        params.append(limit)
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = [self._decode_row(dict(row)) for row in conn.execute(sql, params).fetchall()]
        if entity_id is not None:
            rows = [row for row in rows if entity_id in row.get("related_entity_ids", [])]
        return rows

    def get(self, article_id: int) -> dict[str, Any] | None:
        if self.database_url.startswith("postgresql://"):
            return self._get_pg(article_id)
        self.init_sqlite()
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM news_articles WHERE id=?", (article_id,)).fetchone()
            return self._decode_row(dict(row)) if row else None

    def resolve_entity_ids(self, names: list[str]) -> list[int]:
        if self.database_url.startswith("postgresql://"):
            return self._resolve_entity_ids_pg(names)
        self.init_sqlite()
        needles = [n.lower() for n in names if n]
        if not needles:
            return []
        with sqlite3.connect(self.sqlite_path) as conn:
            rows = conn.execute("SELECT id,name FROM institutions").fetchall()
        ids = []
        for entity_id, name in rows:
            low = (name or "").lower()
            if any(low and (low in needle or needle in low) for needle in needles):
                ids.append(entity_id)
        return ids[:20]

    def _list_pg(self, *, category: str | None, days: int | None, entity_id: int | None, featured: bool | None, limit: int) -> list[dict[str, Any]]:
        if psycopg is None:
            raise RuntimeError("psycopg not installed for PostgreSQL news repository")
        where: list[str] = []
        params: list[Any] = []
        if category:
            where.append("category=%s")
            params.append(category)
        if days is not None:
            where.append("COALESCE(published_at, scraped_at::date) >= CURRENT_DATE - (%s * INTERVAL '1 day')")
            params.append(days)
        if entity_id is not None:
            where.append("%s = ANY(related_entity_ids)")
            params.append(entity_id)
        if featured is not None:
            where.append("is_featured=%s")
            params.append(featured)
        sql = "SELECT * FROM news_articles" + (" WHERE " + " AND ".join(where) if where else "") + " ORDER BY COALESCE(published_at, scraped_at::date) DESC, id DESC LIMIT %s"
        params.append(limit)
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(sql, params)
                return [dict(row) for row in cur.fetchall()]

    def _get_pg(self, article_id: int) -> dict[str, Any] | None:
        if psycopg is None:
            raise RuntimeError("psycopg not installed for PostgreSQL news repository")
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute("SELECT * FROM news_articles WHERE id=%s", (article_id,))
                row = cur.fetchone()
                return dict(row) if row else None

    def _resolve_entity_ids_pg(self, names: list[str]) -> list[int]:
        if psycopg is None:
            raise RuntimeError("psycopg not installed for PostgreSQL news repository")
        needles = [n for n in names if n]
        if not needles:
            return []
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id,name FROM institutions")
                rows = cur.fetchall()
        ids = []
        for entity_id, name in rows:
            low = (name or "").lower()
            if any(low and (low in needle.lower() or needle.lower() in low) for needle in needles):
                ids.append(entity_id)
        return ids[:20]

    @staticmethod
    def _decode_row(row: dict[str, Any]) -> dict[str, Any]:
        for field in ("tags", "related_entity_ids"):
            if isinstance(row.get(field), str):
                try:
                    row[field] = json.loads(row[field] or "[]")
                except json.JSONDecodeError:
                    row[field] = []
        row["is_featured"] = bool(row.get("is_featured"))
        return row


def crawl_news_sync(database_url: str, *, sources: list[str] | None = None) -> dict[str, Any]:
    repository = NewsRepository(database_url)
    articles = asyncio.run(NewsCrawler().crawl(sources=sources, repository=repository))
    saved = repository.upsert_many(articles)
    return {"discovered": len(articles), "saved": saved}
