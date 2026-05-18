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
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib import parse, robotparser
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

LOGGER = logging.getLogger(__name__)

if importlib.util.find_spec("crawl4ai"):
    _crawl4ai = importlib.import_module("crawl4ai")
    AsyncWebCrawler = getattr(_crawl4ai, "AsyncWebCrawler", None)
    BrowserConfig = getattr(_crawl4ai, "BrowserConfig", None)
    CrawlerRunConfig = getattr(_crawl4ai, "CrawlerRunConfig", None)
else:
    AsyncWebCrawler = None
    BrowserConfig = None
    CrawlerRunConfig = None

if importlib.util.find_spec("firecrawl"):
    _firecrawl = importlib.import_module("firecrawl")
    FirecrawlApp = getattr(_firecrawl, "FirecrawlApp", None)
    if FirecrawlApp is None and importlib.util.find_spec("firecrawl.firecrawl"):
        _firecrawl = importlib.import_module("firecrawl.firecrawl")
        FirecrawlApp = getattr(_firecrawl, "FirecrawlApp", None)
else:
    FirecrawlApp = None

litellm = importlib.import_module("litellm") if importlib.util.find_spec("litellm") else None
psycopg = importlib.import_module("psycopg") if importlib.util.find_spec("psycopg") else None

OPEN_ACCESS_SOURCES = ("https://arxiv.org", "https://www.researchgate.net", "https://www.semanticscholar.org", "https://core.ac.uk")
INDIA_SOURCES = ("https://shodhganga.inflibnet.ac.in", "https://irins.org", "https://www.ias.ac.in", "https://insa.nic.in")
UNIVERSITY_PATTERNS = ("/research", "/publications", "/projects")
FIELDS = {"engineering", "medicine", "arts", "commerce", "science", "law", "other"}
TYPES = {"paper", "thesis", "ongoing_project", "patent"}
STATUSES = {"published", "preprint", "ongoing"}
ARXIV_CATEGORIES = ("cs.*", "math.*", "physics.*", "q-bio.*", "q-fin.*", "stat.*", "econ.*", "eess.*")


@dataclass
class ResearchItem:
    title: str
    authors: list[str]
    abstract: str
    type: str
    field: str
    subfield: str = ""
    keywords: list[str] | None = None
    institution_id: int | None = None
    institution_name: str = ""
    published_date: str | None = None
    doi: str = ""
    arxiv_id: str = ""
    pdf_url: str = ""
    source_url: str = ""
    citation_count: int | None = None
    status: str = "published"
    raw_payload: dict[str, Any] | None = None

    def to_row(self) -> dict[str, Any]:
        row = asdict(self)
        row["authors"] = json.dumps(row.get("authors") or [])
        row["keywords"] = json.dumps(row.get("keywords") or [])
        row["raw_payload"] = json.dumps(row.get("raw_payload") or {}, sort_keys=True)
        row["title_author_hash"] = title_author_hash(self.title, self.authors)
        return row


def truncate_words(text: str, limit: int = 500) -> str:
    return " ".join(re.sub(r"\s+", " ", text or "").strip().split()[:limit])


def title_author_hash(title: str, authors: list[str]) -> str:
    normalized = f"{title.strip().lower()}|{'|'.join(sorted(a.strip().lower() for a in authors))}"
    return hashlib.sha256(normalized.encode()).hexdigest()


def parse_date(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip()[:10]
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    match = re.search(r"(20\d{2})", value)
    return date(int(match.group(1)), 1, 1).isoformat() if match else None


def fallback_enrich(title: str, abstract: str, source_url: str = "") -> tuple[str, str, str, list[str]]:
    text = f"{title}\n{abstract}\n{source_url}".lower()
    if any(k in text for k in ("medical", "medicine", "clinical", "aiims", "health", "biology", "cancer")):
        field = "medicine"
    elif any(k in text for k in ("law", "legal", "court", "justice")):
        field = "law"
    elif any(k in text for k in ("commerce", "finance", "business", "economics", "management", "iim")):
        field = "commerce"
    elif any(k in text for k in ("literature", "history", "arts", "humanities", "language")):
        field = "arts"
    elif any(k in text for k in ("engineering", "computer", "machine learning", "iit", "nit", "algorithm", "robotics")):
        field = "engineering"
    elif any(k in text for k in ("physics", "chemistry", "mathematics", "science")):
        field = "science"
    else:
        field = "other"
    subfield_match = re.search(r"\b(machine learning|artificial intelligence|public health|data science|finance|robotics|chemistry|physics|biology|law)\b", text)
    subfield = subfield_match.group(1) if subfield_match else ""
    keywords = sorted(set(re.findall(r"\b(?:AI|IIT|IIM|AIIMS|NIT|machine learning|data science|robotics|health|finance|law|physics|chemistry|biology)\b", f"{title} {abstract}", re.I)), key=str.lower)[:12]
    return truncate_words(abstract, 500), field, subfield, keywords


def infer_type_status(source_url: str, text: str) -> tuple[str, str]:
    lower = f"{source_url} {text}".lower()
    if "shodhganga" in lower or "thesis" in lower:
        return "thesis", "published"
    if "patent" in lower:
        return "patent", "published"
    if "project" in lower or "ongoing" in lower:
        return "ongoing_project", "ongoing"
    if "arxiv" in lower:
        return "paper", "preprint"
    return "paper", "published"


class FirecrawlClient:
    def __init__(self) -> None:
        self.enabled = FirecrawlApp is not None and bool(os.getenv("FIRECRAWL_API_KEY"))
        self.app = FirecrawlApp(api_key=os.getenv("FIRECRAWL_API_KEY")) if self.enabled else None

    def search(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        if not self.app:
            return []
        result = self.app.search(query, limit=limit)
        if isinstance(result, dict):
            return result.get("data") or result.get("results") or []
        return result or []

    def scrape(self, url: str) -> dict[str, Any]:
        if not self.app:
            return {}
        return self.app.scrape_url(url, formats=["markdown", "html", "links"])


class ResearchCrawler:
    def __init__(self, *, firecrawl: FirecrawlClient | None = None, max_pages: int | None = None, rate_limit_seconds: float = 1.0) -> None:
        self.firecrawl = firecrawl or FirecrawlClient()
        self.max_pages = max_pages or int(os.getenv("RESEARCH_MAX_PAGES", "50"))
        self.rate_limit_seconds = rate_limit_seconds
        self._last_domain_fetch: dict[str, float] = {}

    async def crawl(self, *, queries: list[str] | None = None, seed_urls: list[str] | None = None, include_arxiv: bool = True, repository: "ResearchRepository | None" = None) -> list[ResearchItem]:
        items: list[ResearchItem] = []
        if include_arxiv:
            items.extend(self.fetch_arxiv_bulk())
        urls = await self.discover(queries=queries, seed_urls=seed_urls or [])
        for url in urls:
            if not self._robots_allowed(url):
                continue
            try:
                payload = await self._fetch(url)
                item = await self.extract_item(payload, url, repository=repository)
                if item:
                    items.append(item)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Research crawl failed for %s: %s", url, exc)
        return dedupe_items(items)

    async def discover(self, *, queries: list[str] | None = None, seed_urls: list[str]) -> list[str]:
        urls = list(seed_urls)
        for seed in seed_urls:
            try:
                payload = await self._fetch(seed)
                urls.extend(self._discover_links(payload, seed))
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Research seed discovery failed for %s: %s", seed, exc)
        for query in queries or ["arxiv.org Indian researchers 2025", "shodhganga PhD thesis 2025 engineering", "IIT research publications projects 2025", "AIIMS ongoing research projects"]:
            for item in self.firecrawl.search(query, limit=10):
                url = item.get("url") or item.get("link")
                if url:
                    urls.append(url)
        return list(dict.fromkeys(urls))[: self.max_pages]

    def fetch_arxiv_bulk(self, *, categories: tuple[str, ...] = ARXIV_CATEGORIES, max_results: int | None = None) -> list[ResearchItem]:
        if os.getenv("RESEARCH_ARXIV_ENABLED", "true").lower() == "false":
            return []
        max_results = max_results or int(os.getenv("RESEARCH_ARXIV_MAX_RESULTS", "25"))
        query = " OR ".join(f"cat:{cat}" for cat in categories)
        url = "https://export.arxiv.org/api/query?" + parse.urlencode({"search_query": query, "start": 0, "max_results": max_results, "sortBy": "submittedDate", "sortOrder": "descending"})
        try:
            with urlopen(Request(url, headers={"User-Agent": os.getenv("CRAWLER_USER_AGENT", "CollegeCueBot/1.0")}), timeout=30) as response:  # noqa: S310
                xml = response.read()
            return parse_arxiv_feed(xml)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("ArXiv bulk ingestion failed: %s", exc)
            return []

    async def extract_item(self, payload: dict[str, Any], url: str, repository: "ResearchRepository | None" = None) -> ResearchItem | None:
        html = payload.get("html") or ""
        markdown = payload.get("markdown") or payload.get("content") or ""
        soup = BeautifulSoup(html or markdown, "lxml")
        text = markdown or soup.get_text("\n", strip=True)
        if not text:
            return None
        title = self._title(soup, text)
        authors = self._authors(soup, text)
        abstract = self._abstract(soup, text)
        enriched_abstract, field, subfield, keywords = await self._enrich(title, abstract, url)
        item_type, status = infer_type_status(url, text)
        institution_id, institution_name = repository.resolve_institution(title, text) if repository else (None, "")
        return ResearchItem(
            title=title,
            authors=authors,
            abstract=enriched_abstract,
            type=item_type,
            field=field,
            subfield=subfield,
            keywords=keywords,
            institution_id=institution_id,
            institution_name=institution_name,
            published_date=self._published_date(soup, text),
            doi=self._match(text, r"\bdoi\s*[:：]?\s*(10\.\d{4,9}/[-._;()/:A-Z0-9]+)"),
            arxiv_id=self._match(text, r"arXiv\s*[:：]?\s*(\d{4}\.\d{4,5}(?:v\d+)?)"),
            pdf_url=self._pdf_url(soup, url),
            source_url=url,
            citation_count=self._citation_count(text),
            status=status,
            raw_payload={"text_preview": text[:1000]},
        )

    async def _enrich(self, title: str, abstract: str, url: str) -> tuple[str, str, str, list[str]]:
        if litellm is None or os.getenv("RESEARCH_LLM_ENABLED", "false").lower() != "true":
            return fallback_enrich(title, abstract, url)
        prompt = (
            "Summarize and classify this research item for CollegeCue. Return only JSON with keys abstract, field, subfield, keywords. "
            "abstract must be <=500 words; field must be one of " + ", ".join(sorted(FIELDS)) + f".\nTITLE: {title}\nABSTRACT:\n{abstract[:20000]}"
        )
        for attempt in range(3):
            try:
                response = await litellm.acompletion(
                    model=os.getenv("RESEARCH_LLM_MODEL", "claude-sonnet-4-20250514"),
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0,
                    response_format={"type": "json_object"},
                )
                data = json.loads(response["choices"][0]["message"]["content"])
                field = data.get("field") if data.get("field") in FIELDS else fallback_enrich(title, abstract, url)[1]
                return truncate_words(data.get("abstract", abstract), 500), field, str(data.get("subfield", ""))[:120], [str(k) for k in data.get("keywords", [])][:20]
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Research LLM enrichment failed for %s attempt=%s: %s", title, attempt + 1, exc)
                await asyncio.sleep(2**attempt)
        return fallback_enrich(title, abstract, url)

    async def _fetch(self, url: str) -> dict[str, Any]:
        await self._rate_limit(url)
        scraped = self.firecrawl.scrape(url)
        if scraped:
            return scraped
        if self._needs_browser(url) and AsyncWebCrawler is not None and not url.startswith("file://"):
            browser_config = BrowserConfig(headless=True, java_script_enabled=True)
            run_config = CrawlerRunConfig() if CrawlerRunConfig is not None else None
            async with AsyncWebCrawler(config=browser_config) as crawler:
                result = await crawler.arun(url=url, config=run_config)
            html = getattr(result, "html", "") or getattr(result, "cleaned_html", "") or ""
            if html:
                return {"html": html, "markdown": BeautifulSoup(html, "lxml").get_text("\n", strip=True)}
        if url.startswith("file://"):
            html = Path(urlparse(url).path).read_text(encoding="utf-8")
            return {"html": html, "markdown": BeautifulSoup(html, "lxml").get_text("\n", strip=True)}
        with urlopen(Request(url, headers={"User-Agent": os.getenv("CRAWLER_USER_AGENT", "CollegeCueBot/1.0")}), timeout=30) as response:  # noqa: S310
            html = response.read().decode(response.headers.get_content_charset() or "utf-8", errors="replace")
            return {"html": html, "markdown": BeautifulSoup(html, "lxml").get_text("\n", strip=True)}

    def _discover_links(self, payload: dict[str, Any], source_url: str) -> list[str]:
        html = payload.get("html") or ""
        markdown = payload.get("markdown") or ""
        urls = [urljoin(source_url, href) for href in re.findall(r"\[[^\]]+\]\(([^)]+)\)", markdown)]
        soup = BeautifulSoup(html or markdown, "lxml")
        urls.extend(urljoin(source_url, a.get("href")) for a in soup.select("a[href]") if a.get("href"))
        filtered = [u for u in urls if self._looks_research_url(u)]
        return list(dict.fromkeys(filtered))[: self.max_pages]

    @staticmethod
    def _needs_browser(url: str) -> bool:
        lower = url.lower()
        return any(token in lower for token in ("shodhganga", "irins", "/research", "/publications", "/projects"))

    @staticmethod
    def _looks_research_url(url: str) -> bool:
        lower = url.lower()
        return any(token in lower for token in ("research", "publication", "project", "paper", "thesis", "patent", "arxiv", "handle")) and not any(lower.endswith(ext) for ext in (".jpg", ".png", ".zip"))

    async def _rate_limit(self, url: str) -> None:
        domain = urlparse(url).netloc or "file"
        elapsed = time.monotonic() - self._last_domain_fetch.get(domain, 0)
        if elapsed < self.rate_limit_seconds:
            await asyncio.sleep(self.rate_limit_seconds - elapsed)
        self._last_domain_fetch[domain] = time.monotonic()

    def _robots_allowed(self, url: str) -> bool:
        if url.startswith("file://"):
            return True
        parsed = urlparse(url)
        rp = robotparser.RobotFileParser(f"{parsed.scheme}://{parsed.netloc}/robots.txt")
        try:
            rp.read()
            return rp.can_fetch(os.getenv("CRAWLER_USER_AGENT", "CollegeCueBot/1.0"), url)
        except Exception:
            return True

    @staticmethod
    def _title(soup: BeautifulSoup, text: str) -> str:
        node = soup.find(["h1", "title", "h2"])
        return node.get_text(" ", strip=True)[:500] if node and node.get_text(" ", strip=True) else re.split(r"[.\n]", text.strip())[0][:500]

    @staticmethod
    def _authors(soup: BeautifulSoup, text: str) -> list[str]:
        nodes = soup.select("meta[name='citation_author'], .authors, [class*='author']")
        authors = [n.get("content") or n.get_text(" ", strip=True) for n in nodes]
        if authors:
            return [a for a in authors if a][:50]
        match = re.search(r"authors?\s*[:：-]\s*([^\n.]+)", text, re.I)
        return [a.strip() for a in re.split(r",|;| and ", match.group(1)) if a.strip()] if match else []

    @staticmethod
    def _abstract(soup: BeautifulSoup, text: str) -> str:
        node = soup.select_one("meta[name='description'], meta[name='citation_abstract'], .abstract, #abstract")
        value = node.get("content") or node.get_text(" ", strip=True) if node else ""
        if value:
            return truncate_words(value, 500)
        match = re.search(r"abstract\s*[:：-]\s*(.+?)(?:\n\s*(?:keywords|doi|authors?)\b|$)", text, re.I | re.S)
        return truncate_words(match.group(1), 500) if match else truncate_words(text, 500)

    @staticmethod
    def _published_date(soup: BeautifulSoup, text: str) -> str | None:
        node = soup.select_one("meta[name='citation_publication_date'], meta[name='dc.date'], time[datetime]")
        value = node.get("content") or node.get("datetime") if node else None
        return parse_date(value) or parse_date(text[:500])

    @staticmethod
    def _match(text: str, pattern: str) -> str:
        match = re.search(pattern, text or "", re.I)
        return match.group(1).strip().rstrip(".") if match else ""

    @staticmethod
    def _pdf_url(soup: BeautifulSoup, url: str) -> str:
        node = soup.select_one("meta[name='citation_pdf_url']")
        if node and node.get("content"):
            return urljoin(url, node.get("content"))
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if href.lower().endswith(".pdf") or "pdf" in a.get_text(" ", strip=True).lower():
                return urljoin(url, href)
        return ""

    @staticmethod
    def _citation_count(text: str) -> int | None:
        match = re.search(r"(?:citations?|cited by)\D{0,20}(\d+)", text, re.I)
        return int(match.group(1)) if match else None

def parse_arxiv_feed(xml: bytes) -> list[ResearchItem]:
    ns = {"atom": "http://www.w3.org/2005/Atom", "arxiv": "http://arxiv.org/schemas/atom"}
    root = ET.fromstring(xml)
    items: list[ResearchItem] = []
    for entry in root.findall("atom:entry", ns):
        title = " ".join((entry.findtext("atom:title", default="", namespaces=ns) or "").split())
        abstract = truncate_words(entry.findtext("atom:summary", default="", namespaces=ns) or "", 500)
        authors = [a.findtext("atom:name", default="", namespaces=ns) for a in entry.findall("atom:author", ns)]
        entry_id = entry.findtext("atom:id", default="", namespaces=ns) or ""
        arxiv_id = entry_id.rsplit("/", 1)[-1]
        pdf_url = ""
        for link in entry.findall("atom:link", ns):
            if link.attrib.get("title") == "pdf" or link.attrib.get("type") == "application/pdf":
                pdf_url = link.attrib.get("href", "")
        doi = entry.findtext("arxiv:doi", default="", namespaces=ns) or ""
        _, field, subfield, keywords = fallback_enrich(title, abstract, entry_id)
        items.append(
            ResearchItem(
                title=title,
                authors=[a for a in authors if a],
                abstract=abstract,
                type="paper",
                field=field,
                subfield=subfield,
                keywords=keywords,
                published_date=parse_date(entry.findtext("atom:published", default="", namespaces=ns)),
                doi=doi,
                arxiv_id=arxiv_id,
                pdf_url=pdf_url,
                source_url=entry_id,
                status="preprint",
                raw_payload={"source": "arxiv"},
            )
        )
    return items


def dedupe_items(items: list[ResearchItem]) -> list[ResearchItem]:
    out: dict[str, ResearchItem] = {}
    for item in items:
        key = item.doi.lower() or item.arxiv_id.lower() or title_author_hash(item.title, item.authors)
        out.setdefault(key, item)
    return list(out.values())


SQLITE_CREATE_RESEARCH = """
CREATE TABLE IF NOT EXISTS research_items(
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    authors TEXT NOT NULL,
    abstract TEXT,
    type TEXT NOT NULL,
    field TEXT NOT NULL,
    subfield TEXT,
    keywords TEXT,
    institution_id INTEGER,
    institution_name TEXT,
    published_date TEXT,
    doi TEXT,
    arxiv_id TEXT,
    pdf_url TEXT,
    source_url TEXT,
    citation_count INTEGER,
    status TEXT,
    title_author_hash TEXT NOT NULL,
    raw_payload TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
)
"""

SQLITE_CREATE_INSTITUTIONS = """
CREATE TABLE IF NOT EXISTS institutions(
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    source_url TEXT UNIQUE,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
)
"""

SQLITE_UPSERT_RESEARCH = """
INSERT INTO research_items(title,authors,abstract,type,field,subfield,keywords,institution_id,institution_name,published_date,doi,arxiv_id,pdf_url,source_url,citation_count,status,title_author_hash,raw_payload,updated_at)
VALUES(:title,:authors,:abstract,:type,:field,:subfield,:keywords,:institution_id,:institution_name,:published_date,:doi,:arxiv_id,:pdf_url,:source_url,:citation_count,:status,:title_author_hash,:raw_payload,:updated_at)
ON CONFLICT(title_author_hash) DO UPDATE SET
    abstract=excluded.abstract,
    type=excluded.type,
    field=excluded.field,
    subfield=excluded.subfield,
    keywords=excluded.keywords,
    institution_id=excluded.institution_id,
    institution_name=excluded.institution_name,
    published_date=excluded.published_date,
    doi=excluded.doi,
    arxiv_id=excluded.arxiv_id,
    pdf_url=excluded.pdf_url,
    source_url=excluded.source_url,
    citation_count=excluded.citation_count,
    status=excluded.status,
    raw_payload=excluded.raw_payload,
    updated_at=excluded.updated_at
"""

POSTGRES_UPSERT_RESEARCH = """
INSERT INTO research_items(title,authors,abstract,type,field,subfield,keywords,institution_id,institution_name,published_date,doi,arxiv_id,pdf_url,source_url,citation_count,status,title_author_hash,raw_payload,updated_at)
VALUES(%(title)s,%(authors)s::text[],%(abstract)s,%(type)s,%(field)s,%(subfield)s,%(keywords)s::text[],%(institution_id)s,%(institution_name)s,%(published_date)s,%(doi)s,%(arxiv_id)s,%(pdf_url)s,%(source_url)s,%(citation_count)s,%(status)s,%(title_author_hash)s,%(raw_payload)s::jsonb,NOW())
ON CONFLICT(title_author_hash) DO UPDATE SET
    abstract=EXCLUDED.abstract,
    type=EXCLUDED.type,
    field=EXCLUDED.field,
    subfield=EXCLUDED.subfield,
    keywords=EXCLUDED.keywords,
    institution_id=EXCLUDED.institution_id,
    institution_name=EXCLUDED.institution_name,
    published_date=EXCLUDED.published_date,
    doi=EXCLUDED.doi,
    arxiv_id=EXCLUDED.arxiv_id,
    pdf_url=EXCLUDED.pdf_url,
    source_url=EXCLUDED.source_url,
    citation_count=EXCLUDED.citation_count,
    status=EXCLUDED.status,
    raw_payload=EXCLUDED.raw_payload,
    updated_at=NOW()
"""


class ResearchRepository:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self.sqlite_path = database_url.replace("sqlite:///", "") if database_url.startswith("sqlite:///") else ""

    def init_sqlite(self) -> None:
        if not self.sqlite_path:
            return
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.execute(SQLITE_CREATE_INSTITUTIONS)
            conn.execute(SQLITE_CREATE_RESEARCH)
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_research_doi ON research_items(doi) WHERE doi IS NOT NULL AND doi != ''")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_research_arxiv ON research_items(arxiv_id) WHERE arxiv_id IS NOT NULL AND arxiv_id != ''")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_research_title_author_hash ON research_items(title_author_hash)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_research_field_type_year ON research_items(field, type, published_date)")
            conn.commit()

    def upsert_many(self, items: list[ResearchItem]) -> int:
        if self.database_url.startswith("postgresql://"):
            return self._upsert_many_pg(items)
        self.init_sqlite()
        with sqlite3.connect(self.sqlite_path) as conn:
            for item in items:
                row = item.to_row()
                existing_hash = self._existing_hash_sqlite(conn, row.get("doi"), row.get("arxiv_id"), row["title_author_hash"])
                if existing_hash:
                    row["title_author_hash"] = existing_hash
                row["updated_at"] = datetime.now(timezone.utc).isoformat()
                conn.execute(SQLITE_UPSERT_RESEARCH, row)
            conn.commit()
        return len(items)

    @staticmethod
    def _existing_hash_sqlite(conn: sqlite3.Connection, doi: str | None, arxiv_id: str | None, fallback_hash: str) -> str | None:
        clauses = ["title_author_hash=?"]
        params: list[Any] = [fallback_hash]
        if doi:
            clauses.append("doi=?")
            params.append(doi)
        if arxiv_id:
            clauses.append("arxiv_id=?")
            params.append(arxiv_id)
        row = conn.execute(f"SELECT title_author_hash FROM research_items WHERE {' OR '.join(clauses)} LIMIT 1", params).fetchone()
        return row[0] if row else None

    def _upsert_many_pg(self, items: list[ResearchItem]) -> int:
        if psycopg is None:
            raise RuntimeError("psycopg not installed for PostgreSQL research repository")
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                for item in items:
                    row = asdict(item)
                    row["title_author_hash"] = title_author_hash(item.title, item.authors)
                    existing_hash = self._existing_hash_pg(cur, row.get("doi"), row.get("arxiv_id"), row["title_author_hash"])
                    if existing_hash:
                        row["title_author_hash"] = existing_hash
                    row["raw_payload"] = json.dumps(row.get("raw_payload") or {}, sort_keys=True)
                    cur.execute(POSTGRES_UPSERT_RESEARCH, row)
            conn.commit()
        return len(items)

    @staticmethod
    def _existing_hash_pg(cur: Any, doi: str | None, arxiv_id: str | None, fallback_hash: str) -> str | None:
        clauses = ["title_author_hash=%s"]
        params: list[Any] = [fallback_hash]
        if doi:
            clauses.append("doi=%s")
            params.append(doi)
        if arxiv_id:
            clauses.append("arxiv_id=%s")
            params.append(arxiv_id)
        cur.execute(f"SELECT title_author_hash FROM research_items WHERE {' OR '.join(clauses)} LIMIT 1", params)
        row = cur.fetchone()
        return row[0] if row else None

    def list(self, *, field: str | None = None, item_type: str | None = None, year: int | None = None, institution_id: int | None = None, limit: int = 100) -> list[dict[str, Any]]:
        if self.database_url.startswith("postgresql://"):
            return self._list_pg(field=field, item_type=item_type, year=year, institution_id=institution_id, limit=limit)
        self.init_sqlite()
        where: list[str] = []
        params: list[Any] = []
        if field:
            where.append("field=?")
            params.append(field)
        if item_type:
            where.append("type=?")
            params.append(item_type)
        if year:
            where.append("substr(published_date,1,4)=?")
            params.append(str(year))
        if institution_id is not None:
            where.append("institution_id=?")
            params.append(institution_id)
        sql = "SELECT * FROM research_items" + (" WHERE " + " AND ".join(where) if where else "") + " ORDER BY COALESCE(published_date, created_at) DESC, id DESC LIMIT ?"
        params.append(limit)
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.row_factory = sqlite3.Row
            return [self._decode_row(dict(row)) for row in conn.execute(sql, params).fetchall()]

    def get(self, item_id: int) -> dict[str, Any] | None:
        if self.database_url.startswith("postgresql://"):
            return self._get_pg(item_id)
        self.init_sqlite()
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM research_items WHERE id=?", (item_id,)).fetchone()
            return self._decode_row(dict(row)) if row else None

    def search(self, query: str, *, limit: int = 100) -> list[dict[str, Any]]:
        if self.database_url.startswith("postgresql://"):
            return self._search_pg(query, limit=limit)
        self.init_sqlite()
        terms = [term for term in query.lower().split() if term]
        where = " AND ".join(["lower(title || ' ' || abstract || ' ' || institution_name || ' ' || field || ' ' || subfield || ' ' || keywords) LIKE ?" for _ in terms]) or "1=1"
        params = [f"%{term}%" for term in terms]
        params.append(limit)
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(f"SELECT * FROM research_items WHERE {where} ORDER BY id DESC LIMIT ?", params).fetchall()
            return [self._decode_row(dict(row)) for row in rows]

    def resolve_institution(self, title: str, text: str) -> tuple[int | None, str]:
        if self.database_url.startswith("postgresql://"):
            return self._resolve_institution_pg(title, text)
        self.init_sqlite()
        haystack = f"{title}\n{text}".lower()
        with sqlite3.connect(self.sqlite_path) as conn:
            rows = conn.execute("SELECT id,name FROM institutions").fetchall()
        for institution_id, name in rows:
            if name and name.lower() in haystack:
                return institution_id, name
        return None, ""

    def _list_pg(self, *, field: str | None, item_type: str | None, year: int | None, institution_id: int | None, limit: int) -> list[dict[str, Any]]:
        if psycopg is None:
            raise RuntimeError("psycopg not installed for PostgreSQL research repository")
        where: list[str] = []
        params: list[Any] = []
        if field:
            where.append("field=%s")
            params.append(field)
        if item_type:
            where.append("type=%s")
            params.append(item_type)
        if year:
            where.append("EXTRACT(YEAR FROM published_date)=%s")
            params.append(year)
        if institution_id is not None:
            where.append("institution_id=%s")
            params.append(institution_id)
        sql = "SELECT * FROM research_items" + (" WHERE " + " AND ".join(where) if where else "") + " ORDER BY COALESCE(published_date, created_at::date) DESC, id DESC LIMIT %s"
        params.append(limit)
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(sql, params)
                return [dict(row) for row in cur.fetchall()]

    def _get_pg(self, item_id: int) -> dict[str, Any] | None:
        if psycopg is None:
            raise RuntimeError("psycopg not installed for PostgreSQL research repository")
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute("SELECT * FROM research_items WHERE id=%s", (item_id,))
                row = cur.fetchone()
                return dict(row) if row else None

    def _search_pg(self, query: str, *, limit: int) -> list[dict[str, Any]]:
        if psycopg is None:
            raise RuntimeError("psycopg not installed for PostgreSQL research repository")
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute("SELECT * FROM research_items WHERE to_tsvector('english', title || ' ' || abstract || ' ' || institution_name || ' ' || field || ' ' || subfield) @@ plainto_tsquery('english', %s) ORDER BY id DESC LIMIT %s", (query, limit))
                return [dict(row) for row in cur.fetchall()]

    def _resolve_institution_pg(self, title: str, text: str) -> tuple[int | None, str]:
        if psycopg is None:
            raise RuntimeError("psycopg not installed for PostgreSQL research repository")
        haystack = f"{title}\n{text}".lower()
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id,name FROM institutions")
                rows = cur.fetchall()
        for institution_id, name in rows:
            if name and name.lower() in haystack:
                return institution_id, name
        return None, ""

    @staticmethod
    def _decode_row(row: dict[str, Any]) -> dict[str, Any]:
        for field in ("authors", "keywords"):
            if isinstance(row.get(field), str):
                try:
                    row[field] = json.loads(row[field] or "[]")
                except json.JSONDecodeError:
                    row[field] = []
        return row


def crawl_research_sync(database_url: str, *, queries: list[str] | None = None, seed_urls: list[str] | None = None, include_arxiv: bool = True) -> dict[str, Any]:
    repository = ResearchRepository(database_url)
    items = asyncio.run(ResearchCrawler().crawl(queries=queries, seed_urls=seed_urls or [], include_arxiv=include_arxiv, repository=repository))
    saved = repository.upsert_many(items)
    return {"discovered": len(items), "saved": saved}
