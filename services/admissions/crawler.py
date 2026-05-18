from __future__ import annotations

import asyncio
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

psycopg = importlib.import_module("psycopg") if importlib.util.find_spec("psycopg") else None

ADMISSION_PATTERNS = ("/admission", "/admissions", "/apply", "/notice", "/notices", "/entrance", "/cet", "/application")
FORM_HINTS = ("apply", "application", "registration", "form", "portal", "login")
PROGRAM_TYPES = ("UG", "PG", "PhD", "Diploma", "Certificate")
DIRECT_SOURCE_DOMAINS = ("nta.ac.in", "shiksha.com", "careers360.com", "commonapp.org", "ucas.com")
STATE_BOARD_QUERIES = (
    "site:mahacet.org 2025 application form",
    "site:cetcell.mahacet.org 2025 admission application form",
    "site:wbjeeb.nic.in 2025 application form",
    "site:kea.kar.nic.in 2025 cet application form",
    "site:tneaonline.org 2025 admission application form",
    "site:jeecup.admissions.nic.in 2025 application form",
)


@dataclass
class AdmissionNotice:
    entity_id: int | None
    entity_name: str
    admission_type: str
    program_name: str
    intake_year: int
    application_start_date: str | None = None
    application_end_date: str | None = None
    exam_date: str | None = None
    result_date: str | None = None
    application_link: str = ""
    eligibility_text: str = ""
    fee_inr: int | None = None
    mode: str = "online"
    status: str = "upcoming"
    country: str = "India"
    state: str = ""
    source_url: str = ""
    source_name: str = ""
    raw_payload: dict[str, Any] | None = None

    def to_row(self) -> dict[str, Any]:
        data = asdict(self)
        data["raw_payload"] = json.dumps(data.get("raw_payload") or {}, sort_keys=True)
        return data


def _today() -> date:
    override = os.getenv("ADMISSIONS_TODAY")
    if override:
        return date.fromisoformat(override)
    return datetime.now(timezone.utc).date()


def classify_status(start: str | None, end: str | None, *, today: date | None = None) -> str:
    today = today or _today()
    start_date = _parse_date(start)
    end_date = _parse_date(end)
    if end_date and end_date < today:
        return "closed"
    if start_date and start_date > today:
        return "upcoming"
    return "ongoing"


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d %B %Y", "%B %d, %Y", "%d %b %Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def normalize_date(value: str | None, default_year: int | None = None) -> str | None:
    if not value:
        return None
    parsed = _parse_date(value)
    if parsed:
        return parsed.isoformat()
    year = default_year or _today().year
    match = re.search(r"(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,9})(?:\s+(20\d{2}))?", value)
    if match:
        month_names = {m.lower(): i for i, m in enumerate(["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"], 1)}
        month = month_names.get(match.group(2).lower()) or month_names.get(match.group(2).lower()[:3])
        if month:
            return date(int(match.group(3) or year), month, int(match.group(1))).isoformat()
    return None


def infer_admission_type(text: str) -> str:
    lower = text.lower()
    if re.search(r"\b(ph\.?d|doctoral)\b", lower):
        return "PhD"
    if re.search(r"\b(pg|m\.?tech|m\.?sc|m\.?a\.?|m\.?b\.?a|postgraduate)\b", lower):
        return "PG"
    if re.search(r"\b(diploma)\b", lower):
        return "Diploma"
    if re.search(r"\b(certificate)\b", lower):
        return "Certificate"
    return "UG"


def infer_mode(text: str, url: str) -> str:
    lower = f"{text} {url}".lower()
    online = "online" in lower or "apply" in lower or "portal" in lower
    offline = "offline" in lower or "download form" in lower
    if online and offline:
        return "both"
    return "offline" if offline else "online"


def infer_country_state(text: str, url: str) -> tuple[str, str]:
    lower = f"{text} {urlparse(url).netloc}".lower()
    if any(token in lower for token in ("ucas", "united kingdom", ".ac.uk")):
        return "United Kingdom", ""
    if any(token in lower for token in ("common app", "commonapp", "united states", ".edu")):
        return "United States", ""
    states = {
        "UP": ("uttar pradesh", "jeecup"),
        "MH": ("maharashtra", "mahacet", "cetcell"),
        "WB": ("west bengal", "wbjee"),
        "KA": ("karnataka", "kea.kar"),
        "TN": ("tamil nadu", "tnea"),
        "DL": ("delhi",),
    }
    for code, hints in states.items():
        if any(h in lower for h in hints):
            return "India", code
    return "India", ""


def extract_year(text: str) -> int:
    years = [int(y) for y in re.findall(r"20\d{2}", text)]
    return max(years) if years else _today().year


def extract_fee_inr(text: str) -> int | None:
    match = re.search(r"(?:₹|rs\.?|inr)\s*([\d,]+)", text, re.I)
    return int(match.group(1).replace(",", "")) if match else None


def resolve_url(url: str, base_url: str) -> str:
    return urljoin(base_url, url).split("#", 1)[0]


def is_admission_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(pattern in path for pattern in ADMISSION_PATTERNS)


def validate_application_link(url: str) -> bool:
    if not url or url.startswith("file://"):
        return bool(url)
    if os.getenv("ADMISSIONS_VALIDATE_LINKS", "true").lower() == "false":
        return True
    try:
        request = Request(url, headers=get_headers(url))
        with urlopen(request, timeout=float(os.getenv("ADMISSIONS_LINK_TIMEOUT", "10"))) as response:  # noqa: S310
            return 200 <= getattr(response, "status", 200) < 400
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Application link validation failed for %s: %s", url, exc)
        return False


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
        return self.app.scrape_url(url, formats=["html", "markdown", "links"])


class AdmissionsCrawler:
    def __init__(self, *, firecrawl: FirecrawlClient | None = None, max_pages: int | None = None, rate_limit_seconds: float = 1.0) -> None:
        self.firecrawl = firecrawl or FirecrawlClient()
        self.max_pages = max_pages or int(os.getenv("ADMISSIONS_MAX_PAGES", "25"))
        self.rate_limit_seconds = rate_limit_seconds
        self._last_domain_fetch: dict[str, float] = {}

    def build_queries(self, entity_name: str | None = None, intake_year: int | None = None) -> list[str]:
        year = intake_year or _today().year
        queries = [f"site:nta.ac.in {year} application form"]
        queries.extend(STATE_BOARD_QUERIES)
        queries.extend([f"site:{domain} {year} admission application form" for domain in DIRECT_SOURCE_DOMAINS[1:]])
        if entity_name:
            queries.append(f"{entity_name} admission {year} apply online")
            queries.append(f"{entity_name} {year} application form direct link")
        return queries

    async def discover(self, source_url: str | None = None, entity_name: str | None = None, intake_year: int | None = None) -> list[str]:
        urls: list[str] = []
        if source_url:
            urls.append(source_url)
            urls.extend(await self._discover_site_links(source_url))
        for query in self.build_queries(entity_name, intake_year):
            for item in self.firecrawl.search(query, limit=10):
                url = item.get("url") or item.get("link")
                if url:
                    urls.append(url)
        return list(dict.fromkeys(urls))[: self.max_pages]

    async def crawl_source(self, *, entity_id: int | None, entity_name: str, source_url: str | None = None, intake_year: int | None = None) -> list[AdmissionNotice]:
        notices: list[AdmissionNotice] = []
        for url in await self.discover(source_url, entity_name, intake_year):
            if not self._robots_allowed(url):
                continue
            try:
                html = await self._fetch(url)
                notices.extend(self.extract_notices(html, url, entity_id=entity_id, entity_name=entity_name))
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Admission crawl failed for %s: %s", url, exc)
        return dedupe_notices(notices)

    def extract_notices(self, html: str, page_url: str, *, entity_id: int | None, entity_name: str) -> list[AdmissionNotice]:
        soup = BeautifulSoup(html or "", "lxml")
        page_text = soup.get_text("\n", strip=True)
        candidates = self._candidate_blocks(soup)
        if not candidates:
            candidates = [page_text]
        notices = []
        for block in candidates:
            if not re.search(r"admission|application|apply|entrance|registration", block, re.I):
                continue
            notice = self._notice_from_text(block, page_url, entity_id, entity_name, soup)
            if notice.application_link and validate_application_link(notice.application_link):
                notices.append(notice)
        return dedupe_notices(notices)

    async def _discover_site_links(self, source_url: str) -> list[str]:
        html = await self._fetch(source_url)
        soup = BeautifulSoup(html, "lxml")
        urls = [resolve_url(a.get("href", ""), source_url) for a in soup.select("a[href]")]
        return [u for u in urls if is_admission_url(u)][: self.max_pages]

    def _candidate_blocks(self, soup: BeautifulSoup) -> list[str]:
        blocks = []
        selectors = "article, section, tr, li, .notice, .admission, [class*='notice'], [class*='admission'], [id*='admission']"
        for node in soup.select(selectors):
            text = node.get_text(" ", strip=True)
            if len(text) > 20:
                blocks.append(text)
        return blocks

    def _notice_from_text(self, text: str, page_url: str, entity_id: int | None, entity_name: str, soup: BeautifulSoup) -> AdmissionNotice:
        year = extract_year(text)
        start = normalize_date(_extract_date_near(text, ("start", "opens", "from", "commencement")), year)
        end = normalize_date(_extract_date_near(text, ("last date", "deadline", "closes", "end date", "till")), year)
        exam = normalize_date(_extract_date_near(text, ("exam date", "entrance", "test date")), year)
        result = normalize_date(_extract_date_near(text, ("result", "merit list")), year)
        country, state = infer_country_state(text, page_url)
        link = self._find_application_link(soup, page_url)
        program = _program_name(text, entity_name)
        return AdmissionNotice(
            entity_id=entity_id,
            entity_name=entity_name,
            admission_type=infer_admission_type(text),
            program_name=program,
            intake_year=year,
            application_start_date=start,
            application_end_date=end,
            exam_date=exam,
            result_date=result,
            application_link=link,
            eligibility_text=_extract_sentence(text, "eligib")[:2000],
            fee_inr=extract_fee_inr(text),
            mode=infer_mode(text, link or page_url),
            status=classify_status(start, end),
            country=country,
            state=state,
            source_url=page_url,
            source_name=urlparse(page_url).netloc,
            raw_payload={"text_preview": text[:1000]},
        )

    def _find_application_link(self, soup: BeautifulSoup, page_url: str) -> str:
        scored: list[tuple[int, str]] = []
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            text = a.get_text(" ", strip=True).lower()
            target = resolve_url(href, page_url)
            if not target or target == page_url:
                continue
            lower = f"{target} {text}".lower()
            score = sum(3 for hint in FORM_HINTS if hint in lower)
            if "login" in lower or "register" in lower:
                score += 2
            if score:
                scored.append((score, target))
        scored.sort(reverse=True)
        return scored[0][1] if scored else ""

    async def _fetch(self, url: str) -> str:
        max_retries = int(os.getenv("ADMISSIONS_CRAWL_RETRIES", "3"))
        base_delay = float(os.getenv("ADMISSIONS_BACKOFF_SECONDS", "0.5"))
        for attempt in range(max_retries):
            try:
                return await self._fetch_once(url)
            except Exception:
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(max(0.0, add_jitter(base_delay * (2**attempt))))
        raise RuntimeError("unreachable fetch retry state")

    async def _fetch_once(self, url: str) -> str:
        await self._rate_limit(url)
        scraped = self.firecrawl.scrape(url)
        if scraped:
            html = scraped.get("html") or scraped.get("content") or scraped.get("markdown") or ""
            if html:
                return html
        if AsyncWebCrawler is not None and not url.startswith("file://"):
            browser_config = BrowserConfig(headless=True, java_script_enabled=True)
            run_config = CrawlerRunConfig() if CrawlerRunConfig is not None else None
            async with AsyncWebCrawler(config=browser_config) as crawler:
                result = await crawler.arun(url=url, config=run_config)
            html = getattr(result, "html", "") or getattr(result, "cleaned_html", "") or ""
            if html:
                return html
        if url.startswith("file://"):
            return Path(urlparse(url).path).read_text(encoding="utf-8")
        with urlopen(Request(url, headers=get_headers(url)), timeout=30) as response:  # noqa: S310
            return response.read().decode(response.headers.get_content_charset() or "utf-8", errors="replace")

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


def _extract_date_near(text: str, labels: tuple[str, ...]) -> str | None:
    for label in labels:
        pattern = rf"{re.escape(label)}[^\n:：-]{{0,40}}?[:：-]?\s*([0-3]?\d[-/ ][A-Za-z0-9]{{3,9}}[-/ ](?:20)?\d{{2}}|[A-Za-z]{{3,9}}\s+[0-3]?\d,?\s+20\d{{2}})"
        match = re.search(pattern, text, re.I)
        if match:
            return match.group(1)
    match = re.search(r"([0-3]?\d[-/][01]?\d[-/](?:20)?\d{2})", text)
    return match.group(1) if match else None


def _extract_sentence(text: str, needle: str) -> str:
    for sent in re.split(r"(?<=[.!?])\s+|\n", text):
        if needle.lower() in sent.lower():
            return sent.strip()
    return ""


def _program_name(text: str, entity_name: str) -> str:
    patterns = [r"((?:B\.?Tech|M\.?Tech|MBA|BBA|BSc|MSc|BA|MA|Ph\.?D|Diploma|Certificate)[\w\s&.-]{0,80})", r"admission\s+(?:to|for)\s+([\w\s&.-]{3,100})"]
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            return " ".join(match.group(1).split())[:150]
    return f"{entity_name} Admissions"


def dedupe_notices(notices: list[AdmissionNotice]) -> list[AdmissionNotice]:
    chosen: dict[tuple[int | None, str, int], AdmissionNotice] = {}
    for notice in notices:
        key = (notice.entity_id, notice.program_name.lower(), notice.intake_year)
        prev = chosen.get(key)
        if prev is None or (notice.application_link and not prev.application_link):
            chosen[key] = notice
    return list(chosen.values())


class AdmissionsRepository:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self.sqlite_path = database_url.replace("sqlite:///", "") if database_url.startswith("sqlite:///") else ""

    def init_sqlite(self) -> None:
        if not self.sqlite_path:
            return
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.execute(SQLITE_CREATE_ADMISSIONS)
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_admissions_entity_program_year ON admissions(entity_id, program_name, intake_year)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_admissions_status_state_type ON admissions(status, state, admission_type)")
            conn.commit()

    def upsert_many(self, notices: list[AdmissionNotice]) -> int:
        if self.database_url.startswith("postgresql://"):
            return self._upsert_many_pg(notices)
        self.init_sqlite()
        with sqlite3.connect(self.sqlite_path) as conn:
            for notice in notices:
                row = notice.to_row()
                row["updated_at"] = datetime.now(timezone.utc).isoformat()
                conn.execute(SQLITE_UPSERT_ADMISSION, row)
            conn.commit()
        return len(notices)

    def _upsert_many_pg(self, notices: list[AdmissionNotice]) -> int:
        if psycopg is None:
            raise RuntimeError("psycopg not installed for PostgreSQL admissions repository")
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                for notice in notices:
                    row = notice.to_row()
                    cur.execute(POSTGRES_UPSERT_ADMISSION, row)
            conn.commit()
        return len(notices)

    def list(self, *, status: str | None = None, state: str | None = None, admission_type: str | None = None, country: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        if self.database_url.startswith("postgresql://"):
            return self._list_pg(status=status, state=state, admission_type=admission_type, country=country, limit=limit)
        self.init_sqlite()
        where = []
        params: list[Any] = []
        if status:
            where.append("status=?")
            params.append(status)
        if state:
            where.append("state=?")
            params.append(state)
        if admission_type:
            where.append("admission_type=?")
            params.append(admission_type)
        if country:
            where.append("country=?")
            params.append(country)
        sql = "SELECT * FROM admissions" + (" WHERE " + " AND ".join(where) if where else "") + " ORDER BY COALESCE(application_end_date, exam_date, application_start_date) ASC, id DESC LIMIT ?"
        params.append(limit)
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(row) for row in conn.execute(sql, params).fetchall()]

    def get(self, admission_id: int) -> dict[str, Any] | None:
        if self.database_url.startswith("postgresql://"):
            return self._get_pg(admission_id)
        self.init_sqlite()
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM admissions WHERE id=?", (admission_id,)).fetchone()
            return dict(row) if row else None

    def upcoming(self, days: int = 30) -> list[dict[str, Any]]:
        if self.database_url.startswith("postgresql://"):
            return self._upcoming_pg(days)
        self.init_sqlite()
        end = (_today() + timedelta(days=days)).isoformat()
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(row) for row in conn.execute("SELECT * FROM admissions WHERE status='upcoming' AND COALESCE(application_start_date, exam_date, application_end_date) <= ? ORDER BY COALESCE(application_start_date, exam_date, application_end_date) ASC", (end,)).fetchall()]

    def mark_closed(self) -> int:
        if self.database_url.startswith("postgresql://"):
            return self._mark_closed_pg()
        self.init_sqlite()
        today = _today().isoformat()
        with sqlite3.connect(self.sqlite_path) as conn:
            cur = conn.execute("UPDATE admissions SET status='closed', updated_at=? WHERE application_end_date IS NOT NULL AND application_end_date < ? AND status!='closed'", (datetime.now(timezone.utc).isoformat(), today))
            conn.commit()
            return cur.rowcount



    def _list_pg(self, *, status: str | None = None, state: str | None = None, admission_type: str | None = None, country: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        if psycopg is None:
            raise RuntimeError("psycopg not installed for PostgreSQL admissions repository")
        where = []
        params: list[Any] = []
        if status:
            where.append("status=%s")
            params.append(status)
        if state:
            where.append("state=%s")
            params.append(state)
        if admission_type:
            where.append("admission_type=%s")
            params.append(admission_type)
        if country:
            where.append("country=%s")
            params.append(country)
        sql = "SELECT * FROM admissions" + (" WHERE " + " AND ".join(where) if where else "") + " ORDER BY COALESCE(application_end_date, exam_date, application_start_date) ASC, id DESC LIMIT %s"
        params.append(limit)
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(sql, params)
                return list(cur.fetchall())

    def _get_pg(self, admission_id: int) -> dict[str, Any] | None:
        if psycopg is None:
            raise RuntimeError("psycopg not installed for PostgreSQL admissions repository")
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute("SELECT * FROM admissions WHERE id=%s", (admission_id,))
                row = cur.fetchone()
                return dict(row) if row else None

    def _upcoming_pg(self, days: int = 30) -> list[dict[str, Any]]:
        if psycopg is None:
            raise RuntimeError("psycopg not installed for PostgreSQL admissions repository")
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute("SELECT * FROM admissions WHERE status='upcoming' AND COALESCE(application_start_date, exam_date, application_end_date) <= CURRENT_DATE + (%s * INTERVAL '1 day') ORDER BY COALESCE(application_start_date, exam_date, application_end_date) ASC", (days,))
                return list(cur.fetchall())

    def _mark_closed_pg(self) -> int:
        if psycopg is None:
            raise RuntimeError("psycopg not installed for PostgreSQL admissions repository")
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE admissions SET status='closed', updated_at=NOW() WHERE application_end_date IS NOT NULL AND application_end_date < CURRENT_DATE AND status!='closed'")
                count = cur.rowcount
            conn.commit()
            return count

SQLITE_CREATE_ADMISSIONS = """
CREATE TABLE IF NOT EXISTS admissions(
    id INTEGER PRIMARY KEY,
    entity_id INTEGER,
    entity_name TEXT NOT NULL,
    admission_type TEXT NOT NULL,
    program_name TEXT NOT NULL,
    intake_year INTEGER NOT NULL,
    application_start_date TEXT,
    application_end_date TEXT,
    exam_date TEXT,
    result_date TEXT,
    application_link TEXT NOT NULL,
    eligibility_text TEXT,
    fee_inr INTEGER,
    mode TEXT,
    status TEXT,
    country TEXT,
    state TEXT,
    source_url TEXT,
    source_name TEXT,
    raw_payload TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
)
"""

SQLITE_UPSERT_ADMISSION = """
INSERT INTO admissions(entity_id,entity_name,admission_type,program_name,intake_year,application_start_date,application_end_date,exam_date,result_date,application_link,eligibility_text,fee_inr,mode,status,country,state,source_url,source_name,raw_payload,updated_at)
VALUES(:entity_id,:entity_name,:admission_type,:program_name,:intake_year,:application_start_date,:application_end_date,:exam_date,:result_date,:application_link,:eligibility_text,:fee_inr,:mode,:status,:country,:state,:source_url,:source_name,:raw_payload,:updated_at)
ON CONFLICT(entity_id, program_name, intake_year) DO UPDATE SET
    entity_name=excluded.entity_name,
    admission_type=excluded.admission_type,
    application_start_date=excluded.application_start_date,
    application_end_date=excluded.application_end_date,
    exam_date=excluded.exam_date,
    result_date=excluded.result_date,
    application_link=excluded.application_link,
    eligibility_text=excluded.eligibility_text,
    fee_inr=excluded.fee_inr,
    mode=excluded.mode,
    status=excluded.status,
    country=excluded.country,
    state=excluded.state,
    source_url=excluded.source_url,
    source_name=excluded.source_name,
    raw_payload=excluded.raw_payload,
    updated_at=excluded.updated_at
"""


def crawl_admissions_sync(database_url: str, *, entity_id: int | None, entity_name: str, source_url: str | None = None, intake_year: int | None = None) -> dict[str, Any]:
    notices = asyncio.run(AdmissionsCrawler().crawl_source(entity_id=entity_id, entity_name=entity_name, source_url=source_url, intake_year=intake_year))
    saved = AdmissionsRepository(database_url).upsert_many(notices)
    return {"discovered": len(notices), "saved": saved}

POSTGRES_UPSERT_ADMISSION = """
INSERT INTO admissions(entity_id,entity_name,admission_type,program_name,intake_year,application_start_date,application_end_date,exam_date,result_date,application_link,eligibility_text,fee_inr,mode,status,country,state,source_url,source_name,raw_payload,updated_at)
VALUES(%(entity_id)s,%(entity_name)s,%(admission_type)s,%(program_name)s,%(intake_year)s,%(application_start_date)s,%(application_end_date)s,%(exam_date)s,%(result_date)s,%(application_link)s,%(eligibility_text)s,%(fee_inr)s,%(mode)s,%(status)s,%(country)s,%(state)s,%(source_url)s,%(source_name)s,%(raw_payload)s::jsonb,NOW())
ON CONFLICT(entity_id, program_name, intake_year) DO UPDATE SET
    entity_name=EXCLUDED.entity_name,
    admission_type=EXCLUDED.admission_type,
    application_start_date=EXCLUDED.application_start_date,
    application_end_date=EXCLUDED.application_end_date,
    exam_date=EXCLUDED.exam_date,
    result_date=EXCLUDED.result_date,
    application_link=EXCLUDED.application_link,
    eligibility_text=EXCLUDED.eligibility_text,
    fee_inr=EXCLUDED.fee_inr,
    mode=EXCLUDED.mode,
    status=EXCLUDED.status,
    country=EXCLUDED.country,
    state=EXCLUDED.state,
    source_url=EXCLUDED.source_url,
    source_name=EXCLUDED.source_name,
    raw_payload=EXCLUDED.raw_payload,
    updated_at=NOW()
"""
