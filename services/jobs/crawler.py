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

boto3 = importlib.import_module("boto3") if importlib.util.find_spec("boto3") else None
BotoConfig = importlib.import_module("botocore.client").Config if importlib.util.find_spec("botocore") else None
psycopg = importlib.import_module("psycopg") if importlib.util.find_spec("psycopg") else None

GOVT_SOURCES = (
    "sarkariresult.com",
    "sarkariresultnaukri.com",
    "rojgarsamachar.gov.in",
    "upsc.gov.in",
    "ssc.nic.in",
    "indianrailways.gov.in",
    "rrbcdg.gov.in",
    "ibps.in",
    "sbi.co.in",
    "joinindianarmy.nic.in",
    "nausena.nic.in",
)
PRIVATE_SOURCES = ("naukri.com", "linkedin.com/jobs", "indeed.co.in", "infosys.com/careers", "tcs.com/careers", "wipro.com/careers")
INTERNSHIP_SOURCES = ("internshala.com", "letsintern.com", "unstop.com")
GOVT_PRIORITY_PATTERNS = ("/recruitment", "/vacancy", "/apply-online", "/career", "/notification", "/jobs")
JOB_LINK_PATTERNS = ("job", "career", "recruit", "vacancy", "internship", "apply", "notification")
APPLICATION_HINTS = ("apply", "application", "registration", "form", "career", "portal")
LOGIN_HINTS = ("sign in", "signin", "login", "log in", "create account", "register to apply")
PDF_BUCKET = "job-notifications"


@dataclass
class JobPosting:
    title: str
    organization: str
    job_type: str
    category: str
    vacancies: int | None = None
    eligibility_text: str = ""
    age_limit: str = ""
    pay_scale: str = ""
    location: str = "pan-india"
    application_start_date: str | None = None
    application_end_date: str | None = None
    application_link: str = ""
    official_notification_pdf_url: str = ""
    exam_date: str | None = None
    result_date: str | None = None
    source_site: str = ""
    country: str = "India"
    state: str = ""
    status: str = "ongoing"
    requires_login: bool = False
    raw_payload: dict[str, Any] | None = None

    def to_row(self) -> dict[str, Any]:
        row = asdict(self)
        row["raw_payload"] = json.dumps(row.get("raw_payload") or {}, sort_keys=True)
        return row


def _today() -> date:
    override = os.getenv("JOBS_TODAY")
    if override:
        return date.fromisoformat(override)
    return datetime.now(timezone.utc).date()


def classify_status(start: str | None, end: str | None, *, today: date | None = None) -> str:
    today = today or _today()
    start_date = parse_date(start)
    end_date = parse_date(end)
    if end_date and end_date < today:
        return "closed"
    if start_date and start_date > today:
        return "upcoming"
    return "ongoing"


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d %B %Y", "%d %b %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def normalize_date(value: str | None, default_year: int | None = None) -> str | None:
    if not value:
        return None
    parsed = parse_date(value)
    if parsed:
        return parsed.isoformat()
    year = default_year or _today().year
    month_names = {m.lower(): i for i, m in enumerate(["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"], 1)}
    match = re.search(r"(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,9})(?:\s+(20\d{2}))?", value)
    if match:
        month = month_names.get(match.group(2).lower()) or month_names.get(match.group(2).lower()[:3])
        if month:
            return date(int(match.group(3) or year), month, int(match.group(1))).isoformat()
    return None


def infer_job_type(text: str, url: str) -> str:
    lower = f"{text} {url}".lower()
    if "internship" in lower or "internshala" in lower or "letsintern" in lower:
        return "internship"
    if "contract" in lower:
        return "contract"
    if any(host in lower for host in GOVT_SOURCES) or re.search(r"\b(upsc|ssc|rrb|railway|psc|sarkari|rojgar|army|navy|ibps|sbi)\b", lower):
        return "govt"
    return "private"


def infer_category(text: str) -> str:
    lower = text.lower()
    if re.search(r"bank|ibps|sbi|rbi|clerk|po\b", lower):
        return "banking"
    if re.search(r"army|navy|air force|defence|defense|agniveer", lower):
        return "defence"
    if re.search(r"railway|rrb|group d|locopilot", lower):
        return "railway"
    if re.search(r"teacher|teaching|faculty|professor|tet\b|ctet", lower):
        return "teaching"
    if re.search(r"software|developer|engineer|data|tech|it\b|programmer", lower):
        return "tech"
    return "other"


def infer_country_state(text: str, url: str) -> tuple[str, str]:
    lower = f"{text} {urlparse(url).netloc}".lower()
    states = {
        "UP": ("uttar pradesh", "uppsc", "upsc.gov.in"),
        "MH": ("maharashtra", "mpsc"),
        "WB": ("west bengal", "wbpsc"),
        "KA": ("karnataka", "kpsc"),
        "TN": ("tamil nadu", "tnpsc"),
        "RJ": ("rajasthan", "rpsc"),
        "GJ": ("gujarat", "gpsc"),
        "DL": ("delhi",),
    }
    for code, hints in states.items():
        if any(h in lower for h in hints):
            return "India", code
    if "remote" in lower and not any(country in lower for country in ("india", ".in")):
        return "Global", ""
    return "India", ""


def extract_int(pattern: str, text: str) -> int | None:
    match = re.search(pattern, text, re.I)
    return int(match.group(1).replace(",", "")) if match else None


def money_amount(text: str) -> int:
    match = re.search(r"(?:₹|rs\.?|inr)?\s*([\d,]{4,})", text or "", re.I)
    return int(match.group(1).replace(",", "")) if match else 0


def extract_date_near(text: str, labels: tuple[str, ...]) -> str | None:
    for label in labels:
        pattern = rf"{re.escape(label)}[^\n:：-]{{0,50}}?[:：-]?\s*([0-3]?\d[-/ ][A-Za-z0-9]{{3,9}}[-/ ](?:20)?\d{{2}}|[A-Za-z]{{3,9}}\s+[0-3]?\d,?\s+20\d{{2}})"
        match = re.search(pattern, text, re.I)
        if match:
            return match.group(1)
    match = re.search(r"([0-3]?\d[-/][01]?\d[-/](?:20)?\d{2})", text)
    return match.group(1) if match else None


def extract_sentence(text: str, needles: tuple[str, ...]) -> str:
    for sent in re.split(r"(?<=[.!?])\s+|\n", text):
        if any(needle.lower() in sent.lower() for needle in needles):
            return " ".join(sent.split())
    return ""


def resolve_url(url: str, base_url: str) -> str:
    return urljoin(base_url, url).split("#", 1)[0]


def is_job_url(url: str) -> bool:
    lower = url.lower()
    return any(pattern in lower for pattern in JOB_LINK_PATTERNS)


def is_login_wall(html: str, url: str) -> bool:
    lower = f"{html[:4000]} {url}".lower()
    return any(hint in lower for hint in LOGIN_HINTS)


def validate_application_link(url: str) -> tuple[bool, bool]:
    if not url:
        return False, False
    if url.startswith("file://"):
        html = Path(urlparse(url).path).read_text(encoding="utf-8") if Path(urlparse(url).path).exists() else ""
        return True, is_login_wall(html, url)
    if os.getenv("JOBS_VALIDATE_LINKS", "true").lower() == "false":
        return True, False
    try:
        with urlopen(Request(url, headers={"User-Agent": os.getenv("CRAWLER_USER_AGENT", "CollegeCueBot/1.0")}), timeout=float(os.getenv("JOBS_LINK_TIMEOUT", "10"))) as response:  # noqa: S310
            body = response.read(12000).decode(response.headers.get_content_charset() or "utf-8", errors="replace")
            return 200 <= getattr(response, "status", 200) < 400, is_login_wall(body, url)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Job application link validation failed for %s: %s", url, exc)
        return False, False


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


class PdfStore:
    def __init__(self) -> None:
        self.bucket = os.getenv("JOB_PDF_BUCKET", PDF_BUCKET)
        self.enabled = os.getenv("JOBS_S3_ENABLED", "true").lower() != "false" and boto3 is not None
        self.client = None
        if self.enabled:
            self.client = boto3.client(
                "s3",
                endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://minio:9000"),
                aws_access_key_id=os.getenv("S3_ACCESS_KEY", "minio"),
                aws_secret_access_key=os.getenv("S3_SECRET_KEY", "minio123"),
                config=BotoConfig(signature_version="s3v4") if BotoConfig else None,
            )

    def store_pdf(self, pdf_url: str) -> str:
        if not pdf_url:
            return ""
        key = f"jobs/notifications/{hashlib.sha256(pdf_url.encode()).hexdigest()}.pdf"
        if self.client is None:
            return f"disabled://{self.bucket}/{key}"
        if pdf_url.startswith("file://"):
            data = Path(urlparse(pdf_url).path).read_bytes()
        else:
            with urlopen(Request(pdf_url, headers={"User-Agent": os.getenv("CRAWLER_USER_AGENT", "CollegeCueBot/1.0")}), timeout=30) as response:  # noqa: S310
                data = response.read()
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType="application/pdf")
        return f"s3://{self.bucket}/{key}"


class JobsCrawler:
    def __init__(self, *, firecrawl: FirecrawlClient | None = None, pdf_store: PdfStore | None = None, max_pages: int | None = None, rate_limit_seconds: float = 1.0) -> None:
        self.firecrawl = firecrawl or FirecrawlClient()
        self.pdf_store = pdf_store or PdfStore()
        self.max_pages = max_pages or int(os.getenv("JOBS_MAX_PAGES", "50"))
        self.rate_limit_seconds = rate_limit_seconds
        self._last_domain_fetch: dict[str, float] = {}

    def build_queries(self, job_type: str = "private", intake_year: int | None = None) -> list[str]:
        year = intake_year or _today().year
        if job_type == "govt":
            queries = [f"site:{site} {year} recruitment apply online" for site in GOVT_SOURCES]
            queries.extend(["railway group d recruitment apply online", "banking jobs ibps sbi recruitment", "state psc recruitment apply online"])
            return queries
        if job_type == "internship":
            return ["internships India remote stipend", "site:internshala.com internships remote stipend", "site:unstop.com internships apply"]
        return ["fresher jobs India 2025", "site:naukri.com fresher jobs India", "site:linkedin.com/jobs fresher jobs India", "site:indeed.co.in fresher jobs India"]

    async def crawl(self, *, seed_urls: list[str] | None = None, job_type: str = "private", query: str | None = None) -> list[JobPosting]:
        urls = await self.discover(seed_urls=seed_urls or [], job_type=job_type, query=query)
        postings: list[JobPosting] = []
        for url in urls:
            if not self._robots_allowed(url):
                LOGGER.info("Robots blocked jobs URL %s", url)
                continue
            try:
                html = await self._fetch(url)
                postings.extend(self.extract_postings(html, url, default_job_type=job_type))
                if "internshala.com" in urlparse(url).netloc:
                    postings.extend(await self._crawl_internshala_pages(url, html))
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Job crawl failed for %s: %s", url, exc)
        return dedupe_postings(postings)

    async def discover(self, *, seed_urls: list[str], job_type: str, query: str | None = None) -> list[str]:
        urls = list(seed_urls)
        for seed in seed_urls:
            if not self._robots_allowed(seed):
                continue
            try:
                html = await self._fetch(seed)
                urls.extend(self._extract_job_links(html, seed, job_type=job_type))
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Job seed discovery failed for %s: %s", seed, exc)
        queries = [query] if query else self.build_queries(job_type)
        for q in [x for x in queries if x]:
            for item in self.firecrawl.search(q, limit=10):
                url = item.get("url") or item.get("link")
                if url:
                    urls.append(url)
        return list(dict.fromkeys(urls))[: self.max_pages]

    def extract_postings(self, html: str, page_url: str, *, default_job_type: str = "private") -> list[JobPosting]:
        soup = BeautifulSoup(html or "", "lxml")
        candidates = self._candidate_blocks(soup)
        if not candidates:
            candidates = [soup.get_text("\n", strip=True)]
        postings: list[JobPosting] = []
        for block in candidates:
            if not re.search(r"job|recruit|vacancy|internship|apply|notification", block, re.I):
                continue
            posting = self._posting_from_text(block, soup, page_url, default_job_type=default_job_type)
            valid, requires_login = validate_application_link(posting.application_link)
            if not valid:
                continue
            posting.requires_login = requires_login
            posting.raw_payload = posting.raw_payload or {}
            posting.raw_payload["requires_login"] = requires_login
            if posting.official_notification_pdf_url:
                posting.raw_payload["notification_pdf_object"] = self.pdf_store.store_pdf(posting.official_notification_pdf_url)
            postings.append(posting)
        return dedupe_postings(postings)

    def _posting_from_text(self, text: str, soup: BeautifulSoup, page_url: str, *, default_job_type: str) -> JobPosting:
        year = _today().year
        start = normalize_date(extract_date_near(text, ("start date", "opening date", "from", "application start")), year)
        end = normalize_date(extract_date_near(text, ("last date", "closing date", "deadline", "application end")), year)
        exam = normalize_date(extract_date_near(text, ("exam date", "test date", "written exam")), year)
        result = normalize_date(extract_date_near(text, ("result", "merit list")), year)
        title = self._title(text, soup)
        organization = self._organization(text, page_url)
        job_type = infer_job_type(text, page_url) if default_job_type == "auto" else infer_job_type(f"{default_job_type} {text}", page_url)
        country, state = infer_country_state(text, page_url)
        application_link, requires_login_hint = self._find_application_link(soup, page_url)
        pdf_url = self._find_pdf(soup, page_url)
        posting = JobPosting(
            title=title,
            organization=organization,
            job_type=job_type,
            category=infer_category(text),
            vacancies=extract_int(r"(?:vacanc(?:y|ies)|posts?)\D{0,20}([\d,]+)", text),
            eligibility_text=extract_sentence(text, ("eligib", "qualification"))[:2000],
            age_limit=extract_sentence(text, ("age", "years"))[:500],
            pay_scale=extract_sentence(text, ("pay", "salary", "stipend", "ctc"))[:500],
            location=self._location(text),
            application_start_date=start,
            application_end_date=end,
            application_link=application_link,
            official_notification_pdf_url=pdf_url,
            exam_date=exam,
            result_date=result,
            source_site=urlparse(page_url).netloc,
            country=country,
            state=state,
            status=classify_status(start, end),
            requires_login=requires_login_hint,
            raw_payload={"source_url": page_url, "text_preview": text[:1000], "requires_login": requires_login_hint},
        )
        return posting

    def _candidate_blocks(self, soup: BeautifulSoup) -> list[str]:
        selectors = "article, section, tr, li, .job, .jobs, .vacancy, .recruitment, .internship, [class*='job'], [class*='vacancy'], [class*='recruit'], [class*='internship']"
        blocks = []
        for node in soup.select(selectors):
            text = node.get_text(" ", strip=True)
            if len(text) > 20:
                blocks.append(text)
        return blocks

    def _extract_job_links(self, html: str, base_url: str, *, job_type: str) -> list[str]:
        soup = BeautifulSoup(html or "", "lxml")
        links = [resolve_url(a.get("href", ""), base_url) for a in soup.select("a[href]")]
        if job_type == "govt":
            links = sorted(links, key=lambda u: 0 if any(pattern in urlparse(u).path.lower() for pattern in GOVT_PRIORITY_PATTERNS) else 1)
        return [u for u in links if is_job_url(u)][: self.max_pages]

    def _find_application_link(self, soup: BeautifulSoup, page_url: str) -> tuple[str, bool]:
        scored: list[tuple[int, str, bool]] = []
        for a in soup.select("a[href]"):
            text = a.get_text(" ", strip=True).lower()
            target = resolve_url(a.get("href", ""), page_url)
            if not target or target == page_url or target.lower().endswith(".pdf"):
                continue
            lower = f"{target} {text}".lower()
            score = sum(3 for hint in APPLICATION_HINTS if hint in lower)
            login = any(hint in lower for hint in LOGIN_HINTS)
            if login:
                score += 1
            if score:
                scored.append((score, target, login))
        scored.sort(reverse=True)
        return (scored[0][1], scored[0][2]) if scored else ("", False)

    def _find_pdf(self, soup: BeautifulSoup, page_url: str) -> str:
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            text = a.get_text(" ", strip=True).lower()
            if href.lower().endswith(".pdf") or "notification" in text:
                target = resolve_url(href, page_url)
                if target.lower().endswith(".pdf"):
                    return target
        return ""

    async def _crawl_internshala_pages(self, first_url: str, first_html: str) -> list[JobPosting]:
        postings: list[JobPosting] = []
        html = first_html
        current = first_url
        seen = {first_url}
        for _ in range(49):
            next_url = self._next_page_url(html, current)
            if not next_url or next_url in seen or not self._robots_allowed(next_url):
                break
            seen.add(next_url)
            html = await self._fetch(next_url)
            postings.extend(self.extract_postings(html, next_url, default_job_type="internship"))
            current = next_url
        return postings

    def _next_page_url(self, html: str, current_url: str) -> str:
        soup = BeautifulSoup(html or "", "lxml")
        node = soup.select_one("a[rel='next'], a.next, .pagination a[aria-label*='Next']")
        if node and node.get("href"):
            return resolve_url(node.get("href"), current_url)
        for link in soup.select("a[href]"):
            if "next" in link.get_text(" ", strip=True).lower():
                return resolve_url(link.get("href"), current_url)
        return ""

    async def _fetch(self, url: str) -> str:
        max_retries = int(os.getenv("JOBS_CRAWL_RETRIES", "3"))
        base_delay = float(os.getenv("JOBS_BACKOFF_SECONDS", "0.5"))
        for attempt in range(max_retries):
            try:
                return await self._fetch_once(url)
            except Exception:
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(base_delay * (2**attempt))
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
        with urlopen(Request(url, headers={"User-Agent": os.getenv("CRAWLER_USER_AGENT", "CollegeCueBot/1.0")}), timeout=30) as response:  # noqa: S310
            return response.read().decode(response.headers.get_content_charset() or "utf-8", errors="replace")

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
    def _title(text: str, soup: BeautifulSoup) -> str:
        heading = soup.find(["h1", "h2"])
        if heading and heading.get_text(" ", strip=True):
            return heading.get_text(" ", strip=True)[:240]
        first = re.split(r"[.|\n]", text.strip())[0]
        return first[:240] or "Untitled Job"

    @staticmethod
    def _organization(text: str, page_url: str) -> str:
        org_line = extract_sentence(text, ("organization", "company", "department", "board"))
        match = re.search(r"(?:organization|company|department|board)\s*[:：-]\s*([\w\s&.,()-]{2,120})", org_line, re.I)
        if match:
            return match.group(1).strip()
        host = urlparse(page_url).netloc.removeprefix("www.")
        return host.split(".")[0].upper() if host else "Unknown Organization"

    @staticmethod
    def _location(text: str) -> str:
        lower = text.lower()
        if "remote" in lower or "work from home" in lower:
            return "remote"
        if "pan india" in lower or "all india" in lower:
            return "pan-india"
        match = re.search(r"(?:location|job location)\s*[:：-]\s*([\w\s,.-]{2,120})", text, re.I)
        return match.group(1).strip() if match else "pan-india"


def dedupe_postings(postings: list[JobPosting]) -> list[JobPosting]:
    out: dict[tuple[str, str, str], JobPosting] = {}
    for posting in postings:
        key = (posting.title.lower(), posting.organization.lower(), posting.application_end_date or "")
        previous = out.get(key)
        if previous is None or (posting.application_link and not previous.application_link):
            out[key] = posting
    return list(out.values())

SQLITE_CREATE_JOBS = """
CREATE TABLE IF NOT EXISTS jobs(
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    organization TEXT NOT NULL,
    job_type TEXT NOT NULL,
    category TEXT NOT NULL,
    vacancies INTEGER,
    eligibility_text TEXT,
    age_limit TEXT,
    pay_scale TEXT,
    location TEXT,
    application_start_date TEXT,
    application_end_date TEXT,
    application_link TEXT NOT NULL,
    official_notification_pdf_url TEXT,
    exam_date TEXT,
    result_date TEXT,
    source_site TEXT,
    country TEXT,
    state TEXT,
    status TEXT,
    requires_login INTEGER DEFAULT 0,
    raw_payload TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
)
"""

SQLITE_UPSERT_JOB = """
INSERT INTO jobs(title,organization,job_type,category,vacancies,eligibility_text,age_limit,pay_scale,location,application_start_date,application_end_date,application_link,official_notification_pdf_url,exam_date,result_date,source_site,country,state,status,requires_login,raw_payload,updated_at)
VALUES(:title,:organization,:job_type,:category,:vacancies,:eligibility_text,:age_limit,:pay_scale,:location,:application_start_date,:application_end_date,:application_link,:official_notification_pdf_url,:exam_date,:result_date,:source_site,:country,:state,:status,:requires_login,:raw_payload,:updated_at)
ON CONFLICT(title, organization, application_end_date) DO UPDATE SET
    job_type=excluded.job_type,
    category=excluded.category,
    vacancies=excluded.vacancies,
    eligibility_text=excluded.eligibility_text,
    age_limit=excluded.age_limit,
    pay_scale=excluded.pay_scale,
    location=excluded.location,
    application_start_date=excluded.application_start_date,
    application_link=excluded.application_link,
    official_notification_pdf_url=excluded.official_notification_pdf_url,
    exam_date=excluded.exam_date,
    result_date=excluded.result_date,
    source_site=excluded.source_site,
    country=excluded.country,
    state=excluded.state,
    status=excluded.status,
    requires_login=excluded.requires_login,
    raw_payload=excluded.raw_payload,
    updated_at=excluded.updated_at
"""

POSTGRES_UPSERT_JOB = """
INSERT INTO jobs(title,organization,job_type,category,vacancies,eligibility_text,age_limit,pay_scale,location,application_start_date,application_end_date,application_link,official_notification_pdf_url,exam_date,result_date,source_site,country,state,status,requires_login,raw_payload,updated_at)
VALUES(%(title)s,%(organization)s,%(job_type)s,%(category)s,%(vacancies)s,%(eligibility_text)s,%(age_limit)s,%(pay_scale)s,%(location)s,%(application_start_date)s,%(application_end_date)s,%(application_link)s,%(official_notification_pdf_url)s,%(exam_date)s,%(result_date)s,%(source_site)s,%(country)s,%(state)s,%(status)s,%(requires_login)s,%(raw_payload)s::jsonb,NOW())
ON CONFLICT(title, organization, application_end_date) DO UPDATE SET
    job_type=EXCLUDED.job_type,
    category=EXCLUDED.category,
    vacancies=EXCLUDED.vacancies,
    eligibility_text=EXCLUDED.eligibility_text,
    age_limit=EXCLUDED.age_limit,
    pay_scale=EXCLUDED.pay_scale,
    location=EXCLUDED.location,
    application_start_date=EXCLUDED.application_start_date,
    application_link=EXCLUDED.application_link,
    official_notification_pdf_url=EXCLUDED.official_notification_pdf_url,
    exam_date=EXCLUDED.exam_date,
    result_date=EXCLUDED.result_date,
    source_site=EXCLUDED.source_site,
    country=EXCLUDED.country,
    state=EXCLUDED.state,
    status=EXCLUDED.status,
    requires_login=EXCLUDED.requires_login,
    raw_payload=EXCLUDED.raw_payload,
    updated_at=NOW()
"""


class JobsRepository:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self.sqlite_path = database_url.replace("sqlite:///", "") if database_url.startswith("sqlite:///") else ""

    def init_sqlite(self) -> None:
        if not self.sqlite_path:
            return
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.execute(SQLITE_CREATE_JOBS)
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_jobs_title_org_end ON jobs(title, organization, application_end_date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_type_category_state_status ON jobs(job_type, category, state, status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_search ON jobs(title, organization, category, location)")
            conn.commit()

    def upsert_many(self, postings: list[JobPosting]) -> int:
        if self.database_url.startswith("postgresql://"):
            return self._upsert_many_pg(postings)
        self.init_sqlite()
        with sqlite3.connect(self.sqlite_path) as conn:
            for posting in postings:
                row = posting.to_row()
                row["requires_login"] = 1 if row.get("requires_login") else 0
                row["updated_at"] = datetime.now(timezone.utc).isoformat()
                conn.execute(SQLITE_UPSERT_JOB, row)
            conn.commit()
        return len(postings)

    def _upsert_many_pg(self, postings: list[JobPosting]) -> int:
        if psycopg is None:
            raise RuntimeError("psycopg not installed for PostgreSQL jobs repository")
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                for posting in postings:
                    cur.execute(POSTGRES_UPSERT_JOB, posting.to_row())
            conn.commit()
        return len(postings)

    def list(self, *, job_type: str | None = None, category: str | None = None, state: str | None = None, status: str | None = None, location: str | None = None, stipend_min: int | None = None, limit: int = 100) -> list[dict[str, Any]]:
        if self.database_url.startswith("postgresql://"):
            return self._list_pg(job_type=job_type, category=category, state=state, status=status, location=location, stipend_min=stipend_min, limit=limit)
        self.init_sqlite()
        where, params = self._where_sql("?", job_type, category, state, status, location, stipend_min)
        sql = "SELECT * FROM jobs" + (" WHERE " + " AND ".join(where) if where else "") + " ORDER BY COALESCE(application_end_date, exam_date, application_start_date) ASC, id DESC LIMIT ?"
        params.append(limit)
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
        if stipend_min is not None:
            rows = [row for row in rows if money_amount(row.get("pay_scale", "")) >= stipend_min]
        return rows

    def get(self, job_id: int) -> dict[str, Any] | None:
        if self.database_url.startswith("postgresql://"):
            return self._get_pg(job_id)
        self.init_sqlite()
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            return dict(row) if row else None

    def search(self, query: str, *, limit: int = 100) -> list[dict[str, Any]]:
        if self.database_url.startswith("postgresql://"):
            return self._search_pg(query, limit=limit)
        self.init_sqlite()
        needle = f"%{query.lower()}%"
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(row) for row in conn.execute("SELECT * FROM jobs WHERE lower(title || ' ' || organization || ' ' || category || ' ' || location || ' ' || eligibility_text) LIKE ? ORDER BY id DESC LIMIT ?", (needle, limit)).fetchall()]

    def mark_closed(self) -> int:
        if self.database_url.startswith("postgresql://"):
            return self._mark_closed_pg()
        self.init_sqlite()
        with sqlite3.connect(self.sqlite_path) as conn:
            cur = conn.execute("UPDATE jobs SET status='closed', updated_at=? WHERE application_end_date IS NOT NULL AND application_end_date < ? AND status!='closed'", (datetime.now(timezone.utc).isoformat(), _today().isoformat()))
            conn.commit()
            return cur.rowcount

    @staticmethod
    def _where_sql(marker: str, job_type: str | None, category: str | None, state: str | None, status: str | None, location: str | None, stipend_min: int | None) -> tuple[list[str], list[Any]]:
        where: list[str] = []
        params: list[Any] = []
        for column, value in (("job_type", job_type), ("category", category), ("state", state), ("status", status)):
            if value:
                where.append(f"{column}={marker}")
                params.append(value)
        if location:
            where.append(f"lower(location) LIKE {marker}")
            params.append(f"%{location.lower()}%")
        return where, params

    def _list_pg(self, *, job_type: str | None, category: str | None, state: str | None, status: str | None, location: str | None, stipend_min: int | None, limit: int) -> list[dict[str, Any]]:
        if psycopg is None:
            raise RuntimeError("psycopg not installed for PostgreSQL jobs repository")
        where, params = self._where_sql("%s", job_type, category, state, status, location, stipend_min)
        sql = "SELECT * FROM jobs" + (" WHERE " + " AND ".join(where) if where else "") + " ORDER BY COALESCE(application_end_date, exam_date, application_start_date) ASC, id DESC LIMIT %s"
        params.append(limit)
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(sql, params)
                rows = list(cur.fetchall())
        if stipend_min is not None:
            rows = [row for row in rows if money_amount(row.get("pay_scale", "")) >= stipend_min]
        return rows

    def _get_pg(self, job_id: int) -> dict[str, Any] | None:
        if psycopg is None:
            raise RuntimeError("psycopg not installed for PostgreSQL jobs repository")
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute("SELECT * FROM jobs WHERE id=%s", (job_id,))
                row = cur.fetchone()
                return dict(row) if row else None

    def _search_pg(self, query: str, *, limit: int) -> list[dict[str, Any]]:
        if psycopg is None:
            raise RuntimeError("psycopg not installed for PostgreSQL jobs repository")
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute("SELECT * FROM jobs WHERE to_tsvector('english', title || ' ' || organization || ' ' || category || ' ' || location || ' ' || eligibility_text) @@ plainto_tsquery('english', %s) ORDER BY id DESC LIMIT %s", (query, limit))
                return list(cur.fetchall())

    def _mark_closed_pg(self) -> int:
        if psycopg is None:
            raise RuntimeError("psycopg not installed for PostgreSQL jobs repository")
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE jobs SET status='closed', updated_at=NOW() WHERE application_end_date IS NOT NULL AND application_end_date < CURRENT_DATE AND status!='closed'")
                count = cur.rowcount
            conn.commit()
            return count


def crawl_jobs_sync(database_url: str, *, seed_urls: list[str] | None = None, job_type: str = "private", query: str | None = None) -> dict[str, Any]:
    postings = asyncio.run(JobsCrawler().crawl(seed_urls=seed_urls or [], job_type=job_type, query=query))
    saved = JobsRepository(database_url).upsert_many(postings)
    return {"discovered": len(postings), "saved": saved}
