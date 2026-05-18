"""
DeepCrawler: crawls an entire institution website and
returns a merged, structured entity profile.

Uses Crawl4AI for JS-heavy sites and
httpx + BeautifulSoup for static sites. Always uses
professional headers from services.common.user_agents.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin, urlparse

try:
    import httpx
except Exception:
    httpx = None
from bs4 import BeautifulSoup

from services.common.user_agents import add_jitter, get_headers, get_playwright_headers

try:
    from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

    CRAWL4AI_AVAILABLE = True
except ImportError:
    CRAWL4AI_AVAILABLE = False


PRIORITY_PATHS = [
    "/about", "/about-us", "/overview", "/about-the-institute",
    "/about-college", "/about-university", "/history",
    "/courses", "/programmes", "/programs", "/academics",
    "/departments", "/schools", "/faculties",
    "/fee-structure", "/fees", "/fee", "/tuition",
    "/faculty", "/faculty-members", "/people", "/staff",
    "/directory", "/our-faculty",
    "/hostel", "/accommodation", "/campus-life", "/residential",
    "/placement", "/placements", "/career", "/career-development",
    "/training-placement", "/campus-placement",
    "/admissions", "/admission", "/apply", "/how-to-apply",
    "/contact", "/contact-us", "/reach-us", "/location",
    "/rankings", "/achievements", "/accreditation", "/recognition",
    "/gallery", "/campus", "/infrastructure", "/facilities",
    "/news", "/events", "/announcements",
]

SKIP_PATH_PATTERNS = [
    r"\.(pdf|doc|docx|xls|xlsx|ppt|pptx|zip|rar|jpg|jpeg|png|gif|svg|ico|mp4|mp3|css|js|woff|woff2)$",
    r"/(login|signin|logout|register|signup|cart|checkout|payment|pay)",
    r"/(wp-admin|admin|dashboard|backend)",
    r"/(cdn-cgi|__cf|wp-content/uploads)",
]


class DeepCrawler:
    def __init__(self, max_pages: int = 40, rate_limit_seconds: float = 1.5, timeout_seconds: int = 30, max_retries: int = 3):
        self.max_pages = max_pages
        self.rate_limit_seconds = rate_limit_seconds
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self._base_domain = ""

    async def crawl_institution(self, base_url: str, entity_type: str = "college") -> dict:
        self._base_domain = urlparse(base_url).netloc
        js_required = await self._detect_js_required(base_url)
        urls = self._build_url_list(base_url)
        pages = []
        for url in urls:
            if self._skip_url(url):
                continue
            html = await (self._fetch_page_playwright(url) if js_required else self._fetch_page_httpx(url))
            if not html or not self._is_useful_page(html):
                continue
            pages.append(self._extract_structured(html, url))
            await asyncio.sleep(add_jitter(self.rate_limit_seconds))
        merged = self._merge_pages([p for p in pages if p])
        merged.setdefault("entity_type", entity_type)
        merged.setdefault("source_url", base_url)
        merged.setdefault("name", urlparse(base_url).netloc)
        return merged

    async def _detect_js_required(self, url: str) -> bool:
        html = await self._fetch_page_httpx(url)
        if not html:
            return True
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)
        if len(text) < 500:
            return True
        low = html.lower()
        return any(token in low for token in ["ng-app", "__next_data__", "data-reactroot", "nuxt"])

    def _build_url_list(self, base_url: str) -> list[str]:
        out = [base_url]
        origin = f"{urlparse(base_url).scheme}://{urlparse(base_url).netloc}"
        out.extend(urljoin(origin, path) for path in PRIORITY_PATHS)
        dedup = []
        seen = set()
        for url in out:
            if url in seen or self._skip_url(url):
                continue
            seen.add(url)
            dedup.append(url)
            if len(dedup) >= self.max_pages:
                break
        return dedup

    async def _fetch_page_httpx(self, url: str) -> Optional[str]:
        for attempt in range(self.max_retries):
            try:
                if httpx is None:
                    return None
                async with httpx.AsyncClient(timeout=self.timeout_seconds, follow_redirects=True) as client:
                    response = await client.get(url, headers=get_headers(url))
                if response.status_code == 403:
                    return None
                if response.status_code == 429:
                    await asyncio.sleep(60)
                    continue
                if response.status_code >= 500 and attempt < self.max_retries - 1:
                    await asyncio.sleep(add_jitter(2**attempt))
                    continue
                response.raise_for_status()
                return response.text
            except Exception:
                if attempt == self.max_retries - 1:
                    return None
                await asyncio.sleep(add_jitter(2**attempt))
        return None

    async def _fetch_page_playwright(self, url: str) -> Optional[str]:
        if not CRAWL4AI_AVAILABLE:
            return await self._fetch_page_httpx(url)
        try:
            browser_config = BrowserConfig(headless=True, extra_headers=get_playwright_headers(url), viewport_width=1366, viewport_height=768)
            run_config = CrawlerRunConfig(wait_until="networkidle")
            async with AsyncWebCrawler(config=browser_config) as crawler:
                result = await crawler.arun(url=url, config=run_config)
            html = getattr(result, "html", "") or getattr(result, "cleaned_html", "")
            return html or None
        except Exception:
            return await self._fetch_page_httpx(url)

    def _extract_structured(self, html: str, url: str) -> dict:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "nav", "footer"]):
            tag.decompose()
        text = soup.get_text(" ", strip=True)
        out = {}
        paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p") if len(p.get_text(" ", strip=True)) >= 100]
        if paragraphs:
            out["about"] = max(paragraphs, key=len)
        courses = []
        for row in soup.select("table tr"):
            cols = [c.get_text(" ", strip=True) for c in row.find_all(["th", "td"])]
            if len(cols) >= 2 and any(k in " ".join(cols).lower() for k in ["course", "program", "b.tech", "mba", "phd"]):
                if cols[0].strip().lower() in {"course", "program", "programme"}:
                    continue
                courses.append({"name": cols[0], "duration": cols[1] if len(cols) > 1 else None, "fees_inr": None, "eligibility": None})
        if courses:
            out["courses"] = courses
        fees = {}
        numbers = [int(x.replace(",", "")) for x in re.findall(r"(?:₹|Rs\.?|INR)\s*([0-9][0-9,]*)", text, flags=re.I)]
        if numbers:
            fees["tuition_per_year"] = max(numbers)
        if fees:
            out["fees"] = fees
        phones = re.findall(r"(?:\+91[-\s]?)?[6-9]\d{9}", text)
        emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
        if phones or emails:
            out["contact"] = {"phone": phones[0] if phones else None, "email": emails[0] if emails else None, "address": None, "website": url, "map_url": None}
        return out

    def _merge_pages(self, pages: list[dict]) -> dict:
        merged = {"entity_type": "college", "source_url": "", "pages_crawled": len(pages), "crawled_at": datetime.now(timezone.utc).isoformat()}
        courses = []
        faculty = []
        images = []
        rankings = []
        acc = set()
        for page in pages:
            if page.get("about") and len(page["about"]) > len(merged.get("about", "")):
                merged["about"] = page["about"]
            courses.extend(page.get("courses", []))
            faculty.extend(page.get("faculty", []))
            images.extend(page.get("images", []))
            rankings.extend(page.get("rankings", []))
            acc.update(page.get("accreditation", []))
            merged["fees"] = {**merged.get("fees", {}), **page.get("fees", {})}
            if page.get("contact"):
                existing = merged.get("contact", {})
                merged["contact"] = {k: existing.get(k) or v for k, v in page["contact"].items()}
        if courses:
            by_name = {}
            for course in courses:
                by_name[course.get("name")] = course
            merged["courses"] = list(by_name.values())
        if faculty:
            by_name = {}
            for member in faculty:
                by_name[member.get("name")] = member
            merged["faculty"] = list(by_name.values())
        if images:
            merged["images"] = list(dict.fromkeys(images))[:30]
        if rankings:
            seen = set()
            dedup = []
            for rank in rankings:
                key = (rank.get("body"), rank.get("year"))
                if key in seen:
                    continue
                seen.add(key)
                dedup.append(rank)
            merged["rankings"] = dedup
        if acc:
            merged["accreditation"] = sorted(acc)
        return merged

    def _is_useful_page(self, html: str) -> bool:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)
        if len(text) < 300:
            return False
        if soup.select_one("form input[type='password']"):
            return False
        title = (soup.title.get_text(" ", strip=True) if soup.title else "").lower()
        h1 = (soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else "").lower()
        if any(token in title or token in h1 for token in ["404", "not found", "error"]):
            return False
        return True

    def _skip_url(self, url: str) -> bool:
        parsed = urlparse(url)
        if self._base_domain and parsed.netloc and parsed.netloc != self._base_domain:
            return True
        target = (parsed.path or "").lower()
        return any(re.search(pattern, target) for pattern in SKIP_PATH_PATTERNS)


async def crawl_institution(url: str, entity_type: str = "college", max_pages: int = 40, rate_limit_seconds: float = 1.5) -> dict:
    crawler = DeepCrawler(max_pages=max_pages, rate_limit_seconds=rate_limit_seconds)
    return await crawler.crawl_institution(url, entity_type)
