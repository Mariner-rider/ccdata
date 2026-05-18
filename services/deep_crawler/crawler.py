"""Deep Crawl4AI-first institution crawler."""

from __future__ import annotations

import asyncio
import importlib.util
import re
from dataclasses import dataclass
from typing import Any
from urllib import robotparser
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from services.common.user_agents import add_jitter, get_headers, get_random_ua

try:
    import httpx
except Exception:  # pragma: no cover - optional dependency path
    httpx = None


@dataclass
class _PageTarget:
    url: str
    priority: int


class DeepCrawler:
    """
    Crawls an entire institution website (not just landing page).
    Uses Crawl4AI AsyncWebCrawler with BrowserConfig for JS sites.
    Falls back to httpx + BeautifulSoup for static sites.
    """

    PRIORITY_PATHS = [
        "/about", "/about-us", "/overview", "/history",
        "/courses", "/programmes", "/programs", "/academics",
        "/departments", "/schools-of-study",
        "/fee-structure", "/fees", "/fee-schedule",
        "/faculty", "/faculty-members", "/people", "/directory",
        "/hostel", "/accommodation", "/campus-life",
        "/placement", "/placements", "/career-development",
        "/admissions", "/admission", "/apply",
        "/contact", "/contact-us", "/reach-us",
        "/rankings", "/achievements", "/accreditation",
        "/gallery", "/campus", "/infrastructure",
    ]

    def __init__(self) -> None:
        self.rate_limit_seconds = 1.5

    async def crawl_institution(
        self,
        base_url: str,
        entity_type: str,
        max_pages: int = 40,
        rate_limit_seconds: float = 1.5,
    ) -> dict:
        """
        Returns a merged dict of all data found across all pages.
        Never returns a heading-only or empty record.
        Merges multi-page data into one entity profile.
        """
        self.rate_limit_seconds = rate_limit_seconds
        pages = []
        for target in self._build_targets(base_url, max_pages):
            if not self._robots_allowed(target.url):
                continue
            html = await self._crawl_page(target.url)
            extracted = self._extract_structured(html, target.url)
            if extracted:
                pages.append(extracted)
        merged = self._merge_pages(pages)
        if not merged:
            return {}
        merged["entity_type"] = entity_type
        merged["source_url"] = base_url
        merged["pages_crawled"] = len(pages)
        return merged

    async def _crawl_page(self, url: str) -> str:
        """
        Returns page HTML. Uses Playwright (headless Chromium)
        with headers from get_headers(url). Waits for networkidle before extracting.
        Falls back to httpx for non-JS pages.
        Adds jitter between requests using add_jitter().
        """
        await asyncio.sleep(max(0.0, add_jitter(self.rate_limit_seconds)))
        if importlib.util.find_spec("crawl4ai") is not None:
            try:
                crawl4ai = __import__("crawl4ai")
                browser_config = crawl4ai.BrowserConfig(
                    headless=True,
                    headers=get_headers(url),
                    browser_type="chromium",
                )
                run_config = getattr(crawl4ai, "CrawlerRunConfig", None)
                kwargs = {"wait_until": "networkidle"} if run_config else {}
                async with crawl4ai.AsyncWebCrawler(config=browser_config) as crawler:
                    result = await crawler.arun(url=url, config=run_config(**kwargs) if run_config else None)
                    html = getattr(result, "html", None) or getattr(result, "cleaned_html", "")
                    if html:
                        return html
            except Exception:
                pass
        if httpx is None:
            raise RuntimeError("httpx is required for deep crawler fallback")
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            response = await client.get(url, headers=get_headers(url))
            response.raise_for_status()
            return response.text

    def _extract_structured(self, html: str, url: str) -> dict:
        """
        Extracts structured data using CSS/XPath selectors.
        No LLM required — uses pattern matching for about text, courses,
        fees, faculty, images, contact info and placement stats.
        Returns {} if page has no useful content.
        """
        soup = BeautifulSoup(html or "", "html.parser")
        for tag in soup(["script", "style", "noscript", "nav", "footer", "header"]):
            tag.decompose()
        text = soup.get_text(" ", strip=True)
        data: dict[str, Any] = {"source_url": url}

        paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all(["p", "section", "article"])]
        paragraphs = [p for p in paragraphs if len(p.split()) >= 12]
        if paragraphs:
            data["about"] = max(paragraphs, key=len)

        courses = self._extract_course_rows(soup)
        if courses:
            data["courses"] = courses

        fees = self._extract_fee_rows(soup, text)
        if fees:
            data["fees"] = fees

        faculty = self._extract_faculty(soup, text)
        if faculty:
            data["faculty"] = faculty

        images = self._extract_images(soup, url)
        if images:
            data["images"] = images

        contact = self._extract_contact(soup, text)
        if contact:
            data["contact"] = contact

        placement_stats = sorted(set(re.findall(r"\b(?:\d{1,3}%|\d+(?:\.\d+)?\s*LPA)\b", text, flags=re.I)))
        if placement_stats:
            data["placement_stats"] = placement_stats

        useful_keys = set(data) - {"source_url"}
        return data if useful_keys else {}

    def _merge_pages(self, pages: list[dict]) -> dict:
        """
        Merges extracted dicts from multiple pages into one.
        Later pages append to lists (courses, faculty, images).
        Later pages overwrite scalars only if longer/more complete.
        Removes duplicates from lists.
        """
        merged: dict[str, Any] = {}
        for page in pages:
            for key, value in page.items():
                if key == "source_url" or value in (None, "", [], {}):
                    continue
                if isinstance(value, list):
                    merged[key] = self._dedupe_list([*merged.get(key, []), *value])
                elif isinstance(value, dict):
                    current = merged.get(key, {}) if isinstance(merged.get(key), dict) else {}
                    merged[key] = {**current, **{k: v for k, v in value.items() if v}}
                elif len(str(value)) > len(str(merged.get(key, ""))):
                    merged[key] = value
        return merged

    def _build_targets(self, base_url: str, max_pages: int) -> list[_PageTarget]:
        parsed = urlparse(base_url)
        root = f"{parsed.scheme}://{parsed.netloc}"
        targets = [_PageTarget(base_url, 100)]
        targets.extend(_PageTarget(urljoin(root, path), 50 - idx) for idx, path in enumerate(self.PRIORITY_PATHS))
        seen = set()
        out = []
        for target in targets:
            canon = target.url.rstrip("/")
            if canon in seen:
                continue
            seen.add(canon)
            out.append(target)
            if len(out) >= max_pages:
                break
        return out

    def _robots_allowed(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return True
        rp = robotparser.RobotFileParser(f"{parsed.scheme}://{parsed.netloc}/robots.txt")
        try:
            rp.read()
            return rp.can_fetch(get_random_ua(), url)
        except Exception:
            return False

    @staticmethod
    def _extract_course_rows(soup: BeautifulSoup) -> list[dict[str, str]]:
        courses = []
        for table in soup.find_all("table"):
            headers = [cell.get_text(" ", strip=True).lower() for cell in table.find_all("th")]
            if not any(any(word in h for word in ("course", "program", "programme")) for h in headers):
                continue
            for row in table.find_all("tr"):
                cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["td", "th"])]
                if len(cells) >= 2 and not all(c.lower() in headers for c in cells):
                    courses.append({"name": cells[0], "details": " | ".join(cells[1:])})
        return courses

    @staticmethod
    def _extract_fee_rows(soup: BeautifulSoup, text: str) -> list[str]:
        fees = []
        for table in soup.find_all("table"):
            table_text = table.get_text(" ", strip=True)
            if re.search(r"₹|\bRs\.?\b|\bINR\b", table_text, re.I):
                fees.extend(row.get_text(" | ", strip=True) for row in table.find_all("tr") if row.get_text(strip=True))
        fees.extend(re.findall(r"(?:₹|Rs\.?|INR)\s*\d[\d,]*(?:\.\d+)?", text, flags=re.I))
        return [fee for fee in dict.fromkeys(fees) if fee]

    @staticmethod
    def _extract_faculty(soup: BeautifulSoup, text: str) -> list[str]:
        faculty = []
        for item in soup.select(".faculty, .profile, .teacher, .people, li"):
            item_text = item.get_text(" ", strip=True)
            if re.search(r"\b(Dr\.|Prof\.|Professor|Associate Professor|Assistant Professor|Lecturer)\b", item_text):
                faculty.append(item_text)
        faculty.extend(re.findall(r"(?:Dr\.|Prof\.)\s+[A-Z][A-Za-z. ]{2,60}", text))
        return list(dict.fromkeys(faculty))

    @staticmethod
    def _extract_images(soup: BeautifulSoup, url: str) -> list[str]:
        images = []
        skip = re.compile(r"logo|icon|sprite|favicon", re.I)
        keep = re.compile(r"campus|gallery|infra|hostel|building|classroom|library", re.I)
        for image in soup.find_all("img"):
            src = image.get("src") or ""
            alt = image.get("alt") or ""
            if not src or skip.search(src) or skip.search(alt):
                continue
            if keep.search(src) or keep.search(alt):
                images.append(urljoin(url, src))
        return list(dict.fromkeys(images))

    @staticmethod
    def _extract_contact(soup: BeautifulSoup, text: str) -> dict[str, Any]:
        contact: dict[str, Any] = {}
        emails = sorted(set(re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)))
        phones = sorted(set(re.findall(r"(?:\+91[-\s]?)?\b[6-9]\d{9}\b|\b0\d{2,4}[-\s]?\d{6,8}\b", text)))
        maps = [iframe.get("src") for iframe in soup.find_all("iframe") if "map" in (iframe.get("src") or "").lower()]
        if emails:
            contact["emails"] = emails
        if phones:
            contact["phones"] = phones
        if maps:
            contact["maps"] = maps
        return contact

    @staticmethod
    def _dedupe_list(values: list[Any]) -> list[Any]:
        seen = set()
        out = []
        for value in values:
            key = tuple(sorted(value.items())) if isinstance(value, dict) else str(value).strip().lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(value)
        return out
