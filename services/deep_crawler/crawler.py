"""
DeepCrawler: crawls an entire institution website and returns a merged,
structured entity profile.

Replaces the legacy external extraction adapter. Uses Crawl4AI for JS-heavy
sites and httpx + BeautifulSoup for static sites. Always uses professional
headers from services.common.user_agents.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

try:
    import httpx
except ImportError:  # pragma: no cover - optional crawler dependency
    httpx = None
from bs4 import BeautifulSoup

from services.common.user_agents import add_jitter, get_headers, get_playwright_headers

try:
    from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

    CRAWL4AI_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised when optional extra is absent
    AsyncWebCrawler = BrowserConfig = CrawlerRunConfig = None  # type: ignore[assignment]
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

DEGREE_PATTERN = re.compile(r"\b(B\.?Tech|M\.?Tech|MBA|BBA|BSc|B\.Sc|MSc|M\.Sc|PhD|Diploma)\b", re.I)
DESIGNATION_PATTERN = re.compile(r"\b(Professor|Associate Professor|Assistant Professor|Dean|Director|HOD)\b", re.I)
AMOUNT_PATTERN = re.compile(r"(?:₹|Rs\.?|INR)\s*([0-9][0-9,]*(?:\.\d+)?)", re.I)
EMAIL_PATTERN = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_PATTERN = re.compile(r"(?:\+91[-\s]?)?[6-9]\d{9}|(?:\+91[-\s]?)?0?\d{2,4}[-\s]?\d{6,8}")


class DeepCrawler:
    def __init__(
        self,
        max_pages: int = 40,
        rate_limit_seconds: float = 1.5,
        timeout_seconds: int = 30,
        max_retries: int = 3,
    ):
        self.max_pages = max_pages
        self.rate_limit_seconds = rate_limit_seconds
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self._base_netloc = ""
        self._base_url = ""
        self._entity_type = "college"

    async def crawl_institution(self, base_url: str, entity_type: str = "college") -> dict:
        """Crawl an institution website and return one merged entity profile."""
        self._base_url = self._normalise_url(base_url)
        self._base_netloc = urlparse(self._base_url).netloc.lower()
        self._entity_type = entity_type
        js_required = await self._detect_js_required(self._base_url)
        pages = []
        for url in self._build_url_list(self._base_url):
            if self._skip_url(url):
                continue
            await asyncio.sleep(add_jitter(self.rate_limit_seconds))
            html = await (self._fetch_page_playwright(url) if js_required else self._fetch_page_httpx(url))
            if not html or not self._is_useful_page(html):
                continue
            extracted = self._extract_structured(html, url)
            if extracted:
                pages.append(extracted)
        merged = self._merge_pages(pages)
        merged.setdefault("name", self._name_from_url(self._base_url))
        merged["entity_type"] = entity_type
        merged["source_url"] = self._base_url
        merged["pages_crawled"] = len(pages)
        merged["crawled_at"] = datetime.now(timezone.utc).isoformat()
        return merged

    async def _detect_js_required(self, url: str) -> bool:
        """Return True when static HTML appears too sparse or framework-rendered."""
        if httpx is None:
            return CRAWL4AI_AVAILABLE
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds, follow_redirects=True) as client:
                response = await client.get(url, headers=get_headers(url))
            if response.status_code >= 400:
                return True
            html = response.text
        except Exception:
            return CRAWL4AI_AVAILABLE
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        visible_text = soup.get_text(" ", strip=True)
        fingerprints = ("ng-app", "__NEXT_DATA__", "data-reactroot", "nuxt")
        return len(visible_text) < 500 or any(fingerprint.lower() in html.lower() for fingerprint in fingerprints)

    def _build_url_list(self, base_url: str) -> list[str]:
        """Build an ordered, same-domain URL list with priority paths first."""
        base_url = self._normalise_url(base_url)
        parsed = urlparse(base_url)
        root = f"{parsed.scheme}://{parsed.netloc}"
        ordered = [base_url]
        ordered.extend(urljoin(root, path) for path in PRIORITY_PATHS)
        discovered: list[str] = []
        try:
            if httpx is None:
                raise RuntimeError("httpx is required for link discovery")
            response = httpx.get(base_url, headers=get_headers(base_url), timeout=self.timeout_seconds, follow_redirects=True)
            if response.status_code < 400:
                soup = BeautifulSoup(response.text, "html.parser")
                for anchor in soup.find_all("a", href=True):
                    href = urljoin(base_url, anchor["href"]).split("#", 1)[0]
                    if href and not self._skip_url(href):
                        discovered.append(href)
        except Exception:
            discovered = []
        ordered.extend(discovered)
        seen = set()
        result = []
        for url in ordered:
            clean = self._normalise_url(url).rstrip("/")
            if clean in seen or self._skip_url(clean):
                continue
            seen.add(clean)
            result.append(clean)
            if len(result) >= self.max_pages:
                break
        return result

    async def _fetch_page_httpx(self, url: str) -> Optional[str]:
        """Fetch a URL with httpx, professional headers, and retry policy."""
        if httpx is None:
            return None
        async with httpx.AsyncClient(timeout=self.timeout_seconds, follow_redirects=True) as client:
            retry_429_used = False
            for attempt in range(self.max_retries + 1):
                try:
                    response = await client.get(url, headers=get_headers(url))
                    if response.status_code == 403:
                        print(f"warning: 403 blocked for {url}")
                        return None
                    if response.status_code == 429:
                        if retry_429_used:
                            return None
                        retry_429_used = True
                        await asyncio.sleep(60)
                        continue
                    if 500 <= response.status_code < 600:
                        raise RuntimeError(f"server error: {response.status_code}")
                    response.raise_for_status()
                    return response.text
                except Exception as exc:
                    if attempt >= self.max_retries:
                        print(f"warning: failed to fetch {url}: {exc}")
                        return None
                    await asyncio.sleep(add_jitter(2 ** attempt))
        return None

    async def _fetch_page_playwright(self, url: str) -> Optional[str]:
        """Fetch a URL with Crawl4AI/Playwright, falling back to httpx if needed."""
        if not CRAWL4AI_AVAILABLE:
            return await self._fetch_page_httpx(url)
        try:
            browser_config = BrowserConfig(
                headless=True,
                extra_headers=get_playwright_headers(url),
                viewport_width=1366,
                viewport_height=768,
            )
            run_config = CrawlerRunConfig(wait_until="networkidle")
            async with AsyncWebCrawler(config=browser_config) as crawler:
                result = await crawler.arun(url=url, config=run_config)
            return getattr(result, "html", None) or getattr(result, "cleaned_html", None)
        except Exception as exc:
            print(f"warning: Crawl4AI failed for {url}: {exc}")
            return await self._fetch_page_httpx(url)

    def _extract_structured(self, html: str, url: str) -> dict:
        """Extract structured data from HTML using selectors and regex patterns."""
        soup = BeautifulSoup(html or "", "html.parser")
        if soup.find("input", {"type": "password"}):
            return {}
        title_h1 = " ".join(tag.get_text(" ", strip=True) for tag in soup.find_all(["title", "h1"]))
        if re.search(r"\b(404|not found|error|forbidden|access denied)\b", title_h1, re.I):
            return {}
        for tag in soup(["script", "style", "noscript", "nav", "footer", "header"]):
            tag.decompose()
        text = soup.get_text(" ", strip=True)
        if not text:
            return {}
        data: dict[str, Any] = {"source_url": url}
        about = self._extract_about(soup)
        if about:
            data["about"] = about
        courses = self._extract_courses(soup)
        if courses:
            data["courses"] = courses
        fees = self._extract_fees(text)
        if fees:
            data["fees"] = fees
        faculty = self._extract_faculty(soup)
        if faculty:
            data["faculty"] = faculty
        images = self._extract_images(soup, url)
        if images:
            data["images"] = images
        hostel = self._extract_hostel(text)
        if hostel:
            data["hostel"] = hostel
        placement = self._extract_placement(text)
        if placement:
            data["placement"] = placement
        contact = self._extract_contact(soup, text, url)
        if contact:
            data["contact"] = contact
        rankings = self._extract_rankings(text)
        if rankings:
            data["rankings"] = rankings
        accreditation = sorted(set(re.findall(r"\b(NAAC|NBA|AICTE|UGC|ABET|AACSB)\b", text, flags=re.I)))
        if accreditation:
            data["accreditation"] = accreditation
        return data if set(data) - {"source_url"} else {}

    def _merge_pages(self, pages: list[dict]) -> dict:
        """Merge extracted page dictionaries into one entity profile."""
        merged: dict[str, Any] = {
            "entity_type": self._entity_type,
            "source_url": self._base_url,
            "pages_crawled": len(pages),
            "crawled_at": datetime.now(timezone.utc).isoformat(),
        }
        course_by_name: dict[str, dict] = {}
        faculty_by_name: dict[str, dict] = {}
        images: list[str] = []
        rankings: dict[tuple[str, Any], dict] = {}
        accreditations: set[str] = set()
        for page in pages:
            if len(page.get("about", "")) > len(merged.get("about", "")):
                merged["about"] = page["about"]
            for course in page.get("courses", []):
                name = str(course.get("name", "")).strip().lower()
                if name and name not in course_by_name:
                    course_by_name[name] = course
            if page.get("fees"):
                merged["fees"] = {**merged.get("fees", {}), **{k: v for k, v in page["fees"].items() if v is not None}}
            for person in page.get("faculty", []):
                name = str(person.get("name", "")).strip().lower()
                if name and name not in faculty_by_name:
                    faculty_by_name[name] = person
            for image in page.get("images", []):
                if image not in images and len(images) < 30:
                    images.append(image)
            if page.get("hostel"):
                current = merged.get("hostel", {})
                incoming = page["hostel"]
                merged["hostel"] = {**current, **incoming, "available": current.get("available") or incoming.get("available")}
            if page.get("placement"):
                merged["placement"] = self._merge_placement(merged.get("placement", {}), page["placement"])
            if page.get("contact"):
                current = merged.get("contact", {})
                merged["contact"] = {**page["contact"], **current}
            for ranking in page.get("rankings", []):
                rankings[(ranking.get("body", ""), ranking.get("year"))] = ranking
            accreditations.update(page.get("accreditation", []))
        if course_by_name:
            merged["courses"] = list(course_by_name.values())
        if faculty_by_name:
            merged["faculty"] = list(faculty_by_name.values())
        if images:
            merged["images"] = images[:30]
        if rankings:
            merged["rankings"] = list(rankings.values())
        if accreditations:
            merged["accreditation"] = sorted(accreditations)
        return merged

    def _is_useful_page(self, html: str) -> bool:
        """Reject empty, auth, error, and redirect-only pages."""
        soup = BeautifulSoup(html or "", "html.parser")
        if soup.find("input", {"type": "password"}):
            return False
        title_h1 = " ".join(tag.get_text(" ", strip=True) for tag in soup.find_all(["title", "h1"]))
        if re.search(r"\b(404|not found|error|forbidden|access denied)\b", title_h1, re.I):
            return False
        if soup.find("meta", attrs={"http-equiv": re.compile("refresh", re.I)}):
            return False
        for tag in soup(["script", "style", "noscript", "nav", "footer", "header"]):
            tag.decompose()
        visible = soup.get_text(" ", strip=True)
        return len(visible) >= 300

    def _skip_url(self, url: str) -> bool:
        """Return True for skipped patterns and cross-domain URLs."""
        parsed = urlparse(url)
        if self._base_netloc and parsed.netloc and parsed.netloc.lower() != self._base_netloc:
            return True
        path = parsed.path.lower()
        return any(re.search(pattern, path, re.I) for pattern in SKIP_PATH_PATTERNS)

    @staticmethod
    def _normalise_url(url: str) -> str:
        if not url.startswith(("http://", "https://")):
            return f"https://{url}"
        return url

    @staticmethod
    def _name_from_url(url: str) -> str:
        host = urlparse(url).netloc.replace("www.", "")
        return host.split(".")[0].replace("-", " ").title() if host else "Unknown Institution"

    @staticmethod
    def _amount_to_int(value: str) -> int:
        return int(float(value.replace(",", "")))

    def _extract_about(self, soup: BeautifulSoup) -> str | None:
        candidates = []
        for element in soup.find_all(["p", "section", "article", "div"]):
            classes = " ".join(element.get("class", []))
            if re.search(r"nav|footer|header|menu|sidebar", classes, re.I):
                continue
            text = element.get_text(" ", strip=True)
            if len(text) >= 100:
                candidates.append(text)
        return max(candidates, key=len) if candidates else None

    def _extract_courses(self, soup: BeautifulSoup) -> list[dict]:
        courses: list[dict] = []
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            headers = [cell.get_text(" ", strip=True).lower() for cell in rows[0].find_all(["th", "td"])] if rows else []
            if not any(re.search(r"course|program|programme|degree", header, re.I) for header in headers):
                continue
            for row in rows[1:]:
                cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["td", "th"])]
                if not cells or not cells[0]:
                    continue
                item = {"name": cells[0]}
                for index, header in enumerate(headers[1:], start=1):
                    value = cells[index] if index < len(cells) else ""
                    if "duration" in header:
                        item["duration"] = value
                    elif "fee" in header:
                        amount = AMOUNT_PATTERN.search(value)
                        item["fees_inr"] = self._amount_to_int(amount.group(1)) if amount else value
                    elif "eligib" in header:
                        item["eligibility"] = value
                courses.append(item)
        for element in soup.find_all(["li", "div"]):
            text = element.get_text(" ", strip=True)
            if DEGREE_PATTERN.search(text) and 3 <= len(text) <= 180:
                courses.append({"name": text})
        deduped = []
        seen = set()
        for course in courses:
            key = course.get("name", "").strip().lower()
            if key and key not in seen:
                seen.add(key)
                deduped.append(course)
        return deduped

    def _extract_fees(self, text: str) -> dict:
        amounts = [self._amount_to_int(match) for match in AMOUNT_PATTERN.findall(text)]
        if not amounts:
            return {}
        fees: dict[str, int] = {}
        lowered = text.lower()
        if "application" in lowered:
            fees["application_fee"] = amounts[0]
        if "hostel" in lowered and len(amounts) > 1:
            fees["hostel_per_year"] = amounts[-1]
        fees["tuition_per_year"] = amounts[0]
        return fees

    def _extract_faculty(self, soup: BeautifulSoup) -> list[dict]:
        faculty = []
        for element in soup.find_all(["li", "div", "p", "tr"]):
            text = element.get_text(" ", strip=True)
            if not DESIGNATION_PATTERN.search(text):
                continue
            name_match = re.search(r"(?:Dr\.?|Prof\.?)?\s*([A-Z][A-Za-z.]+(?:\s+[A-Z][A-Za-z.]+){1,4})", text)
            designation_match = DESIGNATION_PATTERN.search(text)
            department_match = re.search(r"(?:Department of|Dept\.?)\s+([A-Za-z &]+)", text, re.I)
            if name_match:
                faculty.append({
                    "name": name_match.group(1).strip(),
                    "designation": designation_match.group(1) if designation_match else "",
                    "department": department_match.group(1).strip() if department_match else "",
                })
        return faculty

    @staticmethod
    def _extract_images(soup: BeautifulSoup, url: str) -> list[str]:
        images = []
        for image in soup.find_all("img"):
            src = image.get("src") or ""
            alt = image.get("alt") or ""
            width = int(image.get("width") or 0)
            height = int(image.get("height") or 0)
            marker = f"{src} {alt}".lower()
            if not src or re.search(r"logo|icon|social|facebook|twitter|instagram|sprite", marker):
                continue
            if width and height and (width < 200 or height < 200):
                continue
            if re.search(r"campus|facility|hostel|library|building|classroom|lab|infrastructure|gallery", marker):
                images.append(urljoin(url, src))
            if len(images) >= 20:
                break
        return list(dict.fromkeys(images))

    def _extract_hostel(self, text: str) -> dict:
        if not re.search(r"\b(hostel|accommodation|residential)\b", text, re.I):
            return {}
        data: dict[str, Any] = {"available": True, "facilities": []}
        fees = self._extract_fees(text)
        if fees.get("hostel_per_year"):
            data["fees_per_year"] = fees["hostel_per_year"]
        facilities = re.findall(r"\b(wifi|mess|laundry|gym|security|library|medical|sports)\b", text, re.I)
        if facilities:
            data["facilities"] = sorted({facility.lower() for facility in facilities})
        return data

    @staticmethod
    def _extract_placement(text: str) -> dict:
        placement: dict[str, Any] = {}
        lpa_values = [float(value) for value in re.findall(r"(\d+(?:\.\d+)?)\s*(?:LPA|lakhs per annum)", text, re.I)]
        if lpa_values:
            placement["highest_package_lpa"] = max(lpa_values)
            placement["avg_package_lpa"] = min(lpa_values)
        percentage_match = re.search(r"(\d{1,3})\s*%[^.]{0,80}(?:placement|recruit)", text, re.I)
        if not percentage_match:
            percentage_match = re.search(r"(?:placement|recruit)[^.]{0,80}(\d{1,3})\s*%", text, re.I)
        if percentage_match:
            placement["placement_percentage"] = int(percentage_match.group(1))
        recruiter_match = re.search(r"(?:top recruiters?|recruiters include)[:\s]+([A-Za-z0-9, &.-]{5,160})", text, re.I)
        if recruiter_match:
            placement["top_recruiters"] = [item.strip() for item in recruiter_match.group(1).split(",") if item.strip()]
        return placement

    @staticmethod
    def _extract_contact(soup: BeautifulSoup, text: str, url: str) -> dict:
        contact: dict[str, Any] = {"website": url}
        phone = PHONE_PATTERN.search(text)
        email = EMAIL_PATTERN.search(text)
        if phone:
            contact["phone"] = phone.group(0).strip()
        if email:
            contact["email"] = email.group(0).strip()
        address_match = re.search(r"(?:Address|Location)[:\s]+(.{20,180})", text, re.I)
        if address_match:
            contact["address"] = address_match.group(1).strip()
        map_frame = soup.find("iframe", src=re.compile("map|google", re.I))
        if map_frame and map_frame.get("src"):
            contact["map_url"] = map_frame["src"]
        return contact if set(contact) - {"website"} else {}

    @staticmethod
    def _extract_rankings(text: str) -> list[dict]:
        rankings = []
        for match in re.finditer(r"\b(NIRF|QS|Times Higher Ed|India Today)\b.{0,120}", text, re.I):
            body = match.group(0).strip()
            rank_match = re.search(r"(?:rank(?:ed)?\s*)?#?\s*(\d{1,3})", body, re.I)
            year_match = re.search(r"\b(20\d{2})\b", body)
            rankings.append({"body": body, "rank": int(rank_match.group(1)) if rank_match else None, "year": int(year_match.group(1)) if year_match else None})
        return rankings

    @staticmethod
    def _merge_placement(current: dict, incoming: dict) -> dict:
        merged = dict(current)
        for key, value in incoming.items():
            if key in {"avg_package_lpa", "highest_package_lpa", "placement_percentage"}:
                merged[key] = max(value, merged.get(key, 0))
            elif key == "top_recruiters":
                merged[key] = list(dict.fromkeys([*merged.get(key, []), *value]))
            elif key not in merged:
                merged[key] = value
        return merged


def _profile_digest(profile: dict) -> str:
    return hashlib.sha256(json.dumps(profile, sort_keys=True, default=str).encode()).hexdigest()


async def crawl_institution(
    url: str,
    entity_type: str = "college",
    max_pages: int = 40,
    rate_limit_seconds: float = 1.5,
) -> dict:
    crawler = DeepCrawler(max_pages=max_pages, rate_limit_seconds=rate_limit_seconds)
    profile = await crawler.crawl_institution(url, entity_type)
    profile.setdefault("content_hash", _profile_digest(profile))
    return profile
