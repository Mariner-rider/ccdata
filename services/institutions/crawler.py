from __future__ import annotations

import asyncio
import csv
import hashlib
import importlib
import importlib.util
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import robotparser
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

from services.common.user_agents import add_jitter, get_headers, get_random_ua

boto3 = importlib.import_module("boto3") if importlib.util.find_spec("boto3") else None
BotoConfig = importlib.import_module("botocore.client").Config if importlib.util.find_spec("botocore") else None

if importlib.util.find_spec("crawl4ai"):
    _crawl4ai = importlib.import_module("crawl4ai")
    AsyncWebCrawler = getattr(_crawl4ai, "AsyncWebCrawler", None)
    BrowserConfig = getattr(_crawl4ai, "BrowserConfig", None)
    CrawlerRunConfig = getattr(_crawl4ai, "CrawlerRunConfig", None)
else:
    AsyncWebCrawler = None
    BrowserConfig = None
    CrawlerRunConfig = None

if importlib.util.find_spec("crawl4ai") and importlib.util.find_spec("crawl4ai.extraction_strategy"):
    _strategies = importlib.import_module("crawl4ai.extraction_strategy")
    JsonCssExtractionStrategy = getattr(_strategies, "JsonCssExtractionStrategy", None)
    LLMExtractionStrategy = getattr(_strategies, "LLMExtractionStrategy", None)
else:
    JsonCssExtractionStrategy = None
    LLMExtractionStrategy = None

litellm = importlib.import_module("litellm") if importlib.util.find_spec("litellm") else None

LOGGER = logging.getLogger(__name__)

ENTITY_TYPES = {"college", "university", "school", "coaching_centre", "abroad_university"}
PRIORITY_PATTERNS = (
    "/about",
    "/courses",
    "/programmes",
    "/programs",
    "/academics",
    "/departments",
    "/fee-structure",
    "/fees",
    "/faculty",
    "/hostel",
    "/placement",
    "/placements",
    "/campus-life",
    "/contact",
)
ABROAD_TLDS = {"ac.uk", "edu", "edu.au", "ca", "de", "fr", "sg", "nz", "uk", "us"}
INDIAN_HINTS = {"india", "delhi", "mumbai", "bengaluru", "bangalore", "chennai", "kolkata", "pune", "hyderabad"}

EMPTY_PROFILE: dict[str, Any] = {
    "entity_type": "college",
    "name": "",
    "location": {"city": "", "state": "", "country": ""},
    "about": "",
    "courses": [],
    "fee_structure": {"application_fee": None, "tuition_per_year": None, "hostel_per_year": None, "other_charges": None},
    "images": [],
    "faculty": [],
    "hostel": {"available": False, "capacity": None, "fees_per_year": None, "facilities": []},
    "placement": {"avg_package_lpa": None, "highest_package_lpa": None, "placement_percentage": None, "top_recruiters": []},
    "reviews": [],
    "accreditation": [],
    "contact": {"phone": "", "email": "", "address": "", "website": "", "map_link": ""},
    "ranking": [],
}


@dataclass
class CrawlPage:
    url: str
    html: str
    markdown: str = ""
    depth: int = 0
    raw_s3_key: str | None = None
    extracted: dict[str, Any] = field(default_factory=dict)


@dataclass
class CrawlResult:
    record: dict[str, Any] | None
    status: str
    reason: str = ""
    pages_crawled: int = 0
    raw_html_keys: list[str] = field(default_factory=list)


def clone_empty_profile() -> dict[str, Any]:
    return json.loads(json.dumps(EMPTY_PROFILE))


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme in {"http", "https"}:
        return parsed._replace(fragment="").geturl().rstrip("/")
    return url


def content_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def _best_text(nodes: list[Any]) -> str:
    return "\n".join(t.get_text(" ", strip=True) for t in nodes if t.get_text(" ", strip=True)).strip()


def _numbers(text: str) -> list[float]:
    return [float(x.replace(",", "")) for x in re.findall(r"\d[\d,]*(?:\.\d+)?", text or "")]


def _money_inr(text: str) -> int | None:
    nums = _numbers(text)
    if not nums:
        return None
    n = nums[0]
    lower = text.lower()
    if "lakh" in lower or "lac" in lower:
        n *= 100000
    elif "crore" in lower:
        n *= 10000000
    return int(n)


def _lpa(text: str) -> float | None:
    nums = _numbers(text)
    return nums[0] if nums else None


class S3Store:
    def __init__(self) -> None:
        self.endpoint = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
        self.access_key = os.getenv("S3_ACCESS_KEY", "minio")
        self.secret_key = os.getenv("S3_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", ""))
        self.raw_bucket = os.getenv("S3_BUCKET", "raw-html")
        self.image_bucket = os.getenv("INSTITUTION_IMAGE_BUCKET", "institution-images")
        self.enabled = os.getenv("INSTITUTION_S3_ENABLED", "true").lower() != "false" and boto3 is not None
        self.client = None
        if self.enabled:
            self.client = boto3.client(
                "s3",
                endpoint_url=self.endpoint,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                config=BotoConfig(signature_version="s3v4") if BotoConfig else None,
            )

    def put_bytes(self, bucket: str, key: str, body: bytes, content_type: str) -> str:
        if self.client is not None:
            self.client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)
            return f"s3://{bucket}/{key}"
        return f"disabled://{bucket}/{key}"

    def put_html(self, url: str, html: str) -> str:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        digest = hashlib.sha256(url.encode()).hexdigest()
        return self.put_bytes(self.raw_bucket, f"institutions/raw/{digest[:2]}/{digest}_{ts}.html", html.encode(), "text/html")

    def put_image_url(self, image_url: str) -> str:
        ext = Path(urlparse(image_url).path).suffix or ".img"
        key = f"institutions/images/{hashlib.sha256(image_url.encode()).hexdigest()}{ext}"
        if self.client is None:
            return f"disabled://{self.image_bucket}/{key}"
        if image_url.startswith("file://"):
            data = Path(urlparse(image_url).path).read_bytes()
            ctype = "image/jpeg"
        else:
            with urlopen(Request(image_url, headers=get_headers(image_url)), timeout=20) as resp:  # noqa: S310
                data = resp.read()
                ctype = resp.headers.get_content_type() or "application/octet-stream"
        return self.put_bytes(self.image_bucket, key, data, ctype)


class InstitutionCrawler:
    def __init__(self, *, max_pages: int | None = None, max_depth: int | None = None, rate_limit_seconds: float = 1.0, s3_store: S3Store | None = None) -> None:
        self.max_pages = max_pages or int(os.getenv("INSTITUTION_MAX_PAGES", os.getenv("CRAWL_MAX_PAGES_PER_SOURCE", "50")))
        self.max_depth = max_depth or int(os.getenv("INSTITUTION_MAX_DEPTH", "4"))
        self.rate_limit_seconds = rate_limit_seconds
        self.s3 = s3_store or S3Store()
        self._last_domain_fetch: dict[str, float] = {}

    async def crawl(self, url: str, entity_type: str) -> CrawlResult:
        if entity_type not in ENTITY_TYPES:
            return CrawlResult(None, "quarantined", f"invalid_entity_type:{entity_type}")
        url = normalize_url(url)
        if self._is_abroad(url, ""):
            entity_type = "abroad_university"

        try:
            pages = await self._deep_crawl(url)
            if not pages:
                return CrawlResult(None, "quarantined", "no_pages_crawled")
            for page in pages:
                page.extracted = await self._extract_page(page, entity_type)
            profile = self._merge_pages(url, entity_type, pages)
            ok, reason = validate_profile(profile)
            if not ok:
                return CrawlResult(None, "quarantined", reason, len(pages), [p.raw_s3_key for p in pages if p.raw_s3_key])
            record = self._to_crawl_record(url, profile, pages)
            return CrawlResult(record, "created", pages_crawled=len(pages), raw_html_keys=[p.raw_s3_key for p in pages if p.raw_s3_key])
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Institution crawl failed for %s", url)
            return CrawlResult(None, "quarantined", str(exc))

    async def _deep_crawl(self, seed: str) -> list[CrawlPage]:
        seen: set[str] = set()
        queue: list[tuple[int, str]] = [(0, seed)]
        pages: list[CrawlPage] = []
        base = urlparse(seed)
        while queue and len(pages) < self.max_pages:
            queue.sort(key=lambda item: (0 if self._is_priority(item[1]) else 1, item[0], item[1]))
            depth, url = queue.pop(0)
            url = normalize_url(url)
            if url in seen or depth > self.max_depth:
                continue
            seen.add(url)
            if not self._same_site(seed, url) or not self._robots_allowed(url):
                continue
            html, markdown = await self._fetch(url)
            raw_key = self.s3.put_html(url, html)
            page = CrawlPage(url=url, html=html, markdown=markdown, depth=depth, raw_s3_key=raw_key)
            pages.append(page)
            for link in self._links(url, html):
                link = normalize_url(link)
                parsed = urlparse(link)
                if parsed.scheme not in {"http", "https", "file"}:
                    continue
                if base.scheme != "file" and parsed.netloc != base.netloc:
                    continue
                if any(parsed.path.lower().endswith(ext) for ext in (".pdf", ".zip", ".jpg", ".jpeg", ".png", ".gif", ".webp")):
                    continue
                if link not in seen:
                    queue.append((depth + 1, link))
        return pages

    async def _fetch(self, url: str) -> tuple[str, str]:
        max_retries = int(os.getenv("INSTITUTION_CRAWL_RETRIES", "3"))
        base_delay = float(os.getenv("INSTITUTION_CRAWL_BACKOFF_SECONDS", "0.5"))
        for attempt in range(max_retries):
            try:
                return await self._fetch_once(url)
            except Exception:
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(max(0.0, add_jitter(base_delay * (2**attempt))))
        raise RuntimeError("unreachable fetch retry state")

    async def _fetch_once(self, url: str) -> tuple[str, str]:
        await self._rate_limit(url)
        if AsyncWebCrawler is not None and not url.startswith("file://"):
            browser_config = BrowserConfig(headless=True, java_script_enabled=True)
            run_config = CrawlerRunConfig() if CrawlerRunConfig is not None else None
            async with AsyncWebCrawler(config=browser_config) as crawler:
                result = await crawler.arun(url=url, config=run_config)
            html = getattr(result, "html", "") or getattr(result, "cleaned_html", "") or ""
            markdown = getattr(result, "markdown", "") or ""
            if html:
                return html, markdown
        if url.startswith("file://"):
            parsed = urlparse(url)
            path = parsed.path or (parsed.netloc + parsed.path)
            return Path(path).read_text(encoding="utf-8"), ""
        with urlopen(Request(url, headers=get_headers(url)), timeout=30) as resp:  # noqa: S310
            return resp.read().decode(resp.headers.get_content_charset() or "utf-8", errors="replace"), ""

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
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        rp = robotparser.RobotFileParser(robots_url)
        try:
            rp.read()
            return rp.can_fetch(get_random_ua(), url)
        except Exception:
            LOGGER.warning("robots.txt check failed for %s; allowing crawl", url)
            return True

    @staticmethod
    def _links(url: str, html: str) -> list[str]:
        soup = BeautifulSoup(html or "", "lxml")
        return [urljoin(url, a.get("href")) for a in soup.select("a[href]")]

    @staticmethod
    def _same_site(seed: str, url: str) -> bool:
        return seed.startswith("file://") or urlparse(seed).netloc == urlparse(url).netloc

    @staticmethod
    def _is_priority(url: str) -> bool:
        path = urlparse(url).path.lower()
        return any(pattern in path for pattern in PRIORITY_PATTERNS)

    async def _extract_page(self, page: CrawlPage, entity_type: str) -> dict[str, Any]:
        llm = await self._llm_extract(page, entity_type)
        if llm:
            return llm
        return self._css_extract(page, entity_type)

    async def _llm_extract(self, page: CrawlPage, entity_type: str) -> dict[str, Any] | None:
        if litellm is None or os.getenv("INSTITUTION_LLM_ENABLED", "false").lower() != "true":
            return None
        text = BeautifulSoup(page.html, "lxml").get_text("\n", strip=True)[:50000]
        prompt = (
            "Extract one CollegeCue institution JSON object from this page. "
            "Return only JSON matching these keys: entity_type,name,location,about,courses,fee_structure,"
            "images,faculty,hostel,placement,reviews,accreditation,contact,ranking. "
            f"Requested entity_type={entity_type}. URL={page.url}\n\n{text}"
        )
        for attempt in range(3):
            try:
                response = await litellm.acompletion(
                    model=os.getenv("INSTITUTION_LLM_MODEL", "claude-sonnet-4-20250514"),
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0,
                    response_format={"type": "json_object"},
                )
                content = response["choices"][0]["message"]["content"]
                return json.loads(content)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("LLM extraction failed for %s attempt=%s: %s", page.url, attempt + 1, exc)
                await asyncio.sleep(max(0.0, add_jitter(2**attempt)))
        return None

    def _css_extract(self, page: CrawlPage, entity_type: str) -> dict[str, Any]:
        # crawl4ai JsonCssExtractionStrategy is intentionally optional; the deterministic CSS fallback below
        # provides equivalent structured extraction in offline tests and in degraded production mode.
        profile = clone_empty_profile()
        profile["entity_type"] = entity_type
        soup = BeautifulSoup(page.html, "lxml")
        title = (soup.find("h1") or soup.find("title"))
        profile["name"] = title.get_text(" ", strip=True) if title else ""
        text = soup.get_text("\n", strip=True)
        lower_url = page.url.lower()

        about_nodes = soup.select("#about, .about, [class*='overview'], [id*='overview'], main p, article p")
        if "about" in lower_url or "overview" in lower_url or not profile["about"]:
            profile["about"] = _best_text(about_nodes[:5])[:5000]

        emails = re.findall(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", text, re.I)
        phones = re.findall(r"(?:\+?91[-\s]?)?[6-9]\d{9}|\+?\d[\d\s().-]{7,}\d", text)
        profile["contact"].update({"email": emails[0] if emails else "", "phone": phones[0] if phones else "", "website": page.url})
        address = soup.select_one("address, .address, [class*='address'], [id*='address']")
        if address:
            profile["contact"]["address"] = address.get_text(" ", strip=True)
        map_link = soup.select_one("a[href*='google.com/maps'], a[href*='maps.app.goo.gl']")
        if map_link:
            profile["contact"]["map_link"] = urljoin(page.url, map_link.get("href"))

        loc_text = " ".join([profile["contact"]["address"], text[:2000]]).lower()
        country = "India" if ".in" in urlparse(page.url).netloc or any(h in loc_text for h in INDIAN_HINTS) else ""
        profile["location"] = {"city": self._meta(soup, "city"), "state": self._meta(soup, "state"), "country": country}

        for row in soup.select("tr, li, .course, [class*='course'], [class*='program']"):
            rt = row.get_text(" ", strip=True)
            if re.search(r"\b(B\.?Tech|M\.?Tech|MBA|BBA|BSc|MSc|BA|MA|PhD|Diploma|Course|Program|Programme)\b", rt, re.I):
                profile["courses"].append({"name": rt[:180], "duration": self._match(rt, r"(\d+\s*(?:year|yr|month|semester)s?)"), "fees_inr": _money_inr(rt), "eligibility": "", "seats": None})
        profile["courses"] = _dedupe_dicts(profile["courses"], "name")[:100]

        if "fee" in lower_url or re.search(r"tuition|hostel fee|application fee", text, re.I):
            profile["fee_structure"] = {
                "application_fee": _money_inr(self._line(text, "application fee")),
                "tuition_per_year": _money_inr(self._line(text, "tuition")),
                "hostel_per_year": _money_inr(self._line(text, "hostel")),
                "other_charges": _money_inr(self._line(text, "other")),
            }

        image_urls = [urljoin(page.url, img.get("src")) for img in soup.select("img[src]")]
        profile["images"] = [u for u in image_urls if not re.search(r"logo|icon|sprite", u, re.I)][:30]

        if "faculty" in lower_url or re.search(r"professor|faculty|dean|lecturer", text, re.I):
            for node in soup.select("tr, li, .faculty, [class*='faculty'], [class*='professor'], [class*='teacher']"):
                nt = node.get_text(" ", strip=True)
                if re.search(r"professor|dean|lecturer|teacher|faculty|hod", nt, re.I):
                    img = node.select_one("img[src]")
                    profile["faculty"].append({"name": nt.split("-")[0][:120], "designation": self._match(nt, r"(Professor|Dean|Lecturer|Teacher|HOD|Director)[\w\s]*"), "department": "", "qualification": self._match(nt, r"(Ph\.?D\.?|M\.?Tech|MSc|MA|MBA|B\.?Ed)[\w\s.]*"), "image_url": urljoin(page.url, img.get("src")) if img else ""})
            profile["faculty"] = _dedupe_dicts(profile["faculty"], "name")[:100]

        hostel_line = self._line(text, "hostel")
        if hostel_line:
            profile["hostel"] = {"available": True, "capacity": int(_numbers(hostel_line)[0]) if _numbers(hostel_line) else None, "fees_per_year": _money_inr(hostel_line), "facilities": self._facilities(hostel_line)}

        placement = profile["placement"]
        placement["avg_package_lpa"] = _lpa(self._line(text, "average package"))
        placement["highest_package_lpa"] = _lpa(self._line(text, "highest package"))
        pp_line = self._line(text, "placement")
        if "%" in pp_line and _numbers(pp_line):
            placement["placement_percentage"] = _numbers(pp_line)[0]
        rec_line = self._line(text, "recruiters")
        placement["top_recruiters"] = [x.strip() for x in re.split(r",|\|", rec_line) if 2 < len(x.strip()) < 80][:30]

        for body in ("NAAC", "NBA", "UGC", "AICTE", "NIRF"):
            if body.lower() in text.lower():
                if body == "NIRF":
                    profile["ranking"].append({"body": body, "rank": self._match(self._line(text, body), r"(?:rank(?:ed)?\s*)?(\d{1,4})"), "year": self._match(self._line(text, body), r"(20\d{2})")})
                else:
                    profile["accreditation"].append({"body": body, "grade": self._match(self._line(text, body), r"A\+\+|A\+|A|B\+\+|B\+|B"), "valid_until": self._match(self._line(text, body), r"valid[^\d]*(\d{4})")})
        return profile

    def _merge_pages(self, official_url: str, entity_type: str, pages: list[CrawlPage]) -> dict[str, Any]:
        merged = clone_empty_profile()
        merged["entity_type"] = "abroad_university" if self._is_abroad(official_url, " ".join(p.html[:2000] for p in pages)) else entity_type
        merged["contact"]["website"] = official_url
        sources: dict[str, str] = {}
        for page in pages:
            profile = page.extracted or {}
            if profile.get("name") and (not merged.get("name") or page.depth == 0):
                merged["name"] = profile["name"]
                sources["name"] = page.url
            if profile.get("about") and len(str(profile["about"])) > len(str(merged.get("about", ""))):
                merged["about"] = profile["about"]
                sources["about"] = page.url
            if profile.get("location", {}).get("country") and not merged["location"].get("country"):
                merged["location"] = profile["location"]
            for key, value in profile.get("contact", {}).items():
                if value and not merged["contact"].get(key):
                    merged["contact"][key] = value
            merged["courses"].extend(profile.get("courses") or [])
            merged["images"].extend(profile.get("images") or [])
            merged["faculty"].extend(profile.get("faculty") or [])
            merged["reviews"].extend(profile.get("reviews") or [])
            merged["accreditation"].extend(profile.get("accreditation") or [])
            merged["ranking"].extend(profile.get("ranking") or [])
            for key, value in profile.get("fee_structure", {}).items():
                if value and not merged["fee_structure"].get(key):
                    merged["fee_structure"][key] = value
            if profile.get("hostel", {}).get("available"):
                merged["hostel"] = profile["hostel"]
            for key, value in profile.get("placement", {}).items():
                if value and not merged["placement"].get(key):
                    merged["placement"][key] = value
        merged["courses"] = _dedupe_dicts(merged["courses"], "name")
        merged["faculty"] = _dedupe_dicts(merged["faculty"], "name")
        merged["images"] = list(dict.fromkeys(merged["images"]))[:100]
        stored_images = []
        for image_url in merged["images"]:
            try:
                stored_images.append({"source_url": image_url, "object_url": self.s3.put_image_url(image_url)})
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Failed to store institution image %s: %s", image_url, exc)
        merged["metadata"] = {"page_count": len(pages), "pages": [p.url for p in pages], "field_sources": sources, "raw_html_keys": [p.raw_s3_key for p in pages if p.raw_s3_key], "stored_images": stored_images}
        return merged

    def _to_crawl_record(self, official_url: str, profile: dict[str, Any], pages: list[CrawlPage]) -> dict[str, Any]:
        missing = missing_fields(profile)
        return {
            "entity_type": profile["entity_type"],
            "title": profile.get("name") or official_url,
            "source_url": official_url,
            "official_url": official_url,
            "fields": profile,
            "info": {"name": profile.get("name"), "location": profile.get("location"), "contact": profile.get("contact"), "about": profile.get("about")},
            "courses_and_fees": {"courses": profile.get("courses", []), "fee_structure": profile.get("fee_structure", {})},
            "gallery": profile.get("images", []),
            "faculty": profile.get("faculty", []),
            "hostel": profile.get("hostel", {}),
            "placement": profile.get("placement", {}),
            "reviews": profile.get("reviews", []),
            "metadata": profile.get("metadata", {}) | {"crawl4ai": AsyncWebCrawler is not None, "llm_model": os.getenv("INSTITUTION_LLM_MODEL", "claude-sonnet-4-20250514")},
            "missing_fields": missing,
            "confidence_score": confidence(profile),
            "trust_tier": "official",
            "content_hash": content_hash(profile),
            "last_crawled_at": datetime.now(timezone.utc).isoformat(),
        }

    @staticmethod
    def _is_abroad(url: str, text: str) -> bool:
        host = urlparse(url).netloc.lower().removeprefix("www.")
        if host.endswith(".in") or "india" in text.lower():
            return False
        return any(host.endswith("." + tld) or host == tld for tld in ABROAD_TLDS)

    @staticmethod
    def _meta(soup: BeautifulSoup, name: str) -> str:
        node = soup.select_one(f"meta[name='{name}'], meta[property='place:{name}']")
        return node.get("content", "").strip() if node else ""

    @staticmethod
    def _match(text: str, pattern: str) -> str:
        match = re.search(pattern, text or "", re.I)
        return (match.group(1) if match.groups() else match.group(0)).strip() if match else ""

    @staticmethod
    def _line(text: str, needle: str) -> str:
        for line in text.splitlines():
            if needle.lower() in line.lower():
                return line.strip()
        return ""

    @staticmethod
    def _facilities(text: str) -> list[str]:
        known = ["wifi", "mess", "laundry", "security", "gym", "library", "sports", "medical"]
        return [k for k in known if k in text.lower()]


def _dedupe_dicts(items: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for item in items:
        marker = str(item.get(key, "")).strip().lower()
        if marker and marker not in seen:
            seen.add(marker)
            out.append(item)
    return out


def missing_fields(profile: dict[str, Any]) -> list[str]:
    missing = []
    for field in ("entity_type", "name", "location", "about", "courses", "contact"):
        value = profile.get(field)
        if field == "location":
            if not any(value.values()) if isinstance(value, dict) else not value:
                missing.append(field)
        elif field == "contact":
            if not any(value.values()) if isinstance(value, dict) else not value:
                missing.append(field)
        elif not value:
            missing.append(field)
    return missing


def confidence(profile: dict[str, Any]) -> float:
    required = 6
    found = required - len(missing_fields(profile))
    bonus = sum(1 for f in ("faculty", "hostel", "placement", "fee_structure", "images") if profile.get(f)) * 0.04
    return round(min(0.99, max(0.0, found / required * 0.8 + bonus)), 3)


def validate_profile(profile: dict[str, Any]) -> tuple[bool, str]:
    if profile.get("entity_type") not in ENTITY_TYPES:
        return False, "schema_invalid:entity_type"
    if not profile.get("name"):
        return False, "schema_invalid:name"
    if not isinstance(profile.get("courses"), list):
        return False, "schema_invalid:courses"
    if not isinstance(profile.get("location"), dict):
        return False, "schema_invalid:location"
    if confidence(profile) < float(os.getenv("INSTITUTION_MIN_CONFIDENCE", "0.45")):
        return False, "schema_invalid:low_confidence"
    return True, ""


def crawl_institution_sync(url: str, entity_type: str) -> CrawlResult:
    return asyncio.run(InstitutionCrawler().crawl(url, entity_type))


def read_bulk_csv(path: str) -> list[dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    out = []
    for row in rows:
        url = row.get("url") or row.get("official_url") or row.get("website")
        entity_type = row.get("type") or row.get("entity_type") or "college"
        if url:
            out.append({"url": url, "entity_type": entity_type, "name": row.get("name", "")})
    return out
