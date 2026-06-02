"""Professional browser-like headers for crawler HTTP requests."""

from __future__ import annotations

import random
from urllib.parse import urlparse

USER_AGENT_POOL = [
    # Chrome on Windows 11: versions 120, 121, 122
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.225 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.185 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.129 Safari/537.36",
    # Chrome on macOS Sonoma: versions 120, 121, 122
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.234 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.184 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.129 Safari/537.36",
    # Chrome on Linux: versions 121, 122
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.184 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.128 Safari/537.36",
    # Firefox on Windows: versions 121, 122
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    # Firefox on macOS: versions 121, 122
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.2; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.3; rv:122.0) Gecko/20100101 Firefox/122.0",
    # Safari on macOS Ventura: versions 16.5, 16.6
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
    # Safari on iOS 17: versions 17.1, 17.2
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    # Edge on Windows 11: versions 121, 122
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.184 Safari/537.36 Edg/121.0.2277.128",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.129 Safari/537.36 Edg/122.0.2365.92",
    # Additional common desktop browser variants to keep the pool broad
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.112 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
]

ACCEPT_LANGUAGES = [
    "en-IN,en;q=0.9,hi;q=0.8",
    "en-US,en;q=0.9",
    "en-GB,en;q=0.9",
    "en-IN,en;q=0.9",
    "hi-IN,hi;q=0.9,en-IN;q=0.8,en;q=0.7",
]

FORBIDDEN_HEADER_WORDS = ("bot", "crawler", "scraper", "spider", "python", "requests", "httpx", "ccdata", "collegecue")


def get_random_ua() -> str:
    """Returns a random user agent from the pool."""
    return random.choice(USER_AGENT_POOL)


def _domain_root(url: str) -> str | None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}"


def _is_subpage(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path or "/"
    return path.rstrip("/") not in {"", "/"}


def _validate_header_values(headers: dict[str, str]) -> None:
    for value in headers.values():
        lowered = value.lower()
        if any(word in lowered for word in FORBIDDEN_HEADER_WORDS):
            raise ValueError("Header value contains a forbidden crawler identifier")


def get_headers(url: str) -> dict[str, str]:
    """
    Returns a complete, realistic browser header dict for the given URL.

    Subpage requests are treated as same-origin navigations and receive a
    Referer pointing at the domain root. Domain-root requests are treated as
    first page loads and intentionally omit Referer.
    """
    subpage = _is_subpage(url)
    headers = {
        "User-Agent": get_random_ua(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": random.choice(ACCEPT_LANGUAGES),
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin" if subpage else "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
        "DNT": "1",
    }
    root = _domain_root(url)
    if subpage and root:
        headers["Referer"] = root
    _validate_header_values(headers)
    return headers


def add_jitter(base_seconds: float) -> float:
    """
    Returns base_seconds + random.uniform(-0.3, 0.8), never below 0.5 seconds.
    """
    return max(0.5, base_seconds + random.uniform(-0.3, 0.8))


def get_playwright_headers(url: str) -> dict[str, str]:
    """
    Same as get_headers(), minus headers Playwright manages automatically.
    """
    headers = get_headers(url)
    for managed_header in ("Accept-Encoding", "Connection"):
        headers.pop(managed_header, None)
    return headers
