from __future__ import annotations

import re
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlparse

try:
    import httpx
except Exception:
    httpx = None


class _Parser(HTMLParser):
    def __init__(self):
        super().__init__(); self.title=""; self.h1=""; self.links=[]; self.text=[]; self._in_title=False; self._in_h1=False
    def handle_starttag(self, tag, attrs):
        if tag=="title": self._in_title=True
        if tag=="h1": self._in_h1=True
        if tag=="a":
            d=dict(attrs); href=d.get("href");
            if href: self.links.append(href)
    def handle_endtag(self, tag):
        if tag=="title": self._in_title=False
        if tag=="h1": self._in_h1=False
    def handle_data(self, data):
        t=data.strip()
        if not t: return
        self.text.append(t)
        if self._in_title: self.title += (" " + t)
        if self._in_h1: self.h1 += (" " + t)


def _read_url_text(url: str, timeout: float) -> str:
    if url.startswith("file://"):
        u=urlparse(url)
        fp = Path((u.netloc + u.path) if u.netloc else u.path)
        return fp.read_text(encoding="utf-8")
    if httpx is None:
        raise RuntimeError("httpx is required for non-file URLs")
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        r = client.get(url); r.raise_for_status(); return r.text


def extract_fallback(url: str, timeout: float = 20.0) -> dict:
    html = _read_url_text(url, timeout)
    p = _Parser(); p.feed(html)
    text = " ".join(p.text)
    fees = re.findall(r"\$\s?\d[\d,]*", text)
    admission = [l for l in p.links if "admission" in l.lower() or "apply" in l.lower()]
    return {"name": (p.h1.strip() or p.title.strip()), "location": "", "official_website": url, "courses": [], "fees": fees[:10], "admission_link": admission[:5], "placement": [], "faculty": [], "hostel": [], "gallery": [], "meta": {"method": "fallback_html_parser", "links": p.links[:100], "is_empty": len(text)==0}}
