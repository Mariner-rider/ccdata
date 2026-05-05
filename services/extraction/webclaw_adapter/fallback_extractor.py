from __future__ import annotations

import re

import httpx
from bs4 import BeautifulSoup


def extract_fallback(url: str, timeout: float = 20.0) -> dict:
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        r = client.get(url)
        r.raise_for_status()
        html = r.text

    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    title_tag = soup.find("title")
    meta_desc = soup.find("meta", attrs={"name": "description"})
    name = (h1.get_text(" ", strip=True) if h1 else (title_tag.get_text(" ", strip=True) if title_tag else "")).strip()

    text = soup.get_text(" ", strip=True)
    fees = re.findall(r"\$\s?\d[\d,]*", text)
    links = [a.get("href") for a in soup.find_all("a", href=True)]
    admission = [l for l in links if "admission" in l.lower() or "apply" in l.lower()]

    return {
        "name": name,
        "location": "",
        "official_website": url,
        "courses": [],
        "fees": fees[:10],
        "admission_link": admission[:5],
        "placement": [],
        "faculty": [],
        "hostel": [],
        "gallery": [],
        "meta": {
            "method": "fallback_http_bs4",
            "meta_description": meta_desc.get("content", "") if meta_desc else "",
            "links": links[:100],
            "is_empty": len(text) == 0,
        },
    }
