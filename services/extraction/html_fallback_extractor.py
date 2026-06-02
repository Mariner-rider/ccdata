from __future__ import annotations
import json, re
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlparse

from services.common.user_agents import get_headers

try:
    import httpx
except Exception:
    httpx = None

class _Parser(HTMLParser):
    def __init__(self):
        super().__init__(); self.title=""; self.h1=""; self.links=[]; self.text=[]; self.images=[]; self.meta={}; self._tag=None
    def handle_starttag(self, tag, attrs):
        self._tag=tag; d=dict(attrs)
        if tag=="a" and d.get("href"): self.links.append((d.get("href"), d.get("title", "")))
        if tag=="img" and d.get("src"): self.images.append(d.get("src"))
        if tag=="meta" and d.get("name") and d.get("content"): self.meta[d.get("name").lower()]=d.get("content")
    def handle_data(self, data):
        t=data.strip()
        if not t: return
        self.text.append(t)
        if self._tag=="title": self.title += (" "+t)
        if self._tag=="h1": self.h1 += (" "+t)

def _read_url_text(url: str, timeout: float) -> str:
    if url.startswith("file://"):
        u=urlparse(url); fp = Path((u.netloc + u.path) if u.netloc else u.path); return fp.read_text(encoding="utf-8")
    if httpx is None: raise RuntimeError("httpx is required for non-file URLs")
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        r = client.get(url, headers=get_headers(url)); r.raise_for_status(); return r.text

def _pick_line(lines, keys):
    for l in lines:
        low=l.lower()
        if any(k in low for k in keys): return l
    return ""

def extract_fallback(url: str, timeout: float = 20.0) -> dict:
    html = _read_url_text(url, timeout)
    p = _Parser(); p.feed(html)
    lines = p.text
    text = " ".join(lines)
    name=(p.h1.strip() or p.title.strip())
    location=_pick_line(lines,["location","address","campus","city","state"])
    courses=[l for l in lines if any(k in l.lower() for k in ["b.tech","mba","course","program","programme","academics","department"])]
    fees=list({*re.findall(r"\$\s?\d[\d,]*", text), *re.findall(r"\b(?:INR|Rs\.?|USD)\s?\d[\d,]*", text, re.I)})
    admission=[h for h,t in p.links if any(k in h.lower() for k in ["admission","apply","programme","program"]) or "admission" in t.lower()]
    placement=[l for l in lines if any(k in l.lower() for k in ["placement","career development","career"]) ]
    faculty=[l for l in lines if any(k in l.lower() for k in ["faculty","prof","dr.","people","department"]) ]
    hostel=[l for l in lines if any(k in l.lower() for k in ["hostel","campus life","residence"]) ]
    contact=[l for l in lines if "contact" in l.lower() or re.search(r"\+?\d[\d\-\s]{7,}",l)]
    sections={"info":bool(name or location),"courses_fees":bool(courses or fees),"admission":bool(admission),"faculty":bool(faculty),"hostel":bool(hostel),"placement":bool(placement),"gallery":bool(p.images),"reviews":False,"contact":bool(contact)}

    field_details={
      "name":{"value":name,"source_url":url,"extraction_method":"heading","confidence":0.99 if name else 0},
      "location":{"value":location.replace("Location:","").strip(),"source_url":url,"extraction_method":"label_heuristic","confidence":0.95 if location else 0},
      "courses":{"value":courses,"source_url":url,"extraction_method":"list_heuristic","confidence":0.93 if courses else 0},
      "fees":{"value":fees,"source_url":url,"extraction_method":"regex_currency","confidence":0.93 if fees else 0},
      "admission_link":{"value":admission,"source_url":url,"extraction_method":"link_heuristic","confidence":0.95 if admission else 0},
      "placement":{"value":placement,"source_url":url,"extraction_method":"keyword_line","confidence":0.92 if placement else 0},
      "faculty":{"value":faculty,"source_url":url,"extraction_method":"keyword_line","confidence":0.92 if faculty else 0},
      "hostel":{"value":hostel,"source_url":url,"extraction_method":"keyword_line","confidence":0.92 if hostel else 0},
      "gallery":{"value":p.images,"source_url":url,"extraction_method":"img_tags","confidence":0.85 if p.images else 0},
      "contact":{"value":contact,"source_url":url,"extraction_method":"keyword_regex","confidence":0.88 if contact else 0},
    }
    return {"name":field_details['name']['value'],"location":field_details['location']['value'],"official_website":url,"courses":courses,"fees":fees,"admission_link":admission,"placement":placement,"faculty":faculty,"hostel":hostel,"gallery":p.images,"contact":contact,"field_details":field_details,"sections":sections,"meta":{"method":"fallback_html_parser","meta_description":p.meta.get('description',''),"links":[h for h,_ in p.links],"is_empty":len(text)==0,"raw_text_preview":text[:300]}}
