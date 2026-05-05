import asyncio
import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

import boto3
from botocore.client import Config
from bs4 import BeautifulSoup
from lxml import etree
from playwright.async_api import async_playwright
from w3lib.url import canonicalize_url

from services.common.config import settings
from services.common.db import get_conn
from services.common.kafka_client import build_consumer, build_producer

SELECT_LATEST_PAGE = """
SELECT s3_key
FROM page_state
WHERE url_hash = %s
ORDER BY last_crawled_at DESC
LIMIT 1;
"""

UPSERT_PARSED = """
INSERT INTO parsed_college_data (
    url,
    url_hash,
    payload,
    extracted_at
)
VALUES (%s, %s, %s::jsonb, NOW())
ON CONFLICT (url_hash)
DO UPDATE SET payload = EXCLUDED.payload, extracted_at = NOW();
"""

INSERT_LOG = """
INSERT INTO crawl_logs(source_id, url, status, detail, event_ts)
VALUES (%s, %s, %s, %s, NOW());
"""


@dataclass
class FieldResult:
    value: Any
    confidence: float
    source: str


class CollegeParser:
    CSS_SELECTORS = {
        "college_name": ["h1", ".college-name", "meta[property='og:site_name']"],
        "courses": [".courses li", "#courses li", "section[id*='course'] li"],
        "fees": [".fees li", "#fees li", "table.fees tr", "section[id*='fee'] p"],
        "faculty": [".faculty li", "#faculty li", "section[id*='faculty'] li"],
        "placements": [".placement li", "#placement li", "section[id*='placement'] p"],
        "admission_links": ["a[href*='admission']", "a[href*='apply']", "a[href*='enroll']"],
    }

    XPATH_SELECTORS = {
        "college_name": ["//h1/text()", "//meta[@property='og:site_name']/@content"],
        "courses": ["//*[contains(translate(@id,'COURSE','course'),'course')]//li/text()"],
        "fees": ["//*[contains(translate(@id,'FEES','fees'),'fee')]//text()"],
        "faculty": ["//*[contains(translate(@id,'FACULTY','faculty'),'faculty')]//li/text()"],
        "placements": ["//*[contains(translate(@id,'PLACEMENTS','placements'),'placement')]//text()"],
        "admission_links": ["//a[contains(@href,'admission') or contains(@href,'apply')]/@href"],
    }

    def __init__(self, url: str, html: str):
        self.url = url
        self.html = html
        self.soup = BeautifulSoup(html, "html.parser")
        self.tree = etree.HTML(html)

    @staticmethod
    def _clean_text(value: str) -> str:
        return re.sub(r"\s+", " ", value).strip()

    def _extract_css(self, field: str) -> list[str]:
        values: list[str] = []
        for selector in self.CSS_SELECTORS[field]:
            for item in self.soup.select(selector):
                if field == "admission_links":
                    raw = item.get("href", "")
                elif item.name == "meta":
                    raw = item.get("content", "")
                else:
                    raw = item.get_text(" ", strip=True)
                cleaned = self._clean_text(raw)
                if cleaned:
                    values.append(cleaned)
            if values:
                break
        return list(dict.fromkeys(values))

    def _extract_xpath(self, field: str) -> list[str]:
        if self.tree is None:
            return []
        values: list[str] = []
        for xpath in self.XPATH_SELECTORS[field]:
            out = self.tree.xpath(xpath)
            for val in out:
                cleaned = self._clean_text(str(val))
                if cleaned:
                    values.append(cleaned)
            if values:
                break
        return list(dict.fromkeys(values))

    def _ai_fallback_extract(self, field: str) -> FieldResult:
        text = self._clean_text(self.soup.get_text(" ", strip=True))
        patterns = {
            "fees": r"(?:tuition|fee|fees)[^\d]{0,20}(\$?\d[\d,]+)",
            "placements": r"(?:placement|package|salary)[^.]{0,60}",
            "faculty": r"(?:faculty|professor|department)[^.]{0,80}",
            "courses": r"(?:courses?|programs? offered)[^.]{0,120}",
            "college_name": r"^([A-Z][A-Za-z&\-\s]{4,80})",
            "admission_links": r"https?://[^\s\"']*(?:admission|apply)[^\s\"']*",
        }
        matches = re.findall(patterns[field], text, flags=re.IGNORECASE)
        if not matches:
            return FieldResult(value=[] if field != "college_name" else "", confidence=0.2, source="ai_fallback")
        if field == "college_name":
            return FieldResult(value=self._clean_text(matches[0]), confidence=0.45, source="ai_fallback")
        return FieldResult(value=list(dict.fromkeys(matches[:20])), confidence=0.45, source="ai_fallback")

    def extract_field(self, field: str) -> FieldResult:
        css_values = self._extract_css(field)
        xpath_values = self._extract_xpath(field)

        merged = list(dict.fromkeys(css_values + xpath_values))
        if field == "college_name":
            value = merged[0] if merged else ""
            if value:
                return FieldResult(value=value, confidence=0.92 if css_values and xpath_values else 0.8, source="css_xpath")
        else:
            if merged:
                confidence = 0.9 if css_values and xpath_values else 0.75
                return FieldResult(value=merged, confidence=confidence, source="css_xpath")

        return self._ai_fallback_extract(field)

    def extract(self) -> dict[str, Any]:
        payload = {"url": self.url}
        for field in ["college_name", "courses", "fees", "faculty", "placements", "admission_links"]:
            result = self.extract_field(field)
            payload[field] = {
                "value": result.value,
                "confidence": round(result.confidence, 2),
                "source": result.source,
            }
        payload["overall_confidence"] = round(
            sum(payload[field]["confidence"] for field in ["college_name", "courses", "fees", "faculty", "placements", "admission_links"])
            / 6,
            2,
        )
        return payload


async def render_with_playwright(url: str) -> str:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, wait_until="networkidle", timeout=45000)
        content = await page.content()
        await browser.close()
        return content


def parse_url(url: str, html: str | None = None) -> dict[str, Any]:
    if not html:
        html = asyncio.run(render_with_playwright(url))
    parser = CollegeParser(url, html)
    payload = parser.extract()

    # If static parsing quality is weak, re-render and re-parse using Playwright.
    if payload["overall_confidence"] < 0.6:
        rendered = asyncio.run(render_with_playwright(url))
        payload = CollegeParser(url, rendered).extract()
        payload["render_mode"] = "playwright_dynamic"
    else:
        payload["render_mode"] = "raw_html"

    return payload


def load_html_from_s3(s3_client, s3_key: str) -> str:
    obj = s3_client.get_object(Bucket=settings.s3_bucket, Key=s3_key)
    return obj["Body"].read().decode("utf-8", errors="ignore")


def main() -> None:
    s3_client = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        config=Config(signature_version="s3v4"),
    )
    consumer = build_consumer("parser-engine", settings.crawl_results_topic)
    producer = build_producer()

    for msg in consumer:
        event = msg.value
        url = event.get("url")
        if not url or event.get("status") != "done":
            consumer.commit()
            continue

        canonical = canonicalize_url(url)
        url_hash = hashlib.sha256(canonical.encode()).hexdigest()

        with get_conn() as (conn, cur):
            cur.execute(SELECT_LATEST_PAGE, (url_hash,))
            row = cur.fetchone()
            if not row:
                cur.execute(INSERT_LOG, (None, canonical, "parse_skip", "no page_state/s3_key found"))
                conn.commit()
                consumer.commit()
                continue
            s3_key = row[0]

        try:
            html = load_html_from_s3(s3_client, s3_key)
            payload = parse_url(canonical, html)
            with get_conn() as (conn, cur):
                cur.execute(UPSERT_PARSED, (canonical, url_hash, json.dumps(payload)))
                cur.execute(INSERT_LOG, (None, canonical, "parsed", f"overall_confidence={payload['overall_confidence']}"))
                conn.commit()
            producer.send(settings.parse_results_topic, payload)
            print(json.dumps(payload))
            consumer.commit()
        except Exception as exc:  # noqa: BLE001
            with get_conn() as (conn, cur):
                cur.execute(INSERT_LOG, (None, canonical, "parse_error", str(exc)[:2000]))
                conn.commit()
            consumer.commit()


if __name__ == "__main__":
    main()
