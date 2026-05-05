from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_fixed


@dataclass
class WebClawConfig:
    base_url: str = os.getenv("WEBCLAW_BASE_URL", "")
    mcp_endpoint: str = os.getenv("WEBCLAW_MCP_ENDPOINT", "")
    timeout_seconds: float = float(os.getenv("WEBCLAW_TIMEOUT_SECONDS", "20"))
    max_retries: int = int(os.getenv("WEBCLAW_MAX_RETRIES", "2"))
    enabled: bool = os.getenv("WEBCLAW_ENABLED", "true").lower() == "true"


class WebClawError(RuntimeError):
    pass


class WebClawAdapter:
    def __init__(self, cfg: WebClawConfig | None = None):
        self.cfg = cfg or WebClawConfig()

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(0.4), reraise=True)
    def _request(self, url: str, payload: dict[str, Any]) -> dict[str, Any]:
        with httpx.Client(timeout=self.cfg.timeout_seconds) as client:
            r = client.post(url, json=payload)
            r.raise_for_status()
            body = r.json()
            if not isinstance(body, dict):
                raise WebClawError("Malformed response: expected object")
            return body

    def _call(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.cfg.enabled or not self.cfg.base_url:
            raise WebClawError("WebClaw disabled or WEBCLAW_BASE_URL not configured")
        url = f"{self.cfg.base_url.rstrip('/')}/{path.lstrip('/')}"
        try:
            return self._request(url, payload)
        except Exception as exc:  # noqa: BLE001
            raise WebClawError(f"WebClaw request failed: {exc}") from exc

    def scrape(self, url: str) -> dict[str, Any]:
        return self._call("scrape", {"url": url})

    def crawl_map(self, url: str, max_depth: int = 1) -> dict[str, Any]:
        return self._call("crawl", {"url": url, "max_depth": max_depth})

    def extract(self, url: str, schema: dict[str, Any]) -> dict[str, Any]:
        return self._call("extract", {"url": url, "schema": schema})

    def summarize(self, text: str) -> dict[str, Any]:
        return self._call("summarize", {"text": text})

    def diff(self, old_text: str, new_text: str) -> dict[str, Any]:
        return self._call("diff", {"old": old_text, "new": new_text})


def normalize_webclaw_output(raw: dict[str, Any]) -> dict[str, Any]:
    data = raw.get("data", raw)
    return {
        "name": data.get("name") or data.get("college_name"),
        "location": data.get("location") or data.get("city"),
        "official_website": data.get("official_website") or data.get("url"),
        "courses": data.get("courses", []),
        "fees": data.get("fees", []),
        "admission_link": data.get("admission_link") or data.get("admission_links", []),
        "placement": data.get("placement") or data.get("placements", []),
        "faculty": data.get("faculty", []),
        "hostel": data.get("hostel", []),
        "gallery": data.get("gallery") or data.get("images", []),
        "meta": {"raw": json.dumps(raw)[:4000]},
    }
