from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse
from urllib import robotparser

from services.extraction.webclaw_adapter.fallback_extractor import extract_fallback
from services.extraction.webclaw_adapter.webclaw_adapter import WebClawAdapter, WebClawError, normalize_webclaw_output

RAW_RETENTION_DAYS = int(os.getenv("RAW_HTML_RETENTION_DAYS", "7"))
LOCAL_RAW_LIMIT_BYTES = int(os.getenv("LOCAL_RAW_LIMIT_BYTES", str(2 * 1024 * 1024 * 1024)))
ARTIFACT_DIR = os.getenv("ARTIFACT_DIR", "./artifacts")


@dataclass
class RuntimeConfig:
    runtime_profile: str
    database_url: str
    queue_backend: str
    webclaw_enabled: bool


class SQLiteRepository:
    def __init__(self, database_url: str):
        self.path = database_url.replace("sqlite:///", "")

    def init_db(self) -> None:
        with sqlite3.connect(self.path) as con:
            con.execute(
                """CREATE TABLE IF NOT EXISTS crawler_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_url TEXT NOT NULL,
                normalized_data TEXT NOT NULL,
                content_hash TEXT UNIQUE,
                last_crawled_at TEXT,
                confidence_score REAL,
                extraction_method TEXT,
                freshness_status TEXT
            )"""
            )
            con.commit()

    def insert_record(self, record: dict) -> None:
        with sqlite3.connect(self.path) as con:
            con.execute(
                """INSERT OR REPLACE INTO crawler_records
                (source_url, normalized_data, content_hash, last_crawled_at, confidence_score, extraction_method, freshness_status)
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    record["source_url"],
                    json.dumps(record),
                    record["content_hash"],
                    record["last_crawled_at"],
                    record["confidence_score"],
                    record["extraction_method"],
                    record["freshness_status"],
                ),
            )
            con.commit()

    def list_records(self) -> list[tuple[str, dict]]:
        with sqlite3.connect(self.path) as con:
            rows = con.execute("SELECT source_url, normalized_data FROM crawler_records").fetchall()
        return [(u, json.loads(p)) for u, p in rows]


class MemoryQueue:
    def __init__(self):
        self.items: list[str] = []

    def enqueue(self, url: str) -> None:
        self.items.append(url)


def _robots_allowed(url: str, user_agent: str = "ccdata-lite-bot") -> bool:
    if url.startswith("file://"):
        return True
    p = urlparse(url)
    rp = robotparser.RobotFileParser()
    rp.set_url(f"{p.scheme}://{p.netloc}/robots.txt")
    try:
        rp.read()
    except Exception:
        return True
    return rp.can_fetch(user_agent, url)


def _load_config() -> RuntimeConfig:
    return RuntimeConfig(
        runtime_profile=os.getenv("RUNTIME_PROFILE", "no-docker"),
        database_url=os.getenv("DATABASE_URL", "sqlite:///./collegecue_local.db"),
        queue_backend=os.getenv("QUEUE_BACKEND", "memory"),
        webclaw_enabled=os.getenv("WEBCLAW_ENABLED", "false").lower() == "true",
    )


def crawl_single(url: str) -> dict:
    if not _robots_allowed(url):
        raise RuntimeError("Blocked by robots.txt")
    cfg = _load_config()
    extraction_method = "fallback"
    if cfg.webclaw_enabled:
        try:
            raw = WebClawAdapter().extract(url, schema={"type": "college"})
            normalized = normalize_webclaw_output(raw)
            extraction_method = "webclaw"
        except WebClawError:
            normalized = extract_fallback(url)
    else:
        normalized = extract_fallback(url)

    normalized["source_url"] = url
    normalized["last_crawled_at"] = datetime.now(timezone.utc).isoformat()
    normalized["content_hash"] = hashlib.sha256(json.dumps(normalized, sort_keys=True).encode()).hexdigest()
    normalized["confidence_score"] = 0.7 if extraction_method == "webclaw" else 0.45
    normalized["extraction_method"] = extraction_method
    normalized["freshness_status"] = "fresh"

    repo = SQLiteRepository(cfg.database_url)
    repo.init_db()
    repo.insert_record(normalized)
    return normalized


def crawl_missing_fields() -> int:
    required = ["name", "location", "official_website", "courses", "fees", "admission_link", "placement", "faculty", "hostel"]
    cfg = _load_config()
    repo = SQLiteRepository(cfg.database_url)
    repo.init_db()
    queue = MemoryQueue()
    for source_url, payload in repo.list_records():
        missing = [k for k in required if not payload.get(k)]
        if missing:
            queue.enqueue(source_url)
    return len(queue.items)


def storage_cleanup() -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(days=RAW_RETENTION_DAYS)
    deleted = 0
    root = Path(ARTIFACT_DIR)
    if not root.exists():
        return 0
    for f in root.rglob("*"):
        if f.is_file() and datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc) < cutoff:
            f.unlink(missing_ok=True)
            deleted += 1
    return deleted


def storage_status() -> dict:
    root = Path(ARTIFACT_DIR)
    root.mkdir(parents=True, exist_ok=True)
    total = sum(f.stat().st_size for f in root.rglob("*") if f.is_file())
    return {"artifact_dir": str(root), "total_bytes": total, "limit_bytes": LOCAL_RAW_LIMIT_BYTES}


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    s1 = sub.add_parser("crawl:single"); s1.add_argument("--url", required=True)
    sub.add_parser("crawl:missing-fields")
    s3 = sub.add_parser("extract:test"); s3.add_argument("--url", required=True)
    sub.add_parser("storage:cleanup"); sub.add_parser("storage:status")
    sub.add_parser("init-db")
    args = parser.parse_args()

    cfg = _load_config(); repo = SQLiteRepository(cfg.database_url)
    if args.cmd == "init-db":
        repo.init_db(); print("initialized")
    elif args.cmd in {"crawl:single", "extract:test"}:
        print(json.dumps(crawl_single(args.url), indent=2))
    elif args.cmd == "crawl:missing-fields":
        print(f"queued={crawl_missing_fields()}")
    elif args.cmd == "storage:cleanup":
        print(f"deleted={storage_cleanup()}")
    elif args.cmd == "storage:status":
        print(json.dumps(storage_status(), indent=2))


if __name__ == "__main__":
    main()
