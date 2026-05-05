from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from urllib import robotparser

import psycopg
import redis
from rq import Queue

from services.extraction.webclaw_adapter.fallback_extractor import extract_fallback
from services.extraction.webclaw_adapter.webclaw_adapter import WebClawAdapter, WebClawError, normalize_webclaw_output

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://crawler:crawler@postgres:5432/crawler")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
RAW_RETENTION_DAYS = int(os.getenv("RAW_HTML_RETENTION_DAYS", "7"))
LOCAL_RAW_LIMIT_BYTES = int(os.getenv("LOCAL_RAW_LIMIT_BYTES", str(2 * 1024 * 1024 * 1024)))
ARTIFACT_DIR = os.getenv("ARTIFACT_DIR", "./artifacts")


def _robots_allowed(url: str, user_agent: str = "ccdata-lite-bot") -> bool:
    from urllib.parse import urlparse

    p = urlparse(url)
    rp = robotparser.RobotFileParser()
    rp.set_url(f"{p.scheme}://{p.netloc}/robots.txt")
    try:
        rp.read()
    except Exception:  # noqa: BLE001
        return True
    return rp.can_fetch(user_agent, url)


def _store_record(url: str, data: dict) -> None:
    content_hash = hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()
    sqlite_path = os.getenv("TEST_SQLITE_PATH", "")
    if sqlite_path:
        import sqlite3

        con = sqlite3.connect(sqlite_path)
        cur = con.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS normalized_records(source_url TEXT, category TEXT, record_hash TEXT UNIQUE, payload TEXT, mapped_at TEXT)")
        cur.execute(
            "INSERT OR IGNORE INTO normalized_records(source_url, category, record_hash, payload, mapped_at) VALUES (?, ?, ?, ?, datetime('now'))",
            (url, "colleges", content_hash, json.dumps(data)),
        )
        con.commit()
        con.close()
        return

    with psycopg.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO normalized_records(source_url, category, record_hash, payload, mapped_at)
                VALUES (%s, %s, %s, %s::jsonb, NOW())
                ON CONFLICT (record_hash) DO NOTHING;
                """,
                (url, "colleges", content_hash, json.dumps(data)),
            )
        conn.commit()


def crawl_single(url: str) -> dict:
    if not _robots_allowed(url):
        raise RuntimeError("Blocked by robots.txt")

    extraction_method = "webclaw"
    try:
        raw = WebClawAdapter().extract(url, schema={"type": "college"})
        normalized = normalize_webclaw_output(raw)
    except WebClawError:
        normalized = extract_fallback(url)
        extraction_method = "fallback"

    normalized["source_url"] = url
    normalized["last_crawled_at"] = datetime.now(timezone.utc).isoformat()
    normalized["content_hash"] = hashlib.sha256(json.dumps(normalized, sort_keys=True).encode()).hexdigest()
    normalized["confidence_score"] = 0.7 if extraction_method == "webclaw" else 0.45
    normalized["extraction_method"] = extraction_method
    normalized["freshness_status"] = "fresh"
    _store_record(url, normalized)
    return normalized


def crawl_missing_fields() -> int:
    required = ["name", "location", "official_website", "courses", "fees", "admission_link", "placement", "faculty", "hostel"]
    count = 0
    with psycopg.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT source_url, payload FROM normalized_records ORDER BY mapped_at DESC LIMIT 2000")
            rows = cur.fetchall()
    q = Queue("crawl", connection=redis.from_url(REDIS_URL))
    for source_url, payload in rows:
        missing = [k for k in required if not (payload.get(k) or payload.get("data", {}).get(k))]
        if missing and source_url:
            q.enqueue(crawl_single, source_url)
            count += 1
    return count


def storage_cleanup() -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(days=RAW_RETENTION_DAYS)
    deleted = 0
    with psycopg.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM crawl_logs WHERE status='raw_html_debug' AND event_ts < %s", (cutoff,))
            deleted = cur.rowcount
        conn.commit()
    return deleted


def storage_status() -> dict:
    from pathlib import Path

    root = Path(ARTIFACT_DIR)
    root.mkdir(parents=True, exist_ok=True)
    total = 0
    files = []
    for f in root.rglob("*"):
        if f.is_file():
            sz = f.stat().st_size
            total += sz
            files.append((f, sz, f.stat().st_mtime))
    if total > LOCAL_RAW_LIMIT_BYTES:
        files.sort(key=lambda x: x[2])
        idx = 0
        while total > LOCAL_RAW_LIMIT_BYTES and idx < len(files):
            fp, sz, _ = files[idx]
            fp.unlink(missing_ok=True)
            total -= sz
            idx += 1
    return {"artifact_dir": str(root), "total_bytes": total, "limit_bytes": LOCAL_RAW_LIMIT_BYTES}


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    s1 = sub.add_parser("crawl:single")
    s1.add_argument("--url", required=True)
    s2 = sub.add_parser("crawl:college")
    s2.add_argument("--college-id", required=True)
    sub.add_parser("crawl:missing-fields")
    sub.add_parser("crawl:monthly-refresh")
    s3 = sub.add_parser("extract:test")
    s3.add_argument("--url", required=True)
    sub.add_parser("storage:cleanup")
    sub.add_parser("storage:status")
    sub.add_parser("docker:size-report")
    args = parser.parse_args()

    if args.cmd in {"crawl:single", "extract:test"}:
        print(json.dumps(crawl_single(args.url), indent=2))
    elif args.cmd == "crawl:college":
        with psycopg.connect(POSTGRES_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT official_website FROM colleges WHERE id=%s", (int(args.college_id),))
                row = cur.fetchone()
        if not row:
            raise RuntimeError("College not found")
        print(json.dumps(crawl_single(row[0]), indent=2))
    elif args.cmd == "crawl:missing-fields":
        print(f"queued={crawl_missing_fields()}")
    elif args.cmd == "crawl:monthly-refresh":
        with psycopg.connect(POSTGRES_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT official_website FROM colleges WHERE official_website IS NOT NULL LIMIT 500")
                urls = [r[0] for r in cur.fetchall()]
        q = Queue("crawl", connection=redis.from_url(REDIS_URL))
        for u in urls:
            q.enqueue(crawl_single, u)
        print(f"queued={len(urls)}")
    elif args.cmd == "storage:cleanup":
        print(f"deleted={storage_cleanup()}")
    elif args.cmd == "storage:status":
        print(json.dumps(storage_status(), indent=2))
    elif args.cmd == "docker:size-report":
        os.system("python scripts/docker_size_report.py || true")


if __name__ == "__main__":
    main()
