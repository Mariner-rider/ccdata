from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from services.extraction.webclaw_adapter.fallback_extractor import extract_fallback
from services.lite_pipeline.main import MemoryQueue, SQLiteRepository, crawl_missing_fields, crawl_single


def _env(monkeypatch, db):
    monkeypatch.setenv("RUNTIME_PROFILE", "no-docker")
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db}")
    monkeypatch.setenv("QUEUE_BACKEND", "memory")
    monkeypatch.setenv("WEBCLAW_ENABLED", "false")


def test_sqlite_repository_write_read(tmp_path):
    db = tmp_path / "t.db"
    repo = SQLiteRepository(f"sqlite:///{db}")
    repo.init_db()
    rec = {"source_url":"file://x","content_hash":"h1","last_crawled_at":"now","confidence_score":0.5,"extraction_method":"fallback","freshness_status":"fresh"}
    repo.insert_record(rec)
    rows = repo.list_records()
    assert rows and rows[0][0] == "file://x"


def test_fixture_extraction_file_scheme():
    out = extract_fallback("file://tests/fixtures/college_sample.html")
    assert out["name"]


def test_file_crawl_and_record(monkeypatch, tmp_path):
    db = tmp_path / "crawl.db"
    _env(monkeypatch, db)
    out = crawl_single("file://tests/fixtures/college_sample.html")
    assert out["source_url"].startswith("file://")
    con = sqlite3.connect(db)
    count = con.execute("select count(*) from crawler_records").fetchone()[0]
    assert count >= 1


def test_memory_queue_and_missing_fields(monkeypatch, tmp_path):
    db = tmp_path / "q.db"
    _env(monkeypatch, db)
    crawl_single("file://tests/fixtures/college_sample.html")
    queued = crawl_missing_fields()
    assert queued >= 0
    q = MemoryQueue(); q.enqueue("u")
    assert q.items == ["u"]
