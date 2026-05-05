from __future__ import annotations

import os

import psycopg
import redis
from fastapi import FastAPI

from services.extraction.webclaw_adapter.webclaw_adapter import WebClawAdapter, WebClawError

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://crawler:crawler@postgres:5432/crawler")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

app = FastAPI(title="collegecue-local-lite")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/health/db")
def health_db() -> dict:
    try:
        with psycopg.connect(POSTGRES_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {"status": "ok"}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "error": str(exc)}


@app.get("/health/redis")
def health_redis() -> dict:
    try:
        r = redis.from_url(REDIS_URL)
        r.ping()
        return {"status": "ok"}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "error": str(exc)}


@app.get("/health/webclaw")
def health_webclaw() -> dict:
    a = WebClawAdapter()
    if not a.cfg.enabled:
        return {"status": "disabled", "enabled": False}
    try:
        a.scrape("https://example.com")
        return {"status": "ok", "enabled": True}
    except WebClawError as exc:
        return {"status": "degraded", "enabled": False, "detail": str(exc)}
