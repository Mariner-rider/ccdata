import hashlib
import json
from datetime import datetime, timezone
from typing import Any

import psycopg

from services.common.config import settings
from services.common.db import get_conn
from services.common.kafka_client import build_consumer, build_producer

UPSERT_COLLEGE_PAGE = """
INSERT INTO college_pages (
    college_id,
    slug,
    title,
    status,
    source_payload,
    content_hash,
    created_at,
    updated_at
)
VALUES (%s, %s, %s, %s, %s::jsonb, %s, NOW(), NOW())
ON CONFLICT (slug)
DO UPDATE SET
    title = EXCLUDED.title,
    status = EXCLUDED.status,
    source_payload = EXCLUDED.source_payload,
    content_hash = EXCLUDED.content_hash,
    updated_at = NOW()
RETURNING id;
"""

UPSERT_SUBSECTION = """
INSERT INTO college_page_sections (
    page_id,
    section_key,
    section_title,
    body,
    metadata,
    content_hash,
    updated_at
)
VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s, NOW())
ON CONFLICT (page_id, section_key)
DO UPDATE SET
    section_title = EXCLUDED.section_title,
    body = EXCLUDED.body,
    metadata = EXCLUDED.metadata,
    content_hash = EXCLUDED.content_hash,
    updated_at = NOW();
"""

INSERT_SYNC_EVENT = """
INSERT INTO college_page_sync_events (
    page_id,
    event_type,
    event_payload,
    sync_status,
    created_at
)
VALUES (%s, %s, %s::jsonb, %s, NOW())
RETURNING id;
"""

UPDATE_SYNC_STATUS = """
UPDATE college_page_sync_events
SET sync_status = %s,
    sync_error = %s,
    synced_at = CASE WHEN %s = 'synced' THEN NOW() ELSE synced_at END
WHERE id = %s;
"""

INSERT_LOG = """
INSERT INTO crawl_logs(source_id, url, status, detail, event_ts)
VALUES (%s, %s, %s, %s, NOW());
"""

SECTION_MAP = {
    "info": "info",
    "courses": "courses",
    "faculty": "faculty",
    "hostel": "hostel",
    "placement": "placement",
}


def _slugify(text: str) -> str:
    import re

    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    text = re.sub(r"\s+", "-", text)
    return re.sub(r"-+", "-", text).strip("-")


def _hash_payload(value: Any) -> str:
    canonical = json.dumps(value, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _build_sections(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    data = payload.get("data", {})
    ai = payload.get("ai_enrichment", {})

    return {
        "info": {
            "section_title": "College Info",
            "body": {
                "name": data.get("name"),
                "category": payload.get("category"),
                "summary": ai.get("summary"),
                "source_url": payload.get("source_url"),
            },
            "metadata": {
                "classification": ai.get("classification"),
            },
        },
        "courses": {
            "section_title": "Courses",
            "body": {
                "courses": data.get("courses", []),
                "admissions": data.get("admission_links", []),
            },
            "metadata": {},
        },
        "faculty": {
            "section_title": "Faculty",
            "body": {
                "faculty": data.get("faculty", []),
            },
            "metadata": {},
        },
        "hostel": {
            "section_title": "Hostel",
            "body": {
                "hostel": data.get("hostel", []),
                "facilities": data.get("hostel_facilities", []),
            },
            "metadata": {},
        },
        "placement": {
            "section_title": "Placement",
            "body": {
                "placement": data.get("placements", []),
                "jobs": data.get("jobs", []),
            },
            "metadata": {
                "sentiment": ai.get("reviews_sentiment"),
            },
        },
    }


def _upsert_page_and_sections(payload: dict[str, Any]) -> tuple[int, str]:
    college_name = payload.get("data", {}).get("name") or payload.get("source_url") or "unknown-college"
    slug = _slugify(str(college_name))
    page_title = f"{college_name} | College Page"
    status = "published"
    content_hash = _hash_payload(payload)

    with get_conn() as (conn, cur):
        cur.execute(
            UPSERT_COLLEGE_PAGE,
            (
                payload.get("data", {}).get("college_id"),
                slug,
                page_title,
                status,
                json.dumps(payload),
                content_hash,
            ),
        )
        page_id = cur.fetchone()[0]

        sections = _build_sections(payload)
        for section_key, section_data in sections.items():
            section_hash = _hash_payload(section_data)
            cur.execute(
                UPSERT_SUBSECTION,
                (
                    page_id,
                    section_key,
                    section_data["section_title"],
                    json.dumps(section_data["body"]),
                    json.dumps(section_data["metadata"]),
                    section_hash,
                ),
            )

        cur.execute(INSERT_LOG, (None, payload.get("source_url", ""), "college_page_upsert", f"page_id={page_id} slug={slug}"))
        conn.commit()

    return page_id, slug


def _sync_chatbot_db(sync_event_id: int, page_id: int, slug: str, payload: dict[str, Any]) -> None:
    chatbot_dsn = settings.chatbot_postgres_dsn
    with psycopg.connect(chatbot_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO chatbot_college_pages(page_slug, title, payload, updated_at)
                VALUES (%s, %s, %s::jsonb, NOW())
                ON CONFLICT (page_slug)
                DO UPDATE SET title = EXCLUDED.title, payload = EXCLUDED.payload, updated_at = NOW();
                """,
                (slug, payload.get("data", {}).get("name") or slug, json.dumps(payload)),
            )
        conn.commit()

    with get_conn() as (conn, cur):
        cur.execute(UPDATE_SYNC_STATUS, ("synced", None, "synced", sync_event_id))
        cur.execute(INSERT_LOG, (None, payload.get("source_url", ""), "chatbot_sync_success", f"page_id={page_id} slug={slug}"))
        conn.commit()


def _process_event(payload: dict[str, Any]) -> dict[str, Any]:
    page_id, slug = _upsert_page_and_sections(payload)

    with get_conn() as (conn, cur):
        cur.execute(
            INSERT_SYNC_EVENT,
            (
                page_id,
                "college_page_upserted",
                json.dumps({"slug": slug, "source_url": payload.get("source_url"), "event_at": datetime.now(timezone.utc).isoformat()}),
                "pending",
            ),
        )
        sync_event_id = cur.fetchone()[0]
        conn.commit()

    try:
        _sync_chatbot_db(sync_event_id, page_id, slug, payload)
    except Exception as exc:  # noqa: BLE001
        with get_conn() as (conn, cur):
            cur.execute(UPDATE_SYNC_STATUS, ("failed", str(exc)[:2000], "failed", sync_event_id))
            cur.execute(INSERT_LOG, (None, payload.get("source_url", ""), "chatbot_sync_failed", str(exc)[:2000]))
            conn.commit()

    return {
        "page_id": page_id,
        "slug": slug,
        "source_url": payload.get("source_url"),
        "status": "updated",
        "event_type": "college_page_changed",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    consumer = build_consumer("college-page-service", settings.enriched_results_topic)
    producer = build_producer()

    for msg in consumer:
        payload = msg.value
        # only process college entities for page generation
        if (payload.get("category") or "").lower() not in {"colleges", "college", "school"}:
            consumer.commit()
            continue
        try:
            out = _process_event(payload)
            producer.send(settings.college_page_events_topic, out)
            consumer.commit()
        except Exception as exc:  # noqa: BLE001
            with get_conn() as (conn, cur):
                cur.execute(INSERT_LOG, (None, payload.get("source_url", ""), "college_page_error", str(exc)[:2000]))
                conn.commit()
            consumer.commit()


if __name__ == "__main__":
    main()
