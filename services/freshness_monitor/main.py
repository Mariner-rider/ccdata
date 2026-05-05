import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from services.common.config import settings
from services.common.db import get_conn
from services.common.kafka_client import build_consumer, build_producer

SELECT_STALE = """
SELECT source_url, category, payload, enriched_at
FROM enriched_records
WHERE enriched_at < NOW() - (%s || ' days')::interval
ORDER BY enriched_at ASC
LIMIT 500;
"""

INSERT_TRIGGER = """
INSERT INTO crawl_trigger_events (
    source_url,
    trigger_type,
    trigger_reason,
    metadata,
    triggered_at
)
VALUES (%s, %s, %s, %s::jsonb, NOW());
"""

INSERT_LOG = """
INSERT INTO crawl_logs(source_id, url, status, detail, event_ts)
VALUES (%s, %s, %s, %s, NOW());
"""


@dataclass(frozen=True)
class TriggerEvent:
    url: str
    trigger_type: str
    reason: str
    metadata: dict[str, Any]


REQUIRED_FIELDS_BY_CATEGORY = {
    "colleges": ["name", "fees", "admission_links", "courses"],
    "school": ["name", "fees", "admission_links", "courses"],
    "jobs": ["title", "company", "apply_link", "description"],
    "scholarships": ["name", "amount", "deadline", "apply_link"],
    "news": ["headline", "summary", "published_at", "url"],
}


def _is_missing(value: Any) -> bool:
    return value in (None, "", [])


def _publish_trigger(producer, event: TriggerEvent) -> None:
    crawl_msg = {
        "source_id": None,
        "url": event.url,
        "priority": "high" if event.trigger_type == "realtime_request" else "normal",
        "trigger_type": event.trigger_type,
        "trigger_reason": event.reason,
        "triggered_at": datetime.now(timezone.utc).isoformat(),
        "max_depth": settings.max_depth,
    }
    producer.send(settings.crawl_queue_topic, crawl_msg)

    with get_conn() as (conn, cur):
        cur.execute(
            INSERT_TRIGGER,
            (
                event.url,
                event.trigger_type,
                event.reason,
                json.dumps(event.metadata),
            ),
        )
        cur.execute(INSERT_LOG, (None, event.url, "crawl_triggered", f"{event.trigger_type}: {event.reason}"))
        conn.commit()


def _find_missing_field_events() -> list[TriggerEvent]:
    events: list[TriggerEvent] = []
    with get_conn() as (conn, cur):
        cur.execute("SELECT source_url, category, payload FROM enriched_records ORDER BY enriched_at DESC LIMIT 1000;")
        rows = cur.fetchall()
        conn.commit()

    for source_url, category, payload in rows:
        data = (payload or {}).get("data", {})
        required = REQUIRED_FIELDS_BY_CATEGORY.get(category, [])
        missing = [field for field in required if _is_missing(data.get(field))]
        if missing and source_url:
            events.append(
                TriggerEvent(
                    url=source_url,
                    trigger_type="targeted_missing_fields",
                    reason=f"missing required fields: {', '.join(missing)}",
                    metadata={"category": category, "missing_fields": missing},
                )
            )
    return events


def _find_stale_events(days: int) -> list[TriggerEvent]:
    events: list[TriggerEvent] = []
    with get_conn() as (conn, cur):
        cur.execute(SELECT_STALE, (days,))
        rows = cur.fetchall()
        conn.commit()

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    for source_url, category, payload, enriched_at in rows:
        if not source_url:
            continue
        events.append(
            TriggerEvent(
                url=source_url,
                trigger_type="scheduled_stale",
                reason=f"data older than {days} days",
                metadata={
                    "category": category,
                    "previous_enriched_at": enriched_at.isoformat() if hasattr(enriched_at, "isoformat") else str(enriched_at),
                    "cutoff": cutoff.isoformat(),
                },
            )
        )
    return events


def run_scheduler_loop() -> None:
    producer = build_producer()
    scan_interval = int(settings.freshness_scan_interval_seconds)
    max_events_per_cycle = int(settings.freshness_max_events_per_cycle)
    while True:
        stale_events = _find_stale_events(settings.freshness_days_threshold)
        missing_field_events = _find_missing_field_events()

        combined = stale_events + missing_field_events
        for event in combined[:max_events_per_cycle]:
            _publish_trigger(producer, event)

        print(
            json.dumps(
                {
                    "service": "freshness-monitor",
                    "stale_events": len(stale_events),
                    "missing_field_events": len(missing_field_events),
                    "published": min(len(combined), max_events_per_cycle),
                    "scan_interval_seconds": scan_interval,
                }
            )
        )
        time.sleep(scan_interval)


def run_realtime_request_listener() -> None:
    producer = build_producer()
    consumer = build_consumer("freshness-realtime-listener", settings.realtime_crawl_request_topic)

    for msg in consumer:
        event = msg.value
        url = event.get("url")
        if not url:
            consumer.commit()
            continue

        trigger = TriggerEvent(
            url=url,
            trigger_type="realtime_request",
            reason=event.get("reason", "user requested real-time crawl"),
            metadata={
                "requested_by": event.get("requested_by", "unknown"),
                "requested_at": event.get("requested_at", datetime.now(timezone.utc).isoformat()),
                "request_context": event,
            },
        )
        _publish_trigger(producer, trigger)
        consumer.commit()


def main() -> None:
    scheduler_thread = threading.Thread(target=run_scheduler_loop, daemon=True)
    scheduler_thread.start()
    run_realtime_request_listener()


if __name__ == "__main__":
    main()
