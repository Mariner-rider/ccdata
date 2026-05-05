import time
from datetime import datetime, timezone

from services.common.config import settings
from services.common.db import get_conn
from services.common.kafka_client import build_producer


SELECT_DUE_SEEDS = """
SELECT id, seed_url, crawl_frequency_minutes
FROM source_registry
WHERE is_active = true
  AND (
      last_dispatched_at IS NULL
      OR last_dispatched_at + make_interval(mins => crawl_frequency_minutes) <= NOW()
  )
ORDER BY id
LIMIT 500;
"""

MARK_DISPATCHED = """
UPDATE source_registry
SET last_dispatched_at = NOW()
WHERE id = %s;
"""

INSERT_LOG = """
INSERT INTO crawl_logs(source_id, url, status, detail, event_ts)
VALUES (%s, %s, %s, %s, NOW());
"""


def dispatch_cycle() -> int:
    producer = build_producer()
    dispatched = 0
    with get_conn() as (conn, cur):
        cur.execute(SELECT_DUE_SEEDS)
        for source_id, url, frequency in cur.fetchall():
            message = {
                "source_id": source_id,
                "url": url,
                "priority": "seed",
                "dispatched_at": datetime.now(timezone.utc).isoformat(),
                "crawl_frequency_minutes": frequency,
                "max_depth": settings.max_depth,
            }
            producer.send(settings.crawl_queue_topic, message)
            cur.execute(MARK_DISPATCHED, (source_id,))
            cur.execute(
                INSERT_LOG,
                (source_id, url, "dispatched", f"seed_dispatched frequency={frequency}m"),
            )
            dispatched += 1
        conn.commit()
    producer.flush()
    return dispatched


def main() -> None:
    interval = int(__import__("os").getenv("SEED_DISPATCH_INTERVAL_SECONDS", "30"))
    while True:
        count = dispatch_cycle()
        print(f"[seed-loader] dispatched={count}")
        time.sleep(interval)


if __name__ == "__main__":
    main()
