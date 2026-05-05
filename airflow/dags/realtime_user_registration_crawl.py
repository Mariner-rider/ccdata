from __future__ import annotations

from datetime import datetime, timezone

import psycopg
from airflow import DAG
from airflow.operators.python import PythonOperator

from _common import POSTGRES_DSN, REALTIME_CRAWL_REQUEST_TOPIC, build_producer, default_dag_args


def trigger_realtime_registration_crawls() -> None:
    producer = build_producer()
    with psycopg.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, profile_url, user_id, created_at
                FROM user_registration_events
                WHERE crawl_requested = false
                ORDER BY created_at ASC
                LIMIT 500
                """
            )
            rows = cur.fetchall()
            for event_id, profile_url, user_id, created_at in rows:
                producer.send(
                    REALTIME_CRAWL_REQUEST_TOPIC,
                    {
                        "url": profile_url,
                        "requested_by": f"user:{user_id}",
                        "reason": "real-time crawl on user registration",
                        "requested_at": datetime.now(timezone.utc).isoformat(),
                        "event_id": event_id,
                        "registration_created_at": created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at),
                    },
                )
                cur.execute(
                    "UPDATE user_registration_events SET crawl_requested = true, crawl_requested_at = NOW() WHERE id = %s",
                    (event_id,),
                )
            conn.commit()
    producer.flush()


with DAG(
    dag_id="realtime_user_registration_crawl",
    description="Near real-time crawl requests generated from new user registrations",
    schedule="*/2 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_dag_args(),
    tags=["crawl", "realtime", "registration"],
) as dag:
    PythonOperator(task_id="publish_registration_requests", python_callable=trigger_realtime_registration_crawls)
