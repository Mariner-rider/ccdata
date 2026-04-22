from __future__ import annotations

from datetime import datetime, timezone

import psycopg
from airflow import DAG
from airflow.operators.python import PythonOperator

from _common import CRAWL_QUEUE_TOPIC, POSTGRES_DSN, build_producer, default_dag_args


def trigger_weekly_job_updates() -> None:
    producer = build_producer()
    with psycopg.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, seed_url
                FROM source_registry
                WHERE is_active = true
                  AND COALESCE(category, 'general') = 'jobs'
                ORDER BY id
                """
            )
            rows = cur.fetchall()
            for source_id, url in rows:
                producer.send(
                    CRAWL_QUEUE_TOPIC,
                    {
                        "source_id": source_id,
                        "url": url,
                        "priority": "jobs",
                        "trigger_type": "airflow_weekly_job_updates",
                        "triggered_at": datetime.now(timezone.utc).isoformat(),
                        "max_depth": 2,
                    },
                )
            conn.commit()
    producer.flush()


with DAG(
    dag_id="weekly_job_updates",
    description="Weekly recrawl for job sources",
    schedule="0 4 * * 1",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_dag_args(),
    tags=["crawl", "weekly", "jobs"],
) as dag:
    PythonOperator(task_id="trigger_job_updates", python_callable=trigger_weekly_job_updates)
