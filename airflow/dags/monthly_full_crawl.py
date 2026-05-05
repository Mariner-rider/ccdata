from __future__ import annotations

from datetime import datetime, timezone

import psycopg
from airflow import DAG
from airflow.operators.python import PythonOperator

from _common import CRAWL_QUEUE_TOPIC, POSTGRES_DSN, build_producer, default_dag_args


def trigger_monthly_full_crawl() -> None:
    producer = build_producer()
    with psycopg.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, seed_url FROM source_registry WHERE is_active = true ORDER BY id")
            for source_id, url in cur.fetchall():
                producer.send(
                    CRAWL_QUEUE_TOPIC,
                    {
                        "source_id": source_id,
                        "url": url,
                        "priority": "full",
                        "trigger_type": "airflow_monthly_full_crawl",
                        "triggered_at": datetime.now(timezone.utc).isoformat(),
                        "max_depth": 3,
                    },
                )
            conn.commit()
    producer.flush()


with DAG(
    dag_id="monthly_full_crawl",
    description="Monthly full crawl for all active sources",
    schedule="0 2 1 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_dag_args(),
    tags=["crawl", "monthly"],
) as dag:
    PythonOperator(task_id="trigger_full_crawl", python_callable=trigger_monthly_full_crawl)
