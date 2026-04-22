from __future__ import annotations

from datetime import datetime, timezone

import psycopg
from airflow import DAG
from airflow.operators.python import PythonOperator

from _common import CRAWL_QUEUE_TOPIC, POSTGRES_DSN, build_producer, default_dag_args


def trigger_daily_news_crawl() -> None:
    producer = build_producer()
    with psycopg.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, seed_url
                FROM source_registry
                WHERE is_active = true
                  AND COALESCE(category, 'general') = 'news'
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
                        "priority": "news",
                        "trigger_type": "airflow_daily_news_crawl",
                        "triggered_at": datetime.now(timezone.utc).isoformat(),
                        "max_depth": 2,
                    },
                )
            conn.commit()
    producer.flush()


with DAG(
    dag_id="daily_news_crawl",
    description="Daily crawl for news sources",
    schedule="0 3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_dag_args(),
    tags=["crawl", "daily", "news"],
) as dag:
    PythonOperator(task_id="trigger_news_crawl", python_callable=trigger_daily_news_crawl)
