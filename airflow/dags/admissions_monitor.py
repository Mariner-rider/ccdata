from __future__ import annotations

from datetime import datetime, timezone

import psycopg
from airflow import DAG
from airflow.operators.python import PythonOperator

from _common import POSTGRES_DSN, default_dag_args
from services.admissions.crawler import crawl_admissions_sync


def recrawl_active_admission_sources() -> None:
    with psycopg.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, entity_name, official_url
                FROM source_registry
                WHERE is_active = 1
                  AND entity_type IN ('college','university','school','coaching_centre','abroad_university')
                ORDER BY id
                """
            )
            sources = cur.fetchall()
    for _source_id, entity_name, official_url in sources:
        with psycopg.connect(POSTGRES_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO institutions(name, source_url, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (source_url) DO UPDATE
                    SET name = EXCLUDED.name, updated_at = NOW()
                    RETURNING id
                    """,
                    (entity_name, official_url),
                )
                institution_id = cur.fetchone()[0]
            conn.commit()
        crawl_admissions_sync(
            POSTGRES_DSN,
            entity_id=institution_id,
            entity_name=entity_name,
            source_url=official_url,
            intake_year=datetime.now(timezone.utc).year,
        )


def mark_closed_admissions() -> None:
    with psycopg.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE admissions
                SET status = 'closed', updated_at = NOW()
                WHERE application_end_date IS NOT NULL
                  AND application_end_date < CURRENT_DATE
                  AND status != 'closed'
                """
            )
        conn.commit()


with DAG(
    dag_id="admissions_monitor",
    description="Re-crawl active admission sources every 6 hours and close expired notices",
    schedule="0 */6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_dag_args(),
    tags=["admissions", "crawl"],
) as dag:
    PythonOperator(task_id="recrawl_active_admission_sources", python_callable=recrawl_active_admission_sources)
    PythonOperator(task_id="mark_closed_admissions", python_callable=mark_closed_admissions)
