from __future__ import annotations

from datetime import datetime

import psycopg
from airflow import DAG
from airflow.operators.python import PythonOperator

from _common import POSTGRES_DSN, default_dag_args
from services.jobs.crawler import GOVT_SOURCES, INTERNSHIP_SOURCES, PRIVATE_SOURCES, crawl_jobs_sync


def crawl_government_jobs() -> None:
    seed_urls = [f"https://{source}" for source in GOVT_SOURCES]
    crawl_jobs_sync(POSTGRES_DSN, seed_urls=seed_urls, job_type="govt")


def crawl_private_jobs() -> None:
    seed_urls = [f"https://{source}" for source in PRIVATE_SOURCES + INTERNSHIP_SOURCES]
    crawl_jobs_sync(POSTGRES_DSN, seed_urls=seed_urls, job_type="private", query="fresher jobs India 2025")
    crawl_jobs_sync(POSTGRES_DSN, seed_urls=[f"https://{source}" for source in INTERNSHIP_SOURCES], job_type="internship", query="internships India remote stipend")


def mark_closed_jobs() -> None:
    with psycopg.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs
                SET status = 'closed', updated_at = NOW()
                WHERE application_end_date IS NOT NULL
                  AND application_end_date < CURRENT_DATE
                  AND status != 'closed'
                """
            )
        conn.commit()


with DAG(
    dag_id="jobs_government_monitor",
    description="Crawl government job sources every 3 hours",
    schedule="0 */3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_dag_args(),
    tags=["jobs", "government", "crawl"],
) as govt_dag:
    PythonOperator(task_id="crawl_government_jobs", python_callable=crawl_government_jobs)
    PythonOperator(task_id="mark_closed_jobs", python_callable=mark_closed_jobs)


with DAG(
    dag_id="jobs_private_internship_monitor",
    description="Crawl private job and internship sources every 6 hours",
    schedule="0 */6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_dag_args(),
    tags=["jobs", "private", "internships", "crawl"],
) as private_dag:
    PythonOperator(task_id="crawl_private_jobs", python_callable=crawl_private_jobs)
    PythonOperator(task_id="mark_closed_jobs", python_callable=mark_closed_jobs)
