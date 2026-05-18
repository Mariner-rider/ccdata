from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from _common import POSTGRES_DSN, default_dag_args
from services.research.crawler import INDIA_SOURCES, crawl_research_sync


def crawl_research_hub() -> None:
    queries = [
        "arxiv.org Indian researchers 2025",
        "shodhganga PhD thesis 2025 engineering",
        "IIT research publications projects 2025",
        "AIIMS ongoing research projects medicine",
        "IIM research publications management 2025",
    ]
    crawl_research_sync(POSTGRES_DSN, queries=queries, seed_urls=list(INDIA_SOURCES), include_arxiv=True)


with DAG(
    dag_id="research_hub_daily",
    description="Daily Research Hub ingestion at 2 AM IST",
    schedule="30 20 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_dag_args(),
    tags=["research", "academic", "crawl"],
) as dag:
    PythonOperator(task_id="crawl_research_hub", python_callable=crawl_research_hub)
