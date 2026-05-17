from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from _common import POSTGRES_DSN, default_dag_args
from services.news.crawler import NEWS_SOURCES, crawl_news_sync


def crawl_education_news() -> None:
    crawl_news_sync(POSTGRES_DSN, sources=list(NEWS_SOURCES))


with DAG(
    dag_id="news_education_monitor",
    description="Crawl education news and government education updates every 2 hours",
    schedule="0 */2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_dag_args(),
    tags=["news", "education", "crawl"],
) as dag:
    PythonOperator(task_id="crawl_education_news", python_callable=crawl_education_news)
