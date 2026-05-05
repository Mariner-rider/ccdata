from __future__ import annotations

from datetime import datetime

import psycopg
from airflow import DAG
from airflow.operators.python import PythonOperator

from _common import POSTGRES_DSN, default_dag_args


def refresh_pipeline_metrics() -> None:
    with psycopg.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO airflow_pipeline_metrics(metric_name, metric_value, measured_at)
                SELECT 'crawl_logs_24h', COUNT(*), NOW()
                FROM crawl_logs
                WHERE event_ts >= NOW() - INTERVAL '24 hours';
                """
            )
            cur.execute(
                """
                INSERT INTO airflow_pipeline_metrics(metric_name, metric_value, measured_at)
                SELECT 'trigger_events_24h', COUNT(*), NOW()
                FROM crawl_trigger_events
                WHERE triggered_at >= NOW() - INTERVAL '24 hours';
                """
            )
            cur.execute(
                """
                INSERT INTO airflow_pipeline_metrics(metric_name, metric_value, measured_at)
                SELECT 'enriched_records_24h', COUNT(*), NOW()
                FROM enriched_records
                WHERE enriched_at >= NOW() - INTERVAL '24 hours';
                """
            )
        conn.commit()


with DAG(
    dag_id="pipeline_monitoring_dashboard",
    description="Updates pipeline metrics for monitoring dashboard",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_dag_args(),
    tags=["monitoring", "dashboard"],
) as dag:
    PythonOperator(task_id="refresh_metrics", python_callable=refresh_pipeline_metrics)
