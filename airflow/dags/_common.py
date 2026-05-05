from __future__ import annotations

import json
import os
from datetime import timedelta
from email.mime.text import MIMEText
import smtplib

import psycopg
from kafka import KafkaProducer

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://crawler:crawler@postgres:5432/crawler")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CRAWL_QUEUE_TOPIC = os.getenv("CRAWL_QUEUE_TOPIC", "crawl.queue")
REALTIME_CRAWL_REQUEST_TOPIC = os.getenv("REALTIME_CRAWL_REQUEST_TOPIC", "crawl.requests.realtime")


def default_dag_args() -> dict:
    return {
        "owner": "ccdata-platform",
        "depends_on_past": False,
        "retries": int(os.getenv("AIRFLOW_TASK_RETRIES", "3")),
        "retry_delay": timedelta(minutes=int(os.getenv("AIRFLOW_TASK_RETRY_MINUTES", "5"))),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=45),
        "email_on_retry": False,
        "on_failure_callback": task_failure_alert,
    }


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
    )


def task_failure_alert(context) -> None:
    task_instance = context["task_instance"]
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    run_id = context.get("run_id")
    exception = context.get("exception")

    body = (
        f"Airflow task failed\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Run: {run_id}\n"
        f"Exception: {exception}\n"
    )

    # Persist failure to operational logs table.
    with psycopg.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO crawl_logs(source_id, url, status, detail, event_ts)
                VALUES (NULL, '', 'airflow_failure', %s, NOW())
                """,
                (body[:2000],),
            )
        conn.commit()

    # Optional email alert (if SMTP configured).
    smtp_host = os.getenv("ALERT_SMTP_HOST")
    smtp_to = os.getenv("ALERT_TO_EMAIL")
    smtp_from = os.getenv("ALERT_FROM_EMAIL", "airflow@ccdata.local")
    if smtp_host and smtp_to:
        msg = MIMEText(body)
        msg["Subject"] = f"[ALERT] Airflow failure: {dag_id}.{task_id}"
        msg["From"] = smtp_from
        msg["To"] = smtp_to
        with smtplib.SMTP(smtp_host, int(os.getenv("ALERT_SMTP_PORT", "25"))) as server:
            server.sendmail(smtp_from, [smtp_to], msg.as_string())
