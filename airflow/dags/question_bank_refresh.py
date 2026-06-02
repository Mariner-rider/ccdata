"""Weekly Airflow refresh for CollegeCue question-bank content."""

from __future__ import annotations

from datetime import datetime

try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
except ImportError:  # pragma: no cover
    DAG = None
    BashOperator = None

EXAMS = ["NDA", "JEE", "BANKING", "RAILWAY", "SSC", "UPSC"]

if DAG and BashOperator:
    with DAG(
        dag_id="question_bank_weekly_refresh",
        schedule="0 20 * * 6",
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["collegecue", "question-bank"],
    ) as dag:
        crawl_tasks = [
            BashOperator(
                task_id=f"crawl_{exam.lower()}",
                bash_command=f"python -m services.lite_pipeline.main test:crawl --exam-type {exam}",
            )
            for exam in EXAMS
        ]

        report_stats = BashOperator(
            task_id="report_stats",
            bash_command="python -m services.lite_pipeline.main test:stats",
        )

        crawl_tasks >> report_stats
else:
    dag = None
