FROM apache/airflow:2.10.3

USER root
RUN apt-get update && apt-get install -y --no-install-recommends gcc && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir psycopg[binary]==3.2.3 kafka-python==2.0.2
