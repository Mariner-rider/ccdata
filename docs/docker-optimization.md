# Docker Optimization

- Split images: core/worker/browser-worker.
- Use slim base images.
- No browser dependencies in core image.
- `.dockerignore` prevents large context upload.
- local-lite excludes Kafka/Airflow/Elasticsearch/Nutch.
- For size report:
  ```bash
  python -m services.lite_pipeline.main docker:size-report
  docker system df
  ```
