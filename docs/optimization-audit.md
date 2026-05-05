# Optimization Audit

## Current services (from `docker-compose.yml`)
- postgres, zookeeper, kafka, elasticsearch, minio, createbucket
- airflow-init, airflow-scheduler, airflow-webserver
- seed-loader, scrapy-crawler, parser-engine, schema-mapper, ai-enrichment
- college-page-service, review-ingestion, search-engine, freshness-monitor
- nutch, nutch-bridge

## Heavy image/disk contributors
1. Playwright image/tooling (`services/Dockerfile.playwright`) + Chromium binaries.
2. Airflow stack + dependencies.
3. Elasticsearch persistent volume.
4. Kafka + Zookeeper logs.
5. Torch/Transformers in base worker image (installed for all services).
6. MinIO retained raw HTML objects.
7. Nutch container + crawldb artifacts.

## Dependency weight risks
- `torch`, `transformers`, `playwright`, `elasticsearch`, full scientific stack in single requirements file.
- Shared image for all services includes unnecessary dependencies for each worker.

## Likely unused/over-provisioned in local-dev
- Airflow in local developer mode.
- Nutch in local developer mode.
- Elasticsearch in local-lite mode.
- Browser rendering defaults for all pages.

## Recommended removals (local-lite)
- Remove: Kafka, Zookeeper, Elasticsearch, Airflow, Nutch, Playwright browser worker.
- Replace queue with Redis + RQ.
- Keep Postgres and a single lightweight FastAPI app + worker.
- Use WebClaw adapter first; fallback HTTP extractor only.
- Disable raw HTML storage by default (`DEBUG_RAW_HTML=false`).

## Target architecture split
- `local-lite`: postgres + redis + lite app/worker (+ optional WebClaw endpoint).
- `local-full`: postgres + redis + workers + optional browser worker + optional meilisearch.
- `production`: scalable queue/search/object storage/observability profile.

## Size reduction strategy
- Multi-stage Dockerfiles, slim images, non-root user.
- Per-service dependency isolation (core/worker/browser requirements).
- No browser packages in core images.
- Remove build caches and apt leftovers.
