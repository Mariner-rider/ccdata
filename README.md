# CollegeCue Data Crawler — VPS Production Deployment Guide

CollegeCue data ingestion is a DB-first crawler and publishing pipeline for institution, admission, job, news and research data.

The production data flow is always:

```text
Crawler → crawl_records (raw) → human review → public_entities (clean) → API/frontend
```

Data NEVER goes from `crawl_records` to the frontend directly. Every record must pass human review before it is published.

## Repository layout

- `services/lite_pipeline/main.py` — CLI entry point for migrations, crawling, review, publishing, workers and maintenance.
- `services/lite_pipeline/api.py` — FastAPI app for health, review/admin actions and public API endpoints.
- `services/deep_crawler/` — Crawl4AI-first deep crawler for institution websites.
- `services/admissions/`, `services/jobs/`, `services/news/`, `services/research/`, `services/institutions/` — domain-specific crawlers and repositories.
- `migrations/` — SQL migrations for crawler and domain tables.
- `airflow/dags/` — scheduled monitoring and crawl DAGs.
- `docker-compose.production.yml` — VPS production stack.
- `.github/workflows/ci.yml.disabled` — disabled CI workflow template, ready for later activation.

## Prerequisites

Install these on the VPS:

- Docker Engine with the Docker Compose plugin
- GNU Make
- Git
- A reverse proxy or load balancer in front of `core-api` if exposing the API publicly

## First-time VPS deployment

1. Clone the repository:

```bash
git clone https://github.com/Mariner-rider/ccdata.git
cd ccdata
```

2. Create the production environment file:

```bash
cp .env.production.example .env
```

3. Generate secrets and edit `.env`:

```bash
openssl rand -hex 32
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Set at least:

- `POSTGRES_PASSWORD`
- `MINIO_ROOT_PASSWORD`
- `SERVICE_API_KEY`
- `AIRFLOW_ADMIN_PASSWORD`
- `AIRFLOW_FERNET_KEY`
- `PUBLIC_CORS_ORIGINS`

4. Validate Compose configuration:

```bash
docker compose -f docker-compose.production.yml config
```

5. Build and start services:

```bash
make deploy
```

6. Check service health:

```bash
make ps
make logs
```

7. Run database setup/migrations when the API image is available:

```bash
docker compose -f docker-compose.production.yml exec core-api python -m services.lite_pipeline.main db:migrate
```

## Daily operations

Common Make targets:

```bash
make deploy          # build and start the production stack
make ps              # show service status
make logs            # follow production logs
make restart         # rebuild/recreate production services
make stop            # stop the production stack without deleting volumes
make test            # run pytest locally
make lint            # run ruff locally
make compile         # compile lite pipeline CLI
make storage-status  # inspect object storage usage
make storage-cleanup # clean old raw HTML through the CLI
```

## Crawling workflow

1. Add or identify a source in `source_registry`.
2. Preview crawl targets before fetching:

```bash
python -m services.lite_pipeline.main source:preview --id X
```

3. Run a dry run first:

```bash
python -m services.lite_pipeline.main source:crawl --id X --dry-run
```

4. Run a persisted crawl:

```bash
python -m services.lite_pipeline.main source:crawl --id X
```

5. Use deep crawl for full institution profiles:

```bash
python -m services.lite_pipeline.main source:deep-crawl --id X
```

## Human review and publishing

Raw crawl output is stored in `crawl_records`. It is not public.

Review and publish flow:

```bash
python -m services.lite_pipeline.main review:list
python -m services.lite_pipeline.main record:approve --id RECORD_ID --reviewed-by reviewer@example.com
python -m services.lite_pipeline.main publish:entity --id RECORD_ID
python -m services.lite_pipeline.main public:list
```

Only `public_entities` is used by `/public/*` endpoints and frontend-facing search.

## API notes

- Public endpoints are under `/public/*` and read from `public_entities`.
- Admin/write endpoints should be protected with `SERVICE_API_KEY` or `ADMIN_API_KEY`.
- CORS is controlled with `PUBLIC_CORS_ORIGINS`; do not use `*` in production.
- `/robots.txt` is served by the API for the CollegeCue platform.

## Airflow

Airflow uses the same PostgreSQL service and requires a valid `AIRFLOW_FERNET_KEY`.

Useful commands:

```bash
docker compose -f docker-compose.production.yml logs airflow-init
docker compose -f docker-compose.production.yml logs airflow-scheduler
docker compose -f docker-compose.production.yml logs airflow-webserver
```

## Troubleshooting

**Port 8000 not responding:**
Check core-api is healthy: `make ps`
Check logs: `docker compose -f docker-compose.production.yml logs core-api`

**Database connection refused:**
Check postgres is healthy. Check POSTGRES_PASSWORD in .env matches
what was used when postgres volume was first created. If you changed
the password after first run, you must delete the volume and re-init:
`docker compose -f docker-compose.production.yml down -v` (WARNING: deletes all data)

**Crawl returns empty results:**
Run source:preview --id X first. Check if robots.txt is blocking.
Check if the site needs JavaScript — confirm Playwright is running.

**Airflow not starting:**
Verify AIRFLOW_FERNET_KEY is set in .env and is a valid Fernet key.
Check: `docker compose -f docker-compose.production.yml logs airflow-init`

**Out of disk space:**
Clean old raw HTML: `make storage-cleanup`
Check MinIO storage: `make storage-status`
Check Docker images: `docker system df`
Prune unused images: `docker image prune -f`

## CI (disabled — ready to activate)

A complete CI workflow is stored at:
  .github/workflows/ci.yml.disabled

To activate when you are ready:
1. Rename the file to ci.yml
2. Add repository secrets in GitHub:
   Settings → Secrets → POSTGRES_PASSWORD, SERVICE_API_KEY
3. Push — CI runs automatically on every PR

---

All phase notes (Phase 5, Phase 7 etc.) are intentionally
removed — they were internal development notes, not
operational documentation. If needed they are preserved
in git history.

End of README. Do not add any content after this line.
