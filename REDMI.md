# REDMI (Operational Quick Guide)

This file mirrors the main project guide.
For full details, read: **README.md**.

## Quick start (local)
1. Install Docker + Compose.
2. Run:
   ```bash
   docker compose up --build -d
   ```
3. Validate:
   ```bash
   docker compose ps
   curl http://localhost:8010/health
   curl http://localhost:8020/health
   ```
4. Airflow UI: `http://localhost:8080` (admin/admin)

## What this platform includes
- Distributed crawling (Nutch + Scrapy)
- Parsing (Playwright + BS4)
- Schema mapping + AI enrichment
- College page generation + chatbot sync
- Review ingestion + fake-review ML + sentiment
- Elasticsearch search + suggest
- Airflow schedules + monitoring DAG
- Freshness-triggered recrawl logic

## Merge into current branch
```bash
git checkout main
git pull origin main
git checkout <feature-branch>
git pull origin <feature-branch>
git checkout main
git merge --no-ff <feature-branch>
git push origin main
```

## VPS deployment summary
- Provision VPS (8vCPU/32GB recommended)
- Install Docker + Compose
- Configure env/secrets
- `docker compose up -d`
- Add reverse proxy + TLS + backups

> See README.md for the complete architecture, topics, schema, service-by-service behavior, and troubleshooting.

## Security quick note
- Set `SERVICE_API_KEY` before use.
- Call protected APIs with `X-API-Key` header.
- Local ports are bound to `127.0.0.1` in compose by default.
