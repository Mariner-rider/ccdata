# CollegeCue Optimized Crawler (Phase 3)

## One-command validation
```bash
make validate-lite
```
This runs compile/tests, docker checks (if available), local-lite compose validation, health probes, crawl smoke commands, and size report generation.

## Dependency locking
- `pyproject.toml` with pinned runtime/dev deps and dependency groups.
- `uv.lock` (repository lock artifact placeholder in restricted env; regenerate with `uv lock`).
- `requirements.lock.txt` pinned export.

Install options:
```bash
uv sync
uv sync --extra dev
# or pip fallback
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

## Local-lite profile
```bash
docker compose -f docker-compose.local-lite.yml up --build
```
Includes only:
- PostgreSQL
- Redis
- core-api
- worker

Excludes:
- Kafka
- Airflow
- Elasticsearch
- Nutch
- Playwright
- Selenium

## Health endpoints
- `/health`
- `/health/db`
- `/health/redis`
- `/health/webclaw`

When `WEBCLAW_ENABLED=false`:
- `health/webclaw` returns disabled
- extraction falls back to HTTP+BS4

## CLI commands
```bash
python -m services.lite_pipeline.main crawl:single --url https://example.com
python -m services.lite_pipeline.main extract:test --url https://example.com
python -m services.lite_pipeline.main crawl:missing-fields
python -m services.lite_pipeline.main crawl:monthly-refresh
python -m services.lite_pipeline.main storage:status
python -m services.lite_pipeline.main storage:cleanup
python -m services.lite_pipeline.main docker:size-report
```

## Make targets
- `make install`
- `make install-dev`
- `make test`
- `make lint`
- `make compile`
- `make docker-build-lite`
- `make docker-up-lite`
- `make docker-down-lite`
- `make docker-size-report`
- `make validate-lite`
- `make crawl-single URL=https://example.com`
- `make storage-cleanup`
- `make storage-status`

## Image-size hard limits
Enforced by `scripts/docker_size_report.py`:
- core image <= 1.5GB
- worker image <= 1.5GB
- browser-worker image <= 3GB
- local-lite total <= 5GB

Override guard:
```bash
ALLOW_LARGE_IMAGES=true make docker-size-report
```

## Verification artifacts
- `docs/docker-size-report.md`
- `docs/local-lite-verification-report.md`

## Additional docs
- `docs/optimization-audit.md`
- `docs/local-lite-setup.md`
- `docs/docker-optimization.md`
- `docs/webclaw-integration.md`
- `docs/data-pipeline.md`

## No-Docker mode
Use offline profile with SQLite and in-memory queue.
Run `make init-db`, `make crawl-fixture`, and `make validate-no-docker`.
WebClaw is optional and disabled by default in this profile.
