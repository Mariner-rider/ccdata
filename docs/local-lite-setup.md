# Local Lite Setup (Phase 3)

## Setup
```bash
uv sync --extra dev
# or
pip install -r requirements-dev.txt
```

## Validate code
```bash
make test
make compile
```

## Build and run
```bash
make docker-build-lite
make docker-up-lite
make validate-lite
make docker-size-report
```

## Health checks
```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/health/db
curl http://127.0.0.1:8000/health/redis
curl http://127.0.0.1:8000/health/webclaw
```

## CLI smoke
```bash
python -m services.lite_pipeline.main extract:test --url https://example.com
python -m services.lite_pipeline.main crawl:single --url https://example.com
python -m services.lite_pipeline.main crawl:missing-fields
python -m services.lite_pipeline.main storage:status
python -m services.lite_pipeline.main storage:cleanup
```

## Troubleshooting
- Docker not installed: install Docker Engine + Compose plugin.
- Docker daemon not running: start daemon/service.
- Port 5432 in use: edit postgres mapping or stop local postgres.
- Port 6379 in use: edit redis mapping or stop local redis.
- Port 8000 in use: change core-api port mapping.
- psycopg issue: use `psycopg[binary]` via requirements-dev install.
- WebClaw disabled mode: set `WEBCLAW_ENABLED=false`; fallback extractor is used.

## Expected usage
- local-lite disk target: <10GB runtime
- local-lite memory target: <4GB
- local-lite image target: <5GB total (if possible)
