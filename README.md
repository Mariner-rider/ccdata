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

## Phase 5: Controlled real-site crawling
Use source registry CLI:
- `python -m services.lite_pipeline.main source:add --entity-type college --entity-name "IIM Bangalore" --url https://www.iimb.ac.in`
- `python -m services.lite_pipeline.main source:list`
- `python -m services.lite_pipeline.main source:crawl-active`
Safe limits via env: CRAWL_MAX_PAGES_PER_SOURCE, CRAWL_MAX_DEPTH, CRAWL_RATE_LIMIT_SECONDS, CRAWL_TIMEOUT_SECONDS, CRAWL_SAME_DOMAIN_ONLY.

## Phase 7 robustness
- Use `source:preview --id <id>` to inspect prioritized crawl URLs, page type, robots decision, and estimated page count.
- Use `source:crawl --id <id> --dry-run` to fetch/merge without DB writes.
- Multi-page crawl merges into one entity profile and removes heading-only pollution in list fields.
- Quality gate routes low-quality records to `quarantine_records`.
- Export page-ready JSON with `export:entity --id <id> --format json`.

## Phase 8 pilot readiness
- Pilot run: `python -m services.lite_pipeline.main pilot:college --name "IIM Bangalore" --url https://www.iimb.ac.in --dry-run`.
- Add `--save` to persist merged entity.
- Configure `CRAWL_ALLOWED_DOMAINS=example.edu,example.ac.in` for safe allowlist control.
- Compliance logs include robots decisions, skipped URLs (binary/cross-domain/allowlist), and extraction errors.

## Phase 9 production hardening
- HTTP smoke dry-run: `python -m services.lite_pipeline.main pilot:http-smoke --url https://example.edu --name "Example"`.
- Validate export: `python -m services.lite_pipeline.main export:validate --id 1`.
- Readiness: `python -m services.lite_pipeline.main readiness:check`.
- Audit export: `python -m services.lite_pipeline.main audit:export --format json`.

## Phase 10 tuning notes
For Indian college domains, crawler now prioritizes programmes/academics/departments/fee-structure/career-development/campus-life/people/directory paths.

## Phase 11 admin review
Use `review:list`, `review:approve/reject`, `publish:entity`, and `chatbot:sync`.
Records are not auto-published; review is mandatory.

## Phase 13 migrations and API auth
Run `python -m services.lite_pipeline.main db:migrate` and `db:status` before local-lite startup.
Set `ADMIN_API_KEY` to protect write endpoints; pass `X-API-Key` header.
Idempotency: pass `Idempotency-Key` header for publish/sync.

## Phase 14 async crawling
- API `POST /sources/{id}/crawl` enqueues crawl job and returns `job_id`.
- Poll job via `GET /jobs/{id}`.
- Worker commands: `worker:once`, `worker:run`, `jobs:list`, `jobs:show`, `jobs:cancel`.
- Scheduler: `scheduler:run-once` enqueues refresh jobs for active sources.

## Phase 15 scheduling policy
- Due-date enqueue rule: active + never-crawled or older than crawl_frequency_days.
- Budgets: `DAILY_MAX_JOBS`, `DAILY_MAX_JOBS_PER_DOMAIN`.
- Failure controls: `MAX_FAILED_JOBS_PER_SOURCE`, `CRAWL_COOLDOWN_HOURS_AFTER_FAILURE`.
- Retry policy: exponential backoff with `retry_count` and `next_retry_at`.
- Stale running recovery: `JOB_STALE_MINUTES`.
