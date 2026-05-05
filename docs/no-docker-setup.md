# No-Docker Setup

Set env:
- `RUNTIME_PROFILE=no-docker`
- `DATABASE_URL=sqlite:///./collegecue_local.db`
- `QUEUE_BACKEND=memory`
- `WEBCLAW_ENABLED=false`

Run:
- `make init-db`
- `make crawl-fixture`
- `make validate-no-docker`

WebClaw is optional and disabled in offline mode.

## Source registry and controlled crawl
Add sources with `source:add`, then run `source:crawl-active`.
Missing required fields mark `freshness_status=incomplete` and create targeted crawl tasks.

## Debug extraction
Run: `python -m services.lite_pipeline.main extract:debug --url file://tests/fixtures/college_sample.html`
It prints sections, per-field confidence, missing fields, and final normalized record.

## Phase 7 commands
- `python -m services.lite_pipeline.main source:preview --id 1`
- `python -m services.lite_pipeline.main source:crawl --id 1 --dry-run`
- `python -m services.lite_pipeline.main export:entity --id 1 --format json`
Quality gate sends low-confidence outputs to quarantine.

## Safe real-site testing
Use pilot command in dry-run first, inspect quality_report, then rerun with --save.
Set `CRAWL_ALLOWED_DOMAINS` to restrict crawl scope.

## Phase 9 commands
- `pilot:http-smoke` (safe dry-run, no DB writes)
- `export:validate` for heading/empty/duplicate checks
- `readiness:check` for runtime readiness
- `audit:export` for compliance logs

## Admin review and publishing
See `docs/admin-review-workflow.md`. API endpoints mirror CLI review/publish/sync flow.
