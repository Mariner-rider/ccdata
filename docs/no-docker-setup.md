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
