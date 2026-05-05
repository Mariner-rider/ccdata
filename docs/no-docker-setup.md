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
