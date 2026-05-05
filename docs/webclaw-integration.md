# WebClaw Integration

Set env:
- `WEBCLAW_ENABLED=true`
- `WEBCLAW_BASE_URL=http://<webclaw-host>:<port>`

Adapter: `services/extraction/webclaw_adapter/webclaw_adapter.py`

Calls supported:
- scrape
- crawl/map
- extract
- summarize
- diff

Fallback:
- `fallback_extractor.py` uses HTTP+BS4 if WebClaw unavailable.
