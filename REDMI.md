# REDMI Quick Guide

```bash
uv sync --extra dev
make test
make docker-build-lite
make docker-up-lite
make validate-lite
```

Health:
```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/health/webclaw
```

Crawl one URL:
```bash
make crawl-single URL=https://example.com
```
