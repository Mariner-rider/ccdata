# CCData Distributed Crawl Platform

A production-style, event-driven crawling and enrichment platform with:
- Apache Nutch + Scrapy crawling
- Playwright/BeautifulSoup parsing
- Schema mapping + AI enrichment
- Review ingestion and fake-review detection
- Elasticsearch search APIs
- Airflow orchestration
- PostgreSQL persistence + audit logs
- Kafka event bus + MinIO raw HTML storage

---

## 1) What this project is

This repository implements a modular data pipeline for discovering, crawling, parsing, enriching, indexing, and serving education/job-related data.

It is built as a set of independent services connected through Kafka topics and a shared Postgres schema.

Core goals:
1. **Scalable crawling** (seeded + discovery + realtime triggers)
2. **Structured extraction** from static/dynamic web pages
3. **Normalized schema outputs** across categories (college, jobs, scholarships, news)
4. **AI enrichments** (classification, summarization, sentiment, fake detection)
5. **Search APIs** (Elasticsearch with low-latency-oriented query config)
6. **Operational reliability** (Airflow schedules, freshness triggers, logs, metrics, audits)

---

## 2) End-to-end architecture

### 2.1 Data flow (high-level)

1. `seed-loader` dispatches due sources -> `crawl.queue`
2. `nutch-bridge` discovers URLs -> `crawl.queue`
3. `scrapy-crawler` fetches pages -> stores raw HTML in MinIO -> emits `crawl.results`
4. `parser-engine` parses structured data -> emits `parse.results`
5. `schema-mapper` normalizes into platform schema -> emits `schema.mapped`
6. `ai-enrichment` enriches records -> emits `content.enriched`
7. `college-page-service` creates/updates page + sections + chatbot sync -> emits `college.pages.events`
8. `search-engine` consumes page events and indexes into Elasticsearch
9. `review-ingestion` consumes crawl outputs for web reviews and accepts user reviews via API -> emits `reviews.events`
10. `freshness-monitor` detects stale/incomplete data and republishes recrawl triggers
11. Airflow orchestrates periodic/realtime crawl pipelines and monitoring metrics

### 2.2 Main runtime components

- **Infra**: `postgres`, `kafka`, `zookeeper`, `minio`, `elasticsearch`
- **Orchestration**: `airflow-init`, `airflow-scheduler`, `airflow-webserver`
- **Crawl pipeline services**: `seed-loader`, `nutch`, `nutch-bridge`, `scrapy-crawler`
- **Processing services**: `parser-engine`, `schema-mapper`, `ai-enrichment`, `college-page-service`, `review-ingestion`, `search-engine`, `freshness-monitor`

---

## 3) Services and responsibilities

### 3.1 `seed-loader`
- Reads `source_registry`
- Dispatches due seeds based on `crawl_frequency_minutes`
- Logs dispatch events in `crawl_logs`

### 3.2 `nutch-bridge`
- Runs Nutch discovery cycle
- Publishes discovered URLs to `crawl.queue`

### 3.3 `scrapy-crawler`
- Consumes crawl tasks from Kafka
- Obeys `robots.txt`, rate limiting, autothrottle
- Detects duplicates and supports incremental crawling via hash/etag/last-modified
- Stores raw HTML in MinIO

### 3.4 `parser-engine`
- Parses pages with BeautifulSoup + XPath (lxml)
- Uses Playwright fallback for JS-heavy pages
- Emits confidence-scored extraction JSON

### 3.5 `schema-mapper`
- Maps raw keys dynamically to canonical schema fields
- Uses exact + fuzzy + semantic matching
- Supports categories: school, colleges, jobs, scholarships, news

### 3.6 `ai-enrichment`
- Fills missing fields with web search
- Classifies content (college/job/news/scholarship)
- Generates summaries
- Computes sentiment and fake-content signals
- Optional LLM API fallback

### 3.7 `college-page-service`
- Creates page if missing, updates if exists
- Upserts sections: `info`, `courses`, `faculty`, `hostel`, `placement`
- Tracks sync events and syncs mirror payload into chatbot DB table

### 3.8 `review-ingestion`
- Crawls web reviews from raw HTML
- Accepts user reviews via API endpoint
- Fake-review probability using lightweight ML
- Sentiment scoring (VADER)

### 3.9 `search-engine`
- Elasticsearch-backed search API
- Filters by country/course/fees
- Ranks by popularity + rating
- Provides autosuggestions

### 3.10 `freshness-monitor`
- Triggers recrawl if data > threshold age (default 30 days)
- Triggers targeted recrawl if required fields missing
- Triggers realtime recrawl from user request topic

### 3.11 Airflow DAGs
- `monthly_full_crawl`
- `daily_news_crawl`
- `weekly_job_updates`
- `realtime_user_registration_crawl`
- `pipeline_monitoring_dashboard`

All DAGs include retry logic + failure callback logging, and optional SMTP alerts.

---

## 4) Kafka topics

Default topics used in this repo:
- `crawl.queue`
- `crawl.results`
- `parse.results`
- `schema.mapped`
- `content.enriched`
- `college.pages.events`
- `crawl.requests.realtime`
- `reviews.events`

---

## 5) PostgreSQL schema overview

### 5.1 Operational tables
- `source_registry`
- `page_state`
- `crawl_logs`
- `parsed_college_data`
- `normalized_records`
- `enriched_records`
- `crawl_trigger_events`
- `user_registration_events`
- `airflow_pipeline_metrics`

### 5.2 Domain tables
- `institutes`
- `colleges`
- `admissions`
- `jobs`
- `scholarships`
- `reviews`

### 5.3 Event/page/chatbot tables
- `college_pages`
- `college_page_sections`
- `college_page_sync_events`
- `chatbot_college_pages`

### 5.4 Audit and scale features
- `audit_logs` (partitioned)
- `reviews` (partitioned)
- JSONB + GIN indexes for flexible querying
- trigram + tsvector search indexes

---

## 6) API endpoints

### 6.1 Search engine (`:8010`)
- `GET /health`
- `GET /search?q=&country=&course=&fees_max=`
- `GET /suggest?q=`
- `POST /index`

### 6.2 Review ingestion (`:8020`)
- `GET /health`
- `POST /reviews/user`

---

## 7) Local machine setup (step-by-step)

## 7.1 Prerequisites
1. Docker Engine + Docker Compose plugin
2. At least 8 CPU / 16 GB RAM recommended (Playwright + ES + Airflow are heavy)
3. Open ports: `8080`, `8010`, `8020`, `9200`

## 7.2 Clone and prepare
```bash
git clone <your-repo-url>
cd ccdata
cp .env.example .env 2>/dev/null || true
```

(Optional) set env variables in shell or `.env`:
- `OPENAI_API_KEY` (if LLM fallback needed)
- SMTP alert vars (`ALERT_SMTP_HOST`, `ALERT_TO_EMAIL`, etc.)

## 7.3 Start stack
```bash
docker compose up --build -d
```

## 7.4 Verify health
```bash
docker compose ps
docker compose logs -f airflow-scheduler
curl http://localhost:8010/health
curl http://localhost:8020/health
curl http://localhost:9200
```

## 7.5 Access UI/services
- Airflow UI: `http://localhost:8080` (default `admin/admin`)
- Search API: `http://localhost:8010`
- Review API: `http://localhost:8020`
- Elasticsearch: `http://localhost:9200`

## 7.6 Seed data
Use psql against Postgres:
```sql
INSERT INTO source_registry(seed_url, crawl_frequency_minutes, is_active, category)
VALUES
  ('https://example.edu', 60, true, 'colleges'),
  ('https://example-news.com', 1440, true, 'news'),
  ('https://example-jobs.com', 10080, true, 'jobs');
```

---

## 8) VPS deployment (step-by-step)

## 8.1 Recommended VPS baseline
- 8 vCPU, 32 GB RAM, 200+ GB SSD
- Ubuntu 22.04+
- Docker + Compose installed
- Reverse proxy (Nginx/Caddy) with TLS

## 8.2 Provision
1. Create non-root deploy user
2. Install Docker/Compose
3. Open firewall ports (or only expose via reverse proxy)
4. Clone repo onto VPS

## 8.3 Configure production env
Set strong secrets and production values:
- database credentials
- S3 credentials
- SMTP credentials
- `OPENAI_API_KEY`
- service-specific resource limits

Example startup:
```bash
docker compose pull
/docker compose build --no-cache

docker compose up -d
```

## 8.4 Persistent data and backups
- Persist Docker volumes (`pgdata`, `minio`, `esdata`)
- Schedule Postgres backups (`pg_dump` / WAL strategy)
- Snapshot VPS volume regularly

## 8.5 Monitoring and operations
- Use Airflow UI for DAG status
- Query `crawl_logs`, `crawl_trigger_events`, `airflow_pipeline_metrics`
- Add external observability (Prometheus/Grafana) in production

---

## 9) How to merge this into your current branch ("current bank")

If by “bank” you mean your **main code branch**:

## 9.1 Merge via Git
```bash
git checkout main
git pull origin main
git checkout <feature-branch>
git pull origin <feature-branch>
git checkout main
git merge --no-ff <feature-branch>
# resolve conflicts if any
git push origin main
```

## 9.2 Rebase option (clean history)
```bash
git checkout <feature-branch>
git fetch origin
git rebase origin/main
# resolve conflicts
git push --force-with-lease origin <feature-branch>
# then merge PR
```

## 9.3 Post-merge validation checklist
1. `docker compose config`
2. `docker compose up --build -d`
3. Airflow DAGs visible and unpaused
4. Kafka topics receiving messages
5. Search API and review API health endpoints pass
6. DB tables created successfully from `sql/init.sql`

---

## 10) What was created beyond base crawling

In addition to crawling/parsing, this project now includes:
- event-driven schema normalization
- AI enrichment with optional LLM fallback
- freshness/retrigger automation
- dynamic college-page generation + chatbot sync
- review ingestion + fake-review detection + sentiment scoring
- Elasticsearch search + autosuggestions
- Airflow scheduled and near-realtime orchestration
- partitioned audit and review tables for scale-oriented data operations

---

## 11) Known caveats / important notes

1. This repository includes many services and heavy dependencies; production hardening is still required (resource limits, secret management, HA Kafka/Postgres, backups, observability).
2. Some ML models are warm-start baselines and should be replaced with offline-trained artifacts for production quality.
3. Partition maintenance (creating future partitions) should be automated with scheduled DB jobs.
4. Docker resource pressure can degrade response times; tune ES JVM, worker counts, and crawl concurrency per environment.

---

## 12) Quick troubleshooting

- **Service crash loop**: `docker compose logs -f <service>`
- **No crawl output**: confirm `source_registry` has active seeds
- **No search results**: ensure `college.pages.events` is populated and `search-engine` indexer is running
- **Review API 500**: verify `reviews` table exists and MinIO credentials are valid
- **Airflow task failures**: inspect `crawl_logs` with `status='airflow_failure'`

---

## 13) REDMI file

A companion `REDMI.md` is included as an operational quick-start mirror of this README.

## 14) Security hardening and API protection

To reduce unauthorized access risk, this project now uses a mandatory service API key for public API endpoints.

### 14.1 API access control
- Protected endpoints require header:
  - `X-API-Key: <SERVICE_API_KEY>`
- Applied to:
  - search API (`/search`, `/suggest`, `/index`)
  - user review submission API (`/reviews/user`)

If `SERVICE_API_KEY` is left as default (`change-me`) the service returns a protection error until secure key is configured.

### 14.2 Rate limiting
- In-memory per-client-IP rate limiting is applied on protected endpoints.
- Config: `API_RATE_LIMIT_PER_MINUTE` (default `120`).

### 14.3 Surface reduction
- Exposed ports are bound to localhost by default in compose:
  - Airflow `127.0.0.1:8080`
  - Elasticsearch `127.0.0.1:9200`
  - Search API `127.0.0.1:8010`
  - Review API `127.0.0.1:8020`

### 14.4 Recommended production controls (mandatory)
1. Rotate `SERVICE_API_KEY` and keep in secret manager (not in repo).
2. Put APIs behind reverse proxy + TLS + WAF.
3. Restrict network access using firewall/security groups.
4. Use dedicated DB credentials per service with least privilege.
5. Enable Elasticsearch security features (auth/TLS) for production clusters.
6. Add centralized auth (JWT/OAuth2) if external users consume APIs directly.
