# CollegeCue Data Crawler

CollegeCue Data Crawler is the data collection and API backend for collegecue.com. It crawls colleges, universities, coaching centres, schools, admissions, jobs, news, research, and exam questions, then serves structured data to the frontend through a public FastAPI.

## What this repo does

- Crawls institution websites with Crawl4AI/Playwright for JavaScript-heavy pages and httpx/BeautifulSoup for static pages.
- Extracts structured profiles for colleges, universities, schools, coaching centres, and abroad universities.
- Collects admissions, jobs, education news, research records, and exam-question practice content.
- Stores crawl records for human review before publishing public entities.
- Provides public FastAPI endpoints for search, entity pages, practice questions, and test series.
- Runs production workers, Redis queues, PostgreSQL, MinIO, and Airflow schedules through Docker Compose.

## System requirements (VPS)

- Ubuntu 22.04 or Ubuntu 24.04
- 4 vCPU
- 8GB RAM
- 80GB SSD
- Docker 24+
- A domain name pointed at the VPS
- Ports 80 and 443 open to the internet

## Step 1 — Prepare your VPS

```bash
sudo apt update
sudo apt upgrade -y
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker "$USER"
newgrp docker
docker run hello-world
```

## Step 2 — Clone the repo

```bash
cd /var/www
git clone https://github.com/Mariner-rider/ccdata.git
cd ccdata
```

## Step 3 — Create your environment file

```bash
cp .env.production.example .env
nano .env
```

Generate secrets:

```bash
openssl rand -hex 24   # POSTGRES_PASSWORD and MINIO_ROOT_PASSWORD
openssl rand -hex 32   # SERVICE_API_KEY
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"  # AIRFLOW_FERNET_KEY
```

Then secure the file:

```bash
chmod 600 .env
```

## Step 4 — Run database migrations

```bash
docker compose -f docker-compose.production.yml run --rm core-api \
  python -m services.lite_pipeline.main db:migrate

docker compose -f docker-compose.production.yml run --rm core-api \
  python -m services.lite_pipeline.main db:status
```

## Step 5 — Start all services

```bash
docker compose -f docker-compose.production.yml up -d --build
make ps
```

If anything shows `Exit` or `Restarting`, inspect logs:

```bash
make logs
```

## Step 6 — Configure Nginx

Create `/etc/nginx/sites-available/collegecue-data`:

```nginx
server {
    listen 80;
    server_name yourdomain.com;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_hide_header X-Powered-By;
        proxy_hide_header Server;
    }
}
```

Enable it:

```bash
sudo ln -s /etc/nginx/sites-available/collegecue-data /etc/nginx/sites-enabled/collegecue-data
sudo nginx -t
sudo systemctl reload nginx
```

## Step 7 — Add SSL with Certbot

```bash
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d yourdomain.com
sudo certbot renew --dry-run
```

## Step 8 — Add your first institution

```bash
docker compose -f docker-compose.production.yml exec core-api \
  python -m services.lite_pipeline.main source:add \
  --entity-type college \
  --entity-name "Example College" \
  --url https://example.edu \
  --trust-tier official

docker compose -f docker-compose.production.yml exec core-api \
  python -m services.lite_pipeline.main source:preview --id 1

docker compose -f docker-compose.production.yml exec core-api \
  python -m services.lite_pipeline.main source:crawl --id 1

docker compose -f docker-compose.production.yml exec core-api \
  python -m services.lite_pipeline.main review:list

docker compose -f docker-compose.production.yml exec core-api \
  python -m services.lite_pipeline.main review:approve --id 1 --reviewed-by admin

curl https://yourdomain.com/public/entities
```

## Step 9 — Bulk add institutions

CSV format:

```csv
entity_type,entity_name,url,trust_tier
college,Example College,https://example.edu,official
university,Example University,https://university.example,official
```

Command:

```bash
docker compose -f docker-compose.production.yml exec core-api \
  python -m services.lite_pipeline.main source:add-bulk --file institutions.csv
```

## Step 10 — Crawl exam questions

```bash
docker compose -f docker-compose.production.yml exec core-api \
  python -m services.lite_pipeline.main test:crawl --exam-type JEE

docker compose -f docker-compose.production.yml exec core-api \
  python -m services.lite_pipeline.main test:stats

docker compose -f docker-compose.production.yml exec core-api \
  python -m services.lite_pipeline.main test:create \
  --name "JEE Mock 1" --exam-type JEE --duration 180 \
  --easy 30 --medium 40 --hard 20
```

## Daily operations

```bash
make logs
make ps
make restart
make stop

docker compose -f docker-compose.production.yml exec core-api \
  python -m services.lite_pipeline.main metrics:summary

docker compose -f docker-compose.production.yml exec core-api \
  python -m services.lite_pipeline.main sources:freshness

docker compose -f docker-compose.production.yml exec core-api \
  python -m services.lite_pipeline.main jobs:failures
```

## Updating

```bash
git pull
docker compose -f docker-compose.production.yml up -d --build
docker compose -f docker-compose.production.yml exec core-api \
  python -m services.lite_pipeline.main db:migrate
```

## Public API endpoints

- `GET /health`
- `GET /public/entities`
- `GET /public/entities/{slug}`
- `GET /search?q=`
- `GET /practice?exam=&difficulty=&count=`
- `GET /tests`
- `GET /tests/{id}/questions`

## Admin API endpoints (X-API-Key required)

- `POST /sources/{id}/crawl`
- `GET /jobs/{id}`
- `GET /admin/review`
- `POST /admin/review/{id}/approve`
- `POST /admin/review/{id}/reject`
- `POST /admin/tests`
- `POST /admin/crawl/questions`

## Architecture

```text
Institution website
        ↓
    Crawl4AI
        ↓
  crawl_records
        ↓
  Human review
        ↓
 public_entities
        ↓
     FastAPI
        ↓
    Frontend
```

## Troubleshooting

- Port 8000 not responding: run `docker compose -f docker-compose.production.yml ps`, confirm `core-api` is healthy, then inspect `docker compose -f docker-compose.production.yml logs core-api`.
- DB connection refused: confirm `postgres` is healthy, verify `POSTGRES_PASSWORD` in `.env`, and run `db:status` from the `core-api` container.
- Crawl returns empty: run `source:preview`, check robots restrictions, increase `CRAWL_TIMEOUT_SECONDS`, and verify the site is reachable from the VPS.
- Airflow not starting: inspect `airflow-init` logs, verify `AIRFLOW_FERNET_KEY`, and rerun `docker compose -f docker-compose.production.yml up -d airflow-init airflow-scheduler`.
- Out of disk space: run `docker system df`, prune unused images with `docker system prune`, and rotate large logs under `/var/lib/docker/containers`.

## CI (disabled — ready to activate)

The CI skeleton is at `.github/workflows/ci.yml.disabled`. To activate it, rename it to `.github/workflows/ci.yml` and add repository secrets for `SERVICE_API_KEY`, `POSTGRES_PASSWORD`, `MINIO_ROOT_PASSWORD`, and `AIRFLOW_FERNET_KEY`.
