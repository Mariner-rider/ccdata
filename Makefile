COMPOSE_PROD=docker compose -f docker-compose.production.yml
COMPOSE_LITE=docker compose -f docker-compose.local-lite.yml
PYTHON?=python

install:
	pip install -e .

install-dev:
	pip install -e ".[dev,webclaw,postgres,redis]"

test:
	pytest -q

lint:
	ruff check services/ tests/

compile:
	$(PYTHON) -m py_compile services/lite_pipeline/main.py

ps:
	$(COMPOSE_PROD) ps

logs:
	$(COMPOSE_PROD) logs -f --tail=200

restart:
	$(COMPOSE_PROD) up -d --build

stop:
	$(COMPOSE_PROD) down

deploy:
	$(COMPOSE_PROD) up -d --build

storage-status:
	$(COMPOSE_PROD) exec minio mc du local/raw-html || true

storage-cleanup:
	$(PYTHON) -m services.lite_pipeline.main storage:cleanup || true

docker-size-report:
	$(PYTHON) scripts/docker_size_report.py

local-config:
	$(COMPOSE_LITE) config

production-config:
	$(COMPOSE_PROD) config
