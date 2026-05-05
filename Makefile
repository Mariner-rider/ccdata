install:
	pip install -r requirements.txt

install-dev:
	pip install -r requirements-dev.txt

test:
	pytest -q

lint:
	python -m compileall services tests

compile:
	python -m compileall services tests

docker-build-lite:
	docker compose -f docker-compose.local-lite.yml build

docker-up-lite:
	docker compose -f docker-compose.local-lite.yml up --build

docker-down-lite:
	docker compose -f docker-compose.local-lite.yml down -v

docker-size-report:
	python scripts/docker_size_report.py

validate-lite:
	python scripts/validate_local_lite.py

crawl-single:
	python -m services.lite_pipeline.main crawl:single --url $(URL)

storage-cleanup:
	python -m services.lite_pipeline.main storage:cleanup

storage-status:
	python -m services.lite_pipeline.main storage:status
