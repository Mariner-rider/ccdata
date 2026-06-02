install:
	pip install -e .

install-dev:
	pip install -e ".[dev,postgres,redis,crawler]"

test:
	pytest -q

validate-no-docker:
	python scripts/validate_no_docker.py

init-db:
	RUNTIME_PROFILE=no-docker DATABASE_URL=sqlite:///./collegecue_local.db python -m services.lite_pipeline.main init-db

crawl-fixture:
	RUNTIME_PROFILE=no-docker DATABASE_URL=sqlite:///./collegecue_local.db QUEUE_BACKEND=memory python -m services.lite_pipeline.main crawl:single --url file://tests/fixtures/college_sample.html
