install:
	pip install -r requirements.txt

install-dev:
	pip install -r requirements-dev.txt

test:
	pytest -q

validate-no-docker:
	python scripts/validate_no_docker.py

init-db:
	RUNTIME_PROFILE=no-docker DATABASE_URL=sqlite:///./collegecue_local.db python -m services.lite_pipeline.main init-db

crawl-fixture:
	RUNTIME_PROFILE=no-docker DATABASE_URL=sqlite:///./collegecue_local.db QUEUE_BACKEND=memory WEBCLAW_ENABLED=false python -m services.lite_pipeline.main crawl:single --url file://tests/fixtures/college_sample.html
