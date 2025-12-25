PYTHON ?= python3
PIP_SYNC ?= pip-sync
PIP_COMPILE ?= pip-compile
DOCKER ?= docker
DOCKER_COMPOSE ?= docker-compose

ENV_FILE ?= .env

.PHONY: install lock ingest-daily curate validate build-features run-dash fmt lint build docker-build docker-up docker-down docker-ingest docker-dash sync-to-gcs sync-from-gcs deploy-cloud-run

install:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install pip-tools
	$(PYTHON) -m pip install -r requirements.txt 'griffe<1.0'

lock:
	$(PIP_COMPILE) --generate-hashes --output-file=requirements.txt requirements.in

ingest-daily:
	PYTHONPATH=src $(PYTHON) -m flows.ingest_prices --run-date $${RUN_DATE:-$$(date +%Y-%m-%d)}
	PYTHONPATH=src $(PYTHON) -m flows.ingest_fundamentals --run-date $${RUN_DATE:-$$(date +%Y-%m-%d)}

curate:
	PYTHONPATH=src $(PYTHON) -m flows.curate_data --run-date $${RUN_DATE:-$$(date +%Y-%m-%d)}

validate:
	PYTHONPATH=src $(PYTHON) -m flows.curate_data --run-date $${RUN_DATE:-$$(date +%Y-%m-%d)}

query-db:
	PYTHONPATH=src $(PYTHON) -m db.query_db --list-tables

backfill:
	PYTHONPATH=src $(PYTHON) scripts/backfill_data.py --target-date $${TARGET_DATE:-$$(date +%Y-%m-%d)} --run-features

backfill-until:
	@if [ -z "$(END_DATE)" ]; then \
		echo "Error: END_DATE is required"; \
		echo "Usage: make backfill-until END_DATE=2024-12-01"; \
		echo "Optional: TARGET_DATE=2024-01-01 (defaults to END_DATE - 120 days)"; \
		exit 1; \
	fi
	@TARGET=$${TARGET_DATE:-$$(python3 -c "from datetime import date, timedelta; print((date.fromisoformat('$(END_DATE)') - timedelta(days=120)).isoformat())")}; \
	PYTHONPATH=src $(PYTHON) scripts/backfill_data.py --target-date $$TARGET --end-date $(END_DATE) --run-features

build-features:
	PYTHONPATH=src $(PYTHON) -m flows.build_features --run-date $${RUN_DATE:-$$(date +%Y-%m-%d)}

run-dash:
	PYTHONPATH=src:./ $(PYTHON) -m dash_app.app

# Docker commands
build: docker-build

docker-build:
	$(DOCKER) build -t unified-signal-platform:latest .

docker-up:
	$(DOCKER_COMPOSE) up -d mosaic

docker-down:
	$(DOCKER_COMPOSE) down

docker-ingest:
	RUN_DATE=$${RUN_DATE:-$$(date +%Y-%m-%d)} $(DOCKER_COMPOSE) run --rm ingest

docker-dash:
	$(DOCKER_COMPOSE) up mosaic

# GCS sync commands
sync-to-gcs:
	PYTHONPATH=src $(PYTHON) scripts/sync_to_gcs.py

sync-from-gcs:
	PYTHONPATH=src $(PYTHON) scripts/sync_from_gcs.py

# Cloud Run deployment
deploy-cloud-run:
	@if [ -z "$(GOOGLE_CLOUD_PROJECT)" ] && [ -z "$(1)" ]; then \
		echo "Error: Set GOOGLE_CLOUD_PROJECT or provide project ID as argument"; \
		echo "Usage: make deploy-cloud-run PROJECT_ID=my-project BUCKET=my-bucket"; \
		exit 1; \
	fi
	./cloud-run-deploy.sh $(or $(GOOGLE_CLOUD_PROJECT),$(1)) $(or $(REGION),us-central1) $(or $(GCS_BUCKET_NAME),$(2))

# Combined commands for easy deployment
deploy: docker-build docker-up
	@echo "Platform deployed! Dashboard available at http://localhost:8050"

# Local development (non-Docker)
dev-ingest: ingest-daily
dev-curate: curate
dev-features: build-features
dev-dash: run-dash

fmt:
	$(PYTHON) -m pip install ruff
	ruff format src

lint:
	$(PYTHON) -m pip install ruff
	ruff check src

