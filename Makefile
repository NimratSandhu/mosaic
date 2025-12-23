PYTHON ?= python3
PIP_SYNC ?= pip-sync
PIP_COMPILE ?= pip-compile
DOCKER ?= docker
DOCKER_COMPOSE ?= docker-compose

ENV_FILE ?= .env

.PHONY: install lock ingest-daily curate validate build-features run-dash fmt lint build docker-build docker-up docker-down docker-ingest docker-dash

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

