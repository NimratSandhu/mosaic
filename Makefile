PYTHON ?= python3
PIP_SYNC ?= pip-sync
PIP_COMPILE ?= pip-compile

ENV_FILE ?= .env

.PHONY: install lock ingest-daily curate validate build-features run-dash fmt lint

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

fmt:
	$(PYTHON) -m pip install ruff
	ruff format src

lint:
	$(PYTHON) -m pip install ruff
	ruff check src

