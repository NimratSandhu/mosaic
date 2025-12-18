PYTHON ?= python3
PIP_SYNC ?= pip-sync
PIP_COMPILE ?= pip-compile

ENV_FILE ?= .env

.PHONY: install lock ingest-daily run-dash fmt lint

install:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install pip-tools
	$(PIP_SYNC) requirements.txt

lock:
	$(PIP_COMPILE) --generate-hashes --output-file=requirements.txt requirements.in

ingest-daily:
	PYTHONPATH=src $(PYTHON) -m flows.ingest_prices --run-date $${RUN_DATE:-$$(date +%Y-%m-%d)}
	PYTHONPATH=src $(PYTHON) -m flows.ingest_fundamentals --run-date $${RUN_DATE:-$$(date +%Y-%m-%d)}

run-dash:
	PYTHONPATH=src:./ $(PYTHON) -m dash_app.app

fmt:
	$(PYTHON) -m pip install ruff
	ruff format src

lint:
	$(PYTHON) -m pip install ruff
	ruff check src

