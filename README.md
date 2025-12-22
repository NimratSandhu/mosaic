# Unified Signal Monitoring and Portfolio Context Platform

Milestone 1 delivers the ingestion foundation: project skeleton, pip-tools environment, Prefect flows for prices (Stooq) and fundamentals (SEC), and Docker/Make targets.

## Layout
- `src/` — Python packages (`config`, `data_sources`, `flows`, `logging_utils`, `utils`)
- `data/raw/`, `data/curated/`, `data/marts/` — data lake layers (raw is gitignored)
- `config/universe/sp100.csv` — default universe list
- `great_expectations/` — placeholder for future validation suites
- `dash_app/` — placeholder Dash app

## Setup (Local Development)

**Prerequisites:** Python 3.11+ (3.11 recommended for prebuilt pyarrow wheels)

```bash
# 1. Create virtual environment
python3.11 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 2. Install dependencies
make install
# Or manually:
# python -m pip install --upgrade pip pip-tools
# pip-sync requirements.txt

# 3. Configure environment
cp .env.example .env
# Edit .env and set:
# - SEC_EDGAR_USER_AGENT="YourName your.email@example.com" (REQUIRED)
# - ALPHAVANTAGE_API_KEY (optional, Stooq is default)
```

## Running Flows (Local, No Docker Required)

You can run flows locally without Docker. Two options:

### Option 1: Using Make (Recommended)
```bash
# Run both price and fundamentals ingestion for a specific date
RUN_DATE=2024-12-01 make ingest-daily

# Run for today (default)
make ingest-daily

# Run individual flows
PYTHONPATH=src make ingest-prices  # Not in Makefile yet, use Option 2
```

### Option 2: Direct Python Execution
```bash
# Make sure venv is activated and PYTHONPATH includes src/
source venv/bin/activate

# Run price ingestion
PYTHONPATH=src python -m flows.ingest_prices --run-date 2024-12-01
# Or use today's date (default)
PYTHONPATH=src python -m flows.ingest_prices

# Run fundamentals ingestion
PYTHONPATH=src python -m flows.ingest_fundamentals --run-date 2024-12-01
```

## Running with Docker

Docker is **optional** for local development but useful for consistent environments:

```bash
# Build the image
docker build -t unified-signal .

# Run price ingestion (default CMD)
docker run --rm \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/.env:/app/.env \
  unified-signal

# Run fundamentals ingestion
docker run --rm \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/.env:/app/.env \
  unified-signal python -m flows.ingest_fundamentals --run-date 2024-12-01

# Interactive shell for debugging
docker run --rm -it \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/.env:/app/.env \
  unified-signal /bin/bash
```

**Note:** Mount `data/` as a volume so ingested data persists outside the container.

## Other Commands
- `make install` — install pinned deps with pip-tools
- `make lock` — regenerate `requirements.txt` from `requirements.in`
- `make run-dash` — start placeholder Dash server (Milestone 4 will replace)

## Data conventions
- Raw prices: `data/raw/prices_stooq/YYYY/MM/DD/{ticker}.parquet`
- Raw fundamentals manifest: `data/raw/fundamentals_sec/YYYY/Qx/{ticker}.parquet` plus downloaded filings under the same partition

## Docker
```
docker build -t unified-signal .
docker run --rm unified-signal
```

