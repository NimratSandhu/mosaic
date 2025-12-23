# Unified Signal Monitoring and Portfolio Context Platform

Milestone 1 delivers the ingestion foundation: project skeleton, pip-tools environment, Prefect flows for prices (Stooq) and fundamentals (SEC), and Docker/Make targets.

Milestone 2 adds data curation: ETL transforms raw data into curated Parquet tables, Great Expectations validation, and DuckDB integration for analytical queries.

Milestone 3 implements the feature engine: calculates price features (20d realized vol, 60d momentum, 5d mean reversion Z-score), fundamental features (YoY revenue growth proxies), signal scoring (Z-score normalization), and position generation (top N longs, bottom N shorts).

Milestone 4 delivers the dashboard: two-page Dash application with Market Overview (sector filters, top long/short candidate tables, sector exposure charts) and Single Name Deep Dive (price charts with feature indicators, signal breakdown tables).

Milestone 4 delivers the dashboard: two-page Dash application with Market Overview (sector filters, top long/short tables, sector exposure charts) and Single Name Deep Dive (price charts with feature indicators, signal breakdown tables).

## Layout
- `src/` — Python packages (`config`, `data_sources`, `flows`, `curation`, `db`, `features`, `logging_utils`, `utils`)
- `data/raw/`, `data/curated/`, `data/marts/` — data lake layers (raw is gitignored)
- `config/universe/sp100.csv` — default universe list
- `great_expectations/` — Great Expectations validation suites
- `dash_app/` — Dash application with Market Overview and Single Name Deep Dive pages
- `src/dashboard/` — Dashboard data access layer

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

# Curate raw data into curated layer (includes validation and DuckDB loading)
RUN_DATE=2024-12-01 make curate

# Run curation for today (default)
make curate

# Build features, signals, and positions
RUN_DATE=2024-12-01 make build-features

# Run feature engine for today (default)
make build-features
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

# Run curation flow (curates prices and fundamentals, validates, loads to DuckDB)
PYTHONPATH=src python -m flows.curate_data --run-date 2024-12-01

# Run feature engine flow (calculates features, scores signals, generates positions)
PYTHONPATH=src python -m flows.build_features --run-date 2024-12-01

# Run dashboard
PYTHONPATH=src:./ python -m dash_app.app
# Or use make:
make run-dash
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
- `make curate` — curate raw data (ETL, validate, load to DuckDB)
- `make validate` — alias for `make curate` (runs validation as part of curation)
- `make build-features` — build features, signals, and positions (Milestone 3)
- `make query-db` — list tables in DuckDB database
- `make run-dash` — start Dash server (Market Overview and Single Name Deep Dive pages)

## Data Conventions

### Raw Layer
- Raw prices: `data/raw/prices_stooq/YYYY/MM/DD/{ticker}.parquet`
- Raw fundamentals manifest: `data/raw/fundamentals_sec/YYYY/Qx/{ticker}.parquet` plus downloaded filings under the same partition

### Curated Layer
- Curated daily prices: `data/curated/daily_prices/YYYY/MM/DD/YYYY-MM-DD.parquet` (one file per day with all tickers)
- Curated quarterly fundamentals: `data/curated/quarterly_fundamentals/YYYY/Qx/YYYY_Qx.parquet` (one file per quarter)

### DuckDB Database
- Database file: `data/marts/duckdb/mosaic.duckdb`
- Tables: 
  - `curated.daily_prices`, `curated.quarterly_fundamentals`
  - `marts.signal_scores`, `marts.positions`

### Marts Layer
- Signal scores: `data/marts/signal_scores/YYYY-MM-DD.parquet` (normalized Z-scores per ticker)
- Positions: `data/marts/positions/YYYY-MM-DD.parquet` (top N longs, bottom N shorts)

## Docker
```
docker build -t unified-signal .
docker run --rm unified-signal
```

