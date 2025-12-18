# Unified Signal Monitoring and Portfolio Context Platform

Milestone 1 delivers the ingestion foundation: project skeleton, pip-tools environment, Prefect flows for prices (Stooq) and fundamentals (SEC), and Docker/Make targets.

## Layout
- `src/` — Python packages (`config`, `data_sources`, `flows`, `logging`, `utils`)
- `data/raw/`, `data/curated/`, `data/marts/` — data lake layers (raw is gitignored)
- `config/universe/sp100.csv` — default universe list
- `great_expectations/` — placeholder for future validation suites
- `dash_app/` — placeholder Dash app

## Setup
```bash
python3 -m pip install --upgrade pip
python3 -m pip install pip-tools
pip-sync requirements.txt
```

## Environment
Copy `.env.example` to `.env` and set:
- `ALPHAVANTAGE_API_KEY` (optional; Stooq default)
- `SEC_EDGAR_USER_AGENT` (required format: \"Name Surname email@example.com\")
- `DATA_ROOT`, `RAW_PRICES_DIR`, `RAW_FUNDAMENTALS_DIR` as needed

## Commands
- `make install` — install pinned deps with pip-tools
- `make lock` — regenerate `requirements.txt` from `requirements.in`
- `RUN_DATE=2024-12-01 make ingest-daily` — run price + fundamentals flows (uses `PYTHONPATH=src`)
- `make run-dash` — start placeholder Dash server (Milestone 4 will replace)

You can also call flows directly:
```bash
PYTHONPATH=src python -m flows.ingest_prices --run-date 2024-12-01
PYTHONPATH=src python -m flows.ingest_fundamentals --run-date 2024-12-01
```

## Data conventions
- Raw prices: `data/raw/prices_stooq/YYYY/MM/DD/{ticker}.parquet`
- Raw fundamentals manifest: `data/raw/fundamentals_sec/YYYY/Qx/{ticker}.parquet` plus downloaded filings under the same partition

## Docker
```
docker build -t unified-signal .
docker run --rm unified-signal
```

