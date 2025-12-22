from __future__ import annotations

import argparse
import sys
from typing import Iterable, List, Optional

import pandas as pd
from prefect import flow, task

from config.settings import get_settings
from data_sources.stooq import fetch_stooq_prices
from logging_utils.setup import configure_logging, logger
from utils.dates import parse_run_date


def load_universe(path: str) -> List[str]:
    df = pd.read_csv(path)
    return df["ticker"].dropna().unique().tolist()


@task
def fetch_and_store_price(ticker: str, run_date_str: str) -> None:
    run_dt = parse_run_date(run_date_str)
    fetch_stooq_prices(ticker, run_dt)


@flow(name="ingest_prices")
def ingest_prices(run_date: Optional[str] = None, tickers: Optional[Iterable[str]] = None) -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    run_date_str = run_date or parse_run_date().strftime("%Y-%m-%d")
    universe = list(tickers or load_universe(settings.universe_file))

    logger.info(f"Starting price ingestion for {len(universe)} tickers on {run_date_str}")
    for ticker in universe:
        fetch_and_store_price.submit(ticker=ticker, run_date_str=run_date_str)


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Ingest daily prices from Stooq.")
    parser.add_argument("--run-date", dest="run_date", help="Run date YYYY-MM-DD", required=False)
    args = parser.parse_args(argv)
    ingest_prices(run_date=args.run_date)


if __name__ == "__main__":
    main(sys.argv[1:])

