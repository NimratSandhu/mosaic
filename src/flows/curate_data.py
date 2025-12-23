from __future__ import annotations

import argparse
import sys
from typing import Optional

from prefect import flow, task

from config.settings import get_settings
from curation.curate_fundamentals import curate_quarterly_fundamentals
from curation.curate_prices import curate_daily_prices
from curation.validate_prices import validate_daily_prices
from db.load_curated import load_curated_fundamentals_to_db, load_curated_prices_to_db
from logging_utils.setup import configure_logging, logger
from utils.dates import parse_run_date


@task
def curate_and_validate_prices(run_date_str: str) -> None:
    """Task to curate and validate daily prices."""
    run_dt = parse_run_date(run_date_str)
    settings = get_settings()
    
    logger.info(f"Starting price curation for {run_dt}")
    
    # Curate prices
    curated_df = curate_daily_prices(run_dt)
    
    if curated_df.empty:
        logger.warning(f"No price data to curate for {run_dt}")
        return
    
    # Validate curated data
    curated_file = (
        settings.curated_dir
        / "daily_prices"
        / f"{run_dt:%Y/%m/%d}"
        / f"{run_dt:%Y-%m-%d}.parquet"
    )
    
    validation_results = validate_daily_prices(curated_df, curated_file, fail_on_error=False)
    
    if not validation_results["valid"]:
        logger.error(f"Validation failed for {run_dt}: {validation_results['errors']}")
        # Don't fail the task, but log the errors
    
    # Load into DuckDB (append mode to accumulate historical data)
    load_curated_prices_to_db(run_dt, if_exists="append")
    
    logger.info(f"Completed price curation for {run_dt}")


@task
def curate_fundamentals(run_date_str: str) -> None:
    """Task to curate quarterly fundamentals."""
    run_dt = parse_run_date(run_date_str)
    settings = get_settings()
    
    logger.info(f"Starting fundamentals curation for {run_dt}")
    
    # Curate fundamentals
    curated_df = curate_quarterly_fundamentals(run_dt)
    
    if curated_df.empty:
        logger.warning(f"No fundamentals data to curate for {run_dt}")
        return
    
    # Load into DuckDB (append mode for fundamentals since multiple quarters)
    load_curated_fundamentals_to_db(run_dt, if_exists="append")
    
    logger.info(f"Completed fundamentals curation for {run_dt}")


@flow(name="curate_data")
def curate_data(run_date: Optional[str] = None) -> None:
    """
    Main curation flow that orchestrates price and fundamentals curation.
    
    Args:
        run_date: Date string in YYYY-MM-DD format. Defaults to today.
    """
    run_date_str = run_date or parse_run_date().strftime("%Y-%m-%d")
    
    logger.info(f"Starting curation flow for {run_date_str}")
    
    # Run price curation
    curate_and_validate_prices(run_date_str)
    
    # Run fundamentals curation
    curate_fundamentals(run_date_str)
    
    logger.info(f"Completed curation flow for {run_date_str}")


def main(argv: Optional[list[str]] = None) -> None:
    """CLI entrypoint for curation flow."""
    if argv is None:
        argv = sys.argv[1:]
    
    parser = argparse.ArgumentParser(description="Curate raw data into curated layer")
    parser.add_argument(
        "--run-date",
        type=str,
        default=None,
        help="Date to curate data for (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level (default: INFO)",
    )
    
    args = parser.parse_args(argv)
    
    configure_logging(log_level=args.log_level)
    
    curate_data(run_date=args.run_date)


if __name__ == "__main__":
    main()

