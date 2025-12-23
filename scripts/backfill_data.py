#!/usr/bin/env python3
"""
Backfill script to fetch and process historical price data.

This script:
1. Runs ingestion for a target date (fetches 120 days of history)
2. Finds all dates in the raw data
3. Runs curation for each date found
4. Optionally runs feature building
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config.settings import get_settings
from flows.curate_data import curate_data
from flows.ingest_prices import ingest_prices
from logging_utils.setup import configure_logging, logger
from utils.dates import date_partition


def find_available_dates(raw_prices_dir: Path) -> list[date]:
    """Find all dates that have raw price data."""
    dates = set()
    
    # Look through all partition directories
    for year_dir in raw_prices_dir.iterdir():
        if not year_dir.is_dir():
            continue
        for month_dir in year_dir.iterdir():
            if not month_dir.is_dir():
                continue
            for day_dir in month_dir.iterdir():
                if not day_dir.is_dir():
                    continue
                
                # Check if there are any ticker files
                ticker_files = list(day_dir.glob("*.parquet"))
                if ticker_files:
                    # Parse date from directory structure: YYYY/MM/DD
                    try:
                        date_val = date(
                            int(year_dir.name),
                            int(month_dir.name),
                            int(day_dir.name)
                        )
                        dates.add(date_val)
                    except (ValueError, TypeError):
                        continue
    
    return sorted(dates)


def backfill_data(
    target_date: date | None = None,
    days_to_fetch: int = 120,
    run_curation: bool = True,
    run_features: bool = False,
) -> None:
    """
    Backfill historical data.
    
    Args:
        target_date: Date to use as reference (defaults to today)
        days_to_fetch: Number of days of history to fetch (default: 120)
        run_curation: Whether to run curation after ingestion (default: True)
        run_features: Whether to run feature building after curation (default: False)
    """
    if target_date is None:
        target_date = date.today()
    
    logger.info(f"Starting backfill for target date {target_date}")
    logger.info(f"Will fetch up to {days_to_fetch} days of historical data")
    
    settings = get_settings()
    
    # Step 1: Run ingestion for target date
    # This will fetch 120 days of history and save each date to its partition
    logger.info("=" * 60)
    logger.info("Step 1: Running ingestion")
    logger.info("=" * 60)
    ingest_prices(run_date=target_date.strftime("%Y-%m-%d"))
    
    # Step 2: Find all dates that were ingested
    logger.info("=" * 60)
    logger.info("Step 2: Finding available dates in raw data")
    logger.info("=" * 60)
    available_dates = find_available_dates(settings.raw_prices_dir)
    logger.info(f"Found {len(available_dates)} dates with raw price data")
    logger.info(f"Date range: {available_dates[0] if available_dates else 'N/A'} to {available_dates[-1] if available_dates else 'N/A'}")
    
    if not available_dates:
        logger.error("No raw price data found. Ingestion may have failed.")
        return
    
    # Step 3: Run curation for each date
    if run_curation:
        logger.info("=" * 60)
        logger.info("Step 3: Running curation for all dates")
        logger.info("=" * 60)
        
        for i, curate_date in enumerate(available_dates, 1):
            logger.info(f"Curating date {curate_date} ({i}/{len(available_dates)})")
            try:
                curate_data(run_date=curate_date.strftime("%Y-%m-%d"))
            except Exception as exc:
                logger.error(f"Failed to curate {curate_date}: {exc}")
                continue
        
        logger.info(f"Completed curation for {len(available_dates)} dates")
    
    # Step 4: Optionally run feature building
    if run_features:
        logger.info("=" * 60)
        logger.info("Step 4: Running feature building")
        logger.info("=" * 60)
        
        from flows.build_features import build_features
        
        # Only run for the most recent date that has enough history
        # Need at least 60 days for momentum feature
        if len(available_dates) >= 60:
            feature_date = available_dates[-1]  # Most recent date
            logger.info(f"Building features for {feature_date} (has {len(available_dates)} days of history)")
            build_features(run_date=feature_date.strftime("%Y-%m-%d"))
        else:
            logger.warning(
                f"Not enough historical data for features. "
                f"Have {len(available_dates)} days, need at least 60."
            )
    
    logger.info("=" * 60)
    logger.info("Backfill completed!")
    logger.info("=" * 60)


def main(argv: list[str] | None = None) -> None:
    """CLI entrypoint."""
    if argv is None:
        argv = sys.argv[1:]
    
    parser = argparse.ArgumentParser(description="Backfill historical price data")
    parser.add_argument(
        "--target-date",
        type=str,
        default=None,
        help="Target date to use as reference (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=120,
        help="Number of days of history to fetch (default: 120)",
    )
    parser.add_argument(
        "--skip-curation",
        action="store_true",
        help="Skip curation step",
    )
    parser.add_argument(
        "--run-features",
        action="store_true",
        help="Also run feature building after curation",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level (default: INFO)",
    )
    
    args = parser.parse_args(argv)
    
    configure_logging(log_level=args.log_level)
    
    target_date = None
    if args.target_date:
        target_date = date.fromisoformat(args.target_date)
    
    backfill_data(
        target_date=target_date,
        days_to_fetch=args.days,
        run_curation=not args.skip_curation,
        run_features=args.run_features,
    )


if __name__ == "__main__":
    main()

