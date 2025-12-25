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
from flows.build_features import build_features
from logging_utils.setup import configure_logging, logger
from utils.dates import date_partition

try:
    from utils.storage import GCS_AVAILABLE, sync_marts_from_gcs
    if GCS_AVAILABLE:
        from google.cloud import storage
except ImportError:
    GCS_AVAILABLE = False


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


def find_existing_dates_local(marts_dir: Path) -> set[date]:
    """Find dates that already have signal scores or positions locally."""
    existing_dates = set()
    
    # Check signal_scores
    signal_scores_dir = marts_dir / "signal_scores"
    if signal_scores_dir.exists():
        for file_path in signal_scores_dir.glob("*.parquet"):
            try:
                # Parse date from filename: YYYY-MM-DD.parquet
                date_str = file_path.stem
                date_val = date.fromisoformat(date_str)
                existing_dates.add(date_val)
            except (ValueError, TypeError):
                continue
    
    # Check positions
    positions_dir = marts_dir / "positions"
    if positions_dir.exists():
        for file_path in positions_dir.glob("*.parquet"):
            try:
                # Parse date from filename: YYYY-MM-DD.parquet
                date_str = file_path.stem
                date_val = date.fromisoformat(date_str)
                existing_dates.add(date_val)
            except (ValueError, TypeError):
                continue
    
    return existing_dates


def find_existing_dates_gcs(
    bucket_name: str,
    gcs_prefix: str = "marts/",
    client=None,
) -> set[date]:
    """Find dates that already exist in GCS."""
    existing_dates = set()
    
    if not GCS_AVAILABLE:
        return existing_dates
    
    if client is None:
        client = storage.Client()
    
    bucket = client.bucket(bucket_name)
    
    try:
        if not bucket.exists():
            return existing_dates
    except Exception:
        return existing_dates
    
    # Check signal_scores
    signal_scores_prefix = f"{gcs_prefix}signal_scores/"
    for blob in bucket.list_blobs(prefix=signal_scores_prefix):
        if blob.name.endswith(".parquet"):
            try:
                # Extract filename: signal_scores/YYYY-MM-DD.parquet
                filename = blob.name.split("/")[-1]
                date_str = filename.replace(".parquet", "")
                date_val = date.fromisoformat(date_str)
                existing_dates.add(date_val)
            except (ValueError, TypeError):
                continue
    
    # Check positions
    positions_prefix = f"{gcs_prefix}positions/"
    for blob in bucket.list_blobs(prefix=positions_prefix):
        if blob.name.endswith(".parquet"):
            try:
                # Extract filename: positions/YYYY-MM-DD.parquet
                filename = blob.name.split("/")[-1]
                date_str = filename.replace(".parquet", "")
                date_val = date.fromisoformat(date_str)
                existing_dates.add(date_val)
            except (ValueError, TypeError):
                continue
    
    return existing_dates


def backfill_data(
    target_date: date | None = None,
    end_date: date | None = None,
    days_to_fetch: int = 120,
    run_curation: bool = True,
    run_features: bool = False,
    skip_existing: bool = True,
) -> None:
    """
    Backfill historical data until a specific date.
    
    Args:
        target_date: Starting date to use as reference (defaults to today)
        end_date: End date to backfill until (inclusive). If None, only processes target_date.
        days_to_fetch: Number of days of history to fetch per ingestion (default: 120)
        run_curation: Whether to run curation after ingestion (default: True)
        run_features: Whether to run feature building after curation (default: False)
        skip_existing: Whether to skip dates that already have data (default: True)
    """
    if target_date is None:
        target_date = date.today()
    
    if end_date is None:
        end_date = target_date
    
    # If end_date is before target_date, swap them
    if end_date < target_date:
        target_date, end_date = end_date, target_date
    
    logger.info(f"Starting backfill from {target_date} until {end_date} (inclusive)")
    logger.info(f"Will fetch up to {days_to_fetch} days of historical data per run")
    
    settings = get_settings()
    
    # Step 0: Check for existing dates (deduplication)
    existing_dates = set()
    if skip_existing:
        logger.info("=" * 60)
        logger.info("Step 0: Checking for existing dates (deduplication)")
        logger.info("=" * 60)
        
        # Check local
        existing_dates.update(find_existing_dates_local(settings.marts_dir))
        logger.info(f"Found {len(existing_dates)} existing dates locally")
        
        # Check GCS if enabled
        if GCS_AVAILABLE and settings.gcs_enabled and settings.gcs_bucket_name:
            try:
                gcs_dates = find_existing_dates_gcs(
                    bucket_name=settings.gcs_bucket_name,
                    gcs_prefix=settings.gcs_marts_prefix,
                )
                existing_dates.update(gcs_dates)
                logger.info(f"Found {len(gcs_dates)} existing dates in GCS")
            except Exception as e:
                logger.warning(f"Could not check GCS for existing dates: {e}")
        
        logger.info(f"Total existing dates: {len(existing_dates)}")
        if existing_dates:
            logger.info(f"Existing date range: {min(existing_dates)} to {max(existing_dates)}")
    
    # Step 1: Run ingestion for dates in range
    logger.info("=" * 60)
    logger.info("Step 1: Running ingestion")
    logger.info("=" * 60)
    
    # Generate list of dates to process (from end_date backwards to target_date)
    # Each ingestion fetches ~120 days, so we need to cover the range
    dates_to_ingest = []
    current_date = end_date
    while current_date >= target_date:
        dates_to_ingest.append(current_date)
        # Move back by days_to_fetch to avoid overlap
        current_date = current_date - timedelta(days=days_to_fetch)
    
    dates_to_ingest = sorted(dates_to_ingest)  # Process chronologically
    
    for i, ingest_date in enumerate(dates_to_ingest, 1):
        logger.info(f"Ingesting for date {ingest_date} ({i}/{len(dates_to_ingest)})")
        try:
            ingest_prices(run_date=ingest_date.strftime("%Y-%m-%d"))
        except Exception as exc:
            logger.error(f"Failed to ingest {ingest_date}: {exc}")
            continue
    
    # Step 2: Find all dates that were ingested
    logger.info("=" * 60)
    logger.info("Step 2: Finding available dates in raw data")
    logger.info("=" * 60)
    available_dates = find_available_dates(settings.raw_prices_dir)
    
    # Filter to only dates in our target range
    available_dates = [d for d in available_dates if target_date <= d <= end_date]
    
    logger.info(f"Found {len(available_dates)} dates with raw price data in range")
    logger.info(f"Date range: {available_dates[0] if available_dates else 'N/A'} to {available_dates[-1] if available_dates else 'N/A'}")
    
    if not available_dates:
        logger.error("No raw price data found in target range. Ingestion may have failed.")
        return
    
    # Filter out existing dates if deduplication is enabled
    if skip_existing:
        dates_to_process = [d for d in available_dates if d not in existing_dates]
        skipped_count = len(available_dates) - len(dates_to_process)
        if skipped_count > 0:
            logger.info(f"Skipping {skipped_count} dates that already have data")
        available_dates = dates_to_process
    
    if not available_dates:
        logger.info("All dates already have data. Nothing to process.")
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
        
        # Build features for each date that has enough history
        # Need at least 60 days for momentum feature
        for feature_date in available_dates:
            # Check if we have enough history for this date
            all_dates = find_available_dates(settings.raw_prices_dir)
            dates_before = [d for d in all_dates if d < feature_date]
            
            if len(dates_before) >= 60:
                logger.info(f"Building features for {feature_date} (has {len(dates_before)} days of history)")
                try:
                    build_features(run_date=feature_date.strftime("%Y-%m-%d"))
                except Exception as exc:
                    logger.error(f"Failed to build features for {feature_date}: {exc}")
                    continue
            else:
                logger.warning(
                    f"Skipping features for {feature_date}: "
                    f"only {len(dates_before)} days of history available, need at least 60."
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
        help="Starting date to use as reference (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date to backfill until (YYYY-MM-DD, inclusive). If not specified, only processes target-date.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=120,
        help="Number of days of history to fetch per ingestion run (default: 120)",
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
        "--no-skip-existing",
        action="store_true",
        help="Process dates even if they already have data (disable deduplication)",
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
    
    end_date = None
    if args.end_date:
        end_date = date.fromisoformat(args.end_date)
    
    backfill_data(
        target_date=target_date,
        end_date=end_date,
        days_to_fetch=args.days,
        run_curation=not args.skip_curation,
        run_features=args.run_features,
        skip_existing=not args.no_skip_existing,
    )


if __name__ == "__main__":
    main()

