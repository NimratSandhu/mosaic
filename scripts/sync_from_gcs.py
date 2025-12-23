#!/usr/bin/env python3
"""
Sync marts data from Google Cloud Storage.

Downloads signal_scores, positions, and DuckDB database from GCS bucket.
Used by Cloud Run container on startup to get latest data.

Usage:
    python scripts/sync_from_gcs.py
    python scripts/sync_from_gcs.py --bucket my-bucket --prefix custom/
"""

import argparse
import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config.settings import get_settings
from loguru import logger
from utils.storage import GCS_AVAILABLE, sync_marts_from_gcs


def main() -> None:
    """Sync marts data from GCS."""
    parser = argparse.ArgumentParser(
        description="Sync marts data from Google Cloud Storage"
    )
    parser.add_argument(
        "--bucket",
        help="GCS bucket name (overrides GCS_BUCKET_NAME env var)",
    )
    parser.add_argument(
        "--prefix",
        default="marts/",
        help="GCS path prefix (default: marts/)",
    )
    parser.add_argument(
        "--marts-dir",
        help="Local marts directory (overrides settings)",
    )
    parser.add_argument(
        "--fail-if-empty",
        action="store_true",
        help="Exit with error if no data found in GCS",
    )
    
    args = parser.parse_args()
    
    if not GCS_AVAILABLE:
        logger.warning("google-cloud-storage is not installed. Skipping GCS sync.")
        # Don't fail in case this is run in environments without GCS
        return
    
    settings = get_settings()
    
    # Check if GCS is enabled
    if not settings.gcs_enabled:
        logger.info("GCS sync is disabled (GCS_ENABLED=false). Skipping.")
        return
    
    bucket_name = args.bucket or settings.gcs_bucket_name
    if not bucket_name:
        logger.warning("GCS bucket name not specified. Skipping sync.")
        return
    
    marts_dir = Path(args.marts_dir) if args.marts_dir else settings.marts_dir
    marts_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Syncing marts data from gs://{bucket_name}/{args.prefix}")
    logger.info(f"Destination: {marts_dir}")
    
    try:
        sync_marts_from_gcs(
            bucket_name=bucket_name,
            local_marts_dir=marts_dir,
            gcs_prefix=args.prefix,
        )
        logger.info("✅ Successfully synced marts data from GCS")
    except FileNotFoundError as e:
        if args.fail_if_empty:
            logger.error(f"❌ No data found in GCS: {e}")
            sys.exit(1)
        else:
            logger.warning(f"No data found in GCS: {e}")
            logger.info("Dashboard will start with empty data. Run pipeline and sync to populate.")
    except Exception as e:
        logger.error(f"❌ Failed to sync from GCS: {e}")
        if args.fail_if_empty:
            sys.exit(1)
        # In production, we might want to continue even if sync fails
        # so the dashboard can still start (just with no data)
        logger.warning("Continuing despite sync failure...")


if __name__ == "__main__":
    main()

