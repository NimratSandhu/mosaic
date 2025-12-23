#!/usr/bin/env python3
"""
Sync marts data to Google Cloud Storage.

Uploads signal_scores, positions, and DuckDB database to GCS bucket.
Can be run after pipeline execution to sync results to cloud.

Usage:
    python scripts/sync_to_gcs.py
    python scripts/sync_to_gcs.py --bucket my-bucket --prefix custom/
"""

import argparse
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config.settings import get_settings
from loguru import logger
from utils.storage import GCS_AVAILABLE, sync_marts_to_gcs


def main() -> None:
    """Sync marts data to GCS."""
    parser = argparse.ArgumentParser(
        description="Sync marts data to Google Cloud Storage"
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
    
    args = parser.parse_args()
    
    if not GCS_AVAILABLE:
        logger.error("google-cloud-storage is not installed. Install it with: pip install google-cloud-storage")
        sys.exit(1)
    
    settings = get_settings()
    
    bucket_name = args.bucket or settings.gcs_bucket_name
    if not bucket_name:
        logger.error("GCS bucket name not specified. Set GCS_BUCKET_NAME env var or use --bucket")
        sys.exit(1)
    
    marts_dir = Path(args.marts_dir) if args.marts_dir else settings.marts_dir
    
    if not marts_dir.exists():
        logger.warning(f"Marts directory does not exist: {marts_dir}")
        logger.info("Nothing to sync. Run the pipeline first to generate data.")
        sys.exit(0)
    
    logger.info(f"Syncing marts data to gs://{bucket_name}/{args.prefix}")
    logger.info(f"Source: {marts_dir}")
    
    try:
        sync_marts_to_gcs(
            bucket_name=bucket_name,
            local_marts_dir=marts_dir,
            gcs_prefix=args.prefix,
        )
        logger.info("✅ Successfully synced marts data to GCS")
    except Exception as e:
        logger.error(f"❌ Failed to sync to GCS: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

