from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

from config.settings import get_settings
from logging_utils.setup import logger
from utils.paths import ensure_dir


class FundamentalsCurationError(Exception):
    """Raised when fundamentals curation fails."""


def _quarter_from_date(dt: date) -> str:
    """Return quarter string like Q1, Q2, Q3, Q4."""
    quarter = (dt.month - 1) // 3 + 1
    return f"Q{quarter}"


def curate_quarterly_fundamentals(
    run_date: date, raw_dir: Path | None = None, curated_dir: Path | None = None
) -> pd.DataFrame:
    """
    Curate raw fundamentals manifest data for a specific quarter.
    
    Reads all ticker manifest Parquet files from raw layer for the given quarter,
    standardizes schema, and writes to curated layer as a single Parquet file.
    
    Args:
        run_date: Date to determine quarter for curation
        raw_dir: Optional override for raw fundamentals directory
        curated_dir: Optional override for curated directory
        
    Returns:
        DataFrame with curated fundamentals manifests for the quarter
    """
    settings = get_settings()
    raw_root = Path(raw_dir) if raw_dir else settings.raw_fundamentals_dir
    curated_root = Path(curated_dir) if curated_dir else settings.curated_dir
    
    year = f"{run_date:%Y}"
    quarter = _quarter_from_date(run_date)
    raw_quarter_dir = raw_root / year / quarter
    
    if not raw_quarter_dir.exists():
        logger.warning(f"No raw fundamentals data found for {year}/{quarter}")
        return pd.DataFrame()
    
    # Read all ticker manifest files for this quarter
    manifest_files = [f for f in raw_quarter_dir.glob("*.parquet") if f.name != "manifest.parquet"]
    if not manifest_files:
        logger.warning(f"No manifest files found in {raw_quarter_dir}")
        return pd.DataFrame()
    
    logger.info(f"Reading {len(manifest_files)} manifest files from {raw_quarter_dir}")
    
    dfs = []
    for manifest_file in manifest_files:
        try:
            df = pd.read_parquet(manifest_file)
            if not df.empty:
                dfs.append(df)
        except Exception as exc:
            logger.error(f"Failed to read {manifest_file}: {exc}")
            continue
    
    if not dfs:
        logger.warning(f"No valid fundamentals data found for {year}/{quarter}")
        return pd.DataFrame()
    
    # Combine all tickers
    combined = pd.concat(dfs, ignore_index=True)
    logger.info(f"Combined {len(combined)} rows from {len(dfs)} tickers")
    
    # Standardize schema
    required_columns = ["ticker", "filing_type", "download_time", "file_path", "source"]
    missing_cols = [col for col in required_columns if col not in combined.columns]
    if missing_cols:
        raise FundamentalsCurationError(f"Missing required columns: {missing_cols}")
    
    # Ensure correct dtypes
    combined["ticker"] = combined["ticker"].astype(str)
    combined["filing_type"] = combined["filing_type"].astype(str)
    combined["download_time"] = pd.to_datetime(combined["download_time"])
    combined["file_path"] = combined["file_path"].astype(str)
    combined["source"] = combined["source"].astype(str)
    
    # Filter out empty manifests (file_path is empty or NaN)
    before_filter = len(combined)
    combined = combined[combined["file_path"].notna() & (combined["file_path"] != "")].copy()
    if len(combined) < before_filter:
        logger.info(f"Filtered out {before_filter - len(combined)} empty manifests")
    
    if combined.empty:
        logger.warning(f"No valid fundamentals data found for {year}/{quarter}")
        return pd.DataFrame()
    
    # Sort by ticker, then filing_type for consistent output
    combined = combined.sort_values(["ticker", "filing_type"]).reset_index(drop=True)
    
    # Write to curated layer
    curated_quarter_dir = curated_root / "quarterly_fundamentals" / year / quarter
    ensure_dir(curated_quarter_dir)
    output_path = curated_quarter_dir / f"{year}_{quarter}.parquet"
    
    combined.to_parquet(output_path, index=False)
    logger.info(
        f"Saved curated fundamentals to {output_path} "
        f"({len(combined)} rows, {combined['ticker'].nunique()} tickers)"
    )
    
    return combined


__all__ = ["curate_quarterly_fundamentals", "FundamentalsCurationError"]

