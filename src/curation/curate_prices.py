from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

from config.settings import get_settings
from logging_utils.setup import logger
from utils.dates import date_partition
from utils.paths import ensure_dir


class PriceCurationError(Exception):
    """Raised when price curation fails."""


def curate_daily_prices(run_date: date, raw_dir: Path | None = None, curated_dir: Path | None = None) -> pd.DataFrame:
    """
    Curate raw price data for a specific date.
    
    Reads all ticker Parquet files from raw layer for the given date,
    standardizes schema, handles missing values, deduplicates, and writes
    to curated layer as a single Parquet file.
    
    Args:
        run_date: Date to curate prices for
        raw_dir: Optional override for raw prices directory
        curated_dir: Optional override for curated directory
        
    Returns:
        DataFrame with curated prices for the date
    """
    settings = get_settings()
    raw_root = Path(raw_dir) if raw_dir else settings.raw_prices_dir
    curated_root = Path(curated_dir) if curated_dir else settings.curated_dir
    
    partition = date_partition(run_date)
    raw_date_dir = raw_root / partition
    
    if not raw_date_dir.exists():
        logger.warning(f"No raw price data found for {partition}")
        return pd.DataFrame()
    
    # Read all ticker files for this date
    ticker_files = list(raw_date_dir.glob("*.parquet"))
    if not ticker_files:
        logger.warning(f"No ticker files found in {raw_date_dir}")
        return pd.DataFrame()
    
    logger.info(f"Reading {len(ticker_files)} ticker files from {raw_date_dir}")
    
    dfs = []
    for ticker_file in ticker_files:
        try:
            df = pd.read_parquet(ticker_file)
            if not df.empty:
                dfs.append(df)
        except Exception as exc:
            logger.error(f"Failed to read {ticker_file}: {exc}")
            continue
    
    if not dfs:
        logger.warning(f"No valid price data found for {partition}")
        return pd.DataFrame()
    
    # Combine all tickers
    combined = pd.concat(dfs, ignore_index=True)
    logger.info(f"Combined {len(combined)} rows from {len(dfs)} tickers")
    
    # Standardize schema
    required_columns = ["date", "ticker", "open", "high", "low", "close", "volume", "source"]
    missing_cols = [col for col in required_columns if col not in combined.columns]
    if missing_cols:
        raise PriceCurationError(f"Missing required columns: {missing_cols}")
    
    # Ensure correct dtypes
    combined["date"] = pd.to_datetime(combined["date"]).dt.date
    combined["ticker"] = combined["ticker"].astype(str)
    combined["open"] = pd.to_numeric(combined["open"], errors="coerce")
    combined["high"] = pd.to_numeric(combined["high"], errors="coerce")
    combined["low"] = pd.to_numeric(combined["low"], errors="coerce")
    combined["close"] = pd.to_numeric(combined["close"], errors="coerce")
    combined["volume"] = pd.to_numeric(combined["volume"], errors="coerce").astype("Int64")  # Nullable int
    combined["source"] = combined["source"].astype(str)
    
    # Filter to target date or latest available date if exact match not found
    date_filtered = combined[combined["date"] == run_date].copy()
    
    if date_filtered.empty:
        # Use latest available date (similar to ingestion logic)
        if not combined.empty:
            latest_date = combined["date"].max()
            date_filtered = combined[combined["date"] == latest_date].copy()
            logger.warning(
                f"No exact match for {run_date}, using latest available date {latest_date} "
                f"({len(date_filtered)} rows)"
            )
        else:
            logger.warning(f"No price data found for date {run_date}")
            return pd.DataFrame()
    
    combined = date_filtered
    
    # Deduplicate: keep last occurrence of same ticker+date
    before_dedup = len(combined)
    combined = combined.drop_duplicates(subset=["ticker", "date"], keep="last")
    if len(combined) < before_dedup:
        logger.info(f"Removed {before_dedup - len(combined)} duplicate rows")
    
    # Sort by ticker for consistent output
    combined = combined.sort_values("ticker").reset_index(drop=True)
    
    # Write to curated layer
    curated_date_dir = curated_root / "daily_prices" / partition
    ensure_dir(curated_date_dir)
    output_path = curated_date_dir / f"{run_date:%Y-%m-%d}.parquet"
    
    combined.to_parquet(output_path, index=False)
    logger.info(f"Saved curated prices to {output_path} ({len(combined)} rows, {combined['ticker'].nunique()} tickers)")
    
    return combined


__all__ = ["curate_daily_prices", "PriceCurationError"]

