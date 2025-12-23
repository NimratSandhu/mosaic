from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from pandas_datareader import data as web
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from config.settings import get_settings
from logging_utils.setup import logger
from utils.dates import date_partition
from utils.paths import ensure_dir


class StooqFetchError(Exception):
    """Raised when Stooq fetch fails after retries."""


@retry(
    reraise=True,
    retry=retry_if_exception_type(Exception),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
)
def _fetch_stooq_window(ticker: str, start: date, end: date) -> pd.DataFrame:
    return web.DataReader(ticker, "stooq", start=start, end=end)


def fetch_stooq_prices(ticker: str, run_date: date, raw_dir: Path | None = None) -> pd.DataFrame:
    """Fetch daily price data for a ticker from Stooq and persist to raw Parquet."""
    settings = get_settings()
    target_dir = Path(raw_dir) if raw_dir else settings.raw_prices_dir
    window_start = run_date - timedelta(days=120)

    try:
        df = _fetch_stooq_window(ticker, window_start, run_date)
    except Exception as exc:  # pragma: no cover - network/remote failure path
        logger.error(f"Stooq fetch failed for {ticker}: {exc}")
        raise StooqFetchError from exc

    if df.empty:
        logger.warning(f"Empty DataFrame returned from Stooq for {ticker}")
        return pd.DataFrame()

    df = df.reset_index()
    # Handle different possible date column names
    date_col = None
    for col in df.columns:
        if col.lower() in ("date", "datetime"):
            date_col = col
            break
    
    if date_col is None:
        logger.error(f"No date column found in Stooq data for {ticker}. Columns: {list(df.columns)}")
        raise StooqFetchError(f"No date column found for {ticker}")
    
    df = df.rename(columns={date_col: "date"})
    df.columns = [c.lower() for c in df.columns]
    df = df.sort_values("date")
    df["ticker"] = ticker
    df["source"] = "stooq"

    df_window = df[df["date"] <= pd.Timestamp(run_date)]
    if df_window.empty:
        logger.warning(f"No Stooq data available for {ticker} up to {run_date}")
        return df_window

    # Save all historical data to enable feature calculation
    # Save each date to its own partition so curation can process them
    dates_saved = 0
    for date_val in df_window["date"].unique():
        date_dt = pd.Timestamp(date_val).date()
        partition = date_partition(date_dt)
        output_dir = target_dir / partition
        ensure_dir(output_dir)
        output_path = output_dir / f"{ticker}.parquet"
        
        # Get data for this specific date
        df_date = df_window[df_window["date"] == pd.Timestamp(date_val)]
        if not df_date.empty:
            df_date.to_parquet(output_path, index=False)
            dates_saved += 1
    
    # Return the data for run_date (or latest available)
    df_selected = df_window[df_window["date"] == pd.Timestamp(run_date)]
    if df_selected.empty:
        df_selected = df_window.tail(1)
        logger.warning(
            f"No exact match for {ticker} on {run_date}, using latest prior date {df_selected.iloc[-1]['date'].date()}"
        )
    
    logger.info(f"Saved Stooq prices for {ticker} ({dates_saved} days of history) to {target_dir}")
    return df_selected


__all__ = ["fetch_stooq_prices", "StooqFetchError"]

