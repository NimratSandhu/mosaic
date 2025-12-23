from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

from db.duckdb_client import DuckDBClient, get_db_path
from logging_utils.setup import logger


def calculate_realized_volatility(
    prices_df: pd.DataFrame, window: int = 20, price_col: str = "close"
) -> pd.Series:
    """
    Calculate realized volatility (rolling standard deviation of returns).
    
    Args:
        prices_df: DataFrame with price data, must be sorted by date
        window: Rolling window size in days (default: 20)
        price_col: Column name for price (default: "close")
        
    Returns:
        Series with realized volatility values
    """
    returns = prices_df[price_col].pct_change()
    realized_vol = returns.rolling(window=window).std() * (252 ** 0.5)  # Annualized
    return realized_vol


def calculate_momentum(
    prices_df: pd.DataFrame, window: int = 60, price_col: str = "close"
) -> pd.Series:
    """
    Calculate momentum as the return over a specified window.
    
    Args:
        prices_df: DataFrame with price data, must be sorted by date
        window: Lookback window in days (default: 60)
        price_col: Column name for price (default: "close")
        
    Returns:
        Series with momentum values (as percentage returns)
    """
    momentum = prices_df[price_col].pct_change(periods=window)
    return momentum


def calculate_mean_reversion_zscore(
    prices_df: pd.DataFrame, window: int = 5, price_col: str = "close"
) -> pd.Series:
    """
    Calculate mean reversion Z-score: (price - rolling_mean) / rolling_std.
    
    Args:
        prices_df: DataFrame with price data, must be sorted by date
        window: Rolling window size in days (default: 5)
        price_col: Column name for price (default: "close")
        
    Returns:
        Series with Z-score values
    """
    rolling_mean = prices_df[price_col].rolling(window=window).mean()
    rolling_std = prices_df[price_col].rolling(window=window).std()
    zscore = (prices_df[price_col] - rolling_mean) / rolling_std
    return zscore


def calculate_price_features(
    as_of_date: date,
    db_path: Path | None = None,
    lookback_days: int = 100,
) -> pd.DataFrame:
    """
    Calculate all price features for all tickers as of a given date.
    
    Features calculated:
    - realized_vol_20d: 20-day realized volatility (annualized)
    - momentum_60d: 60-day momentum (return)
    - mean_reversion_zscore_5d: 5-day mean reversion Z-score
    
    Args:
        as_of_date: Date to calculate features as of
        db_path: Optional path to DuckDB database
        lookback_days: Number of days of history to fetch (default: 100)
        
    Returns:
        DataFrame with columns: ticker, date, realized_vol_20d, momentum_60d, mean_reversion_zscore_5d
    """
    db_file = db_path if db_path else get_db_path()
    
    with DuckDBClient(db_file) as db:
        # Fetch price history for all tickers
        start_date = pd.Timestamp(as_of_date) - pd.Timedelta(days=lookback_days)
        
        query = f"""
        SELECT 
            ticker,
            date,
            close
        FROM curated.daily_prices
        WHERE date >= '{start_date.date()}'
          AND date <= '{as_of_date}'
        ORDER BY ticker, date
        """
        
        prices_df = pd.DataFrame(db.query(query), columns=["ticker", "date", "close"])
    
    if prices_df.empty:
        logger.warning(f"No price data found for date range ending {as_of_date}")
        return pd.DataFrame()
    
    # Calculate features for each ticker
    feature_rows = []
    
    for ticker in prices_df["ticker"].unique():
        ticker_prices = prices_df[prices_df["ticker"] == ticker].copy()
        ticker_prices = ticker_prices.sort_values("date").reset_index(drop=True)
        
        # Need at least 60 days for momentum, but we can still calculate other features with less
        if len(ticker_prices) < 60:
            logger.debug(
                f"Insufficient data for {ticker} (need 60 days for momentum, have {len(ticker_prices)}). "
                f"Skipping this ticker."
            )
            continue
        
        # Calculate features
        realized_vol = calculate_realized_volatility(ticker_prices, window=20)
        momentum = calculate_momentum(ticker_prices, window=60)
        zscore = calculate_mean_reversion_zscore(ticker_prices, window=5)
        
        # Get the latest values (as of as_of_date)
        # Convert date column to datetime if needed for comparison
        ticker_prices["date"] = pd.to_datetime(ticker_prices["date"])
        latest_idx = ticker_prices[ticker_prices["date"] <= pd.Timestamp(as_of_date)].index[-1]
        
        feature_rows.append({
            "ticker": ticker,
            "date": as_of_date,
            "realized_vol_20d": realized_vol.iloc[latest_idx] if pd.notna(realized_vol.iloc[latest_idx]) else None,
            "momentum_60d": momentum.iloc[latest_idx] if pd.notna(momentum.iloc[latest_idx]) else None,
            "mean_reversion_zscore_5d": zscore.iloc[latest_idx] if pd.notna(zscore.iloc[latest_idx]) else None,
        })
    
    features_df = pd.DataFrame(feature_rows)
    logger.info(f"Calculated price features for {len(features_df)} tickers as of {as_of_date}")
    
    return features_df


__all__ = [
    "calculate_realized_volatility",
    "calculate_momentum",
    "calculate_mean_reversion_zscore",
    "calculate_price_features",
]

