"""Data access layer for dashboard queries."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

from config.settings import get_settings
from db.duckdb_client import DuckDBClient, get_db_path
from logging_utils.setup import logger


def _table_exists(db: DuckDBClient, schema: str, table: str) -> bool:
    """Check if a table exists in the database."""
    try:
        # DuckDB uses a different approach - try to query the table
        # If it doesn't exist, it will raise an exception
        db.query(f"SELECT 1 FROM {schema}.{table} LIMIT 1")
        return True
    except Exception as e:
        # Table doesn't exist or other error
        logger.debug(f"Table {schema}.{table} does not exist: {e}")
        return False


def load_universe_with_sectors() -> pd.DataFrame:
    """Load universe tickers with sector information."""
    settings = get_settings()
    df = pd.read_csv(settings.universe_file)
    return df[["ticker", "company", "sector"]].copy()


def get_latest_signal_scores(
    as_of_date: Optional[date] = None,
    sector_filter: Optional[str] = None,
) -> pd.DataFrame:
    """
    Get latest signal scores for all tickers.
    
    Args:
        as_of_date: Date to get signals for (defaults to latest available)
        sector_filter: Optional sector to filter by
        
    Returns:
        DataFrame with columns: ticker, date, signal_score
    """
    db_file = get_db_path()
    
    with DuckDBClient(db_file) as db:
        # Check if table exists
        if not _table_exists(db, "marts", "signal_scores"):
            logger.warning("Table marts.signal_scores does not exist yet. No data available.")
            return pd.DataFrame(columns=["ticker", "date", "signal_score", "company", "sector"])
        
        try:
            if as_of_date:
                date_filter = f"WHERE date = '{as_of_date}'"
            else:
                # Get latest date safely
                max_date_result = db.query("SELECT MAX(date) FROM marts.signal_scores")
                if not max_date_result or not max_date_result[0][0]:
                    return pd.DataFrame(columns=["ticker", "date", "signal_score", "company", "sector"])
                date_filter = f"WHERE date = '{max_date_result[0][0]}'"
            
            query = f"""
            SELECT ticker, date, signal_score
            FROM marts.signal_scores
            {date_filter}
            ORDER BY signal_score DESC
            """
            
            results = db.query(query)
            
            if not results:
                return pd.DataFrame(columns=["ticker", "date", "signal_score", "company", "sector"])
            
            df = pd.DataFrame(results, columns=["ticker", "date", "signal_score"])
        except Exception as e:
            logger.error(f"Error querying signal scores: {e}")
            return pd.DataFrame(columns=["ticker", "date", "signal_score", "company", "sector"])
    
    # Join with universe to get sector information
    universe_df = load_universe_with_sectors()
    df = df.merge(universe_df, on="ticker", how="left")
    
    # Apply sector filter if provided
    if sector_filter:
        df = df[df["sector"] == sector_filter].copy()
    
    return df


def get_latest_positions(
    as_of_date: Optional[date] = None,
    position_type: Optional[str] = None,
) -> pd.DataFrame:
    """
    Get latest positions (longs/shorts).
    
    Args:
        as_of_date: Date to get positions for (defaults to latest available)
        position_type: Optional filter ('long' or 'short')
        
    Returns:
        DataFrame with columns: ticker, date, position_type, signal_score, rank
    """
    db_file = get_db_path()
    
    with DuckDBClient(db_file) as db:
        # Check if table exists
        if not _table_exists(db, "marts", "positions"):
            logger.warning("Table marts.positions does not exist yet. No data available.")
            return pd.DataFrame(
                columns=["ticker", "date", "position_type", "signal_score", "rank", "company", "sector"]
            )
        
        try:
            if as_of_date:
                date_filter = f"AND date = '{as_of_date}'"
            else:
                # Get latest date safely
                max_date_result = db.query("SELECT MAX(date) FROM marts.positions")
                if not max_date_result or not max_date_result[0][0]:
                    return pd.DataFrame(
                        columns=["ticker", "date", "position_type", "signal_score", "rank", "company", "sector"]
                    )
                date_filter = f"AND date = '{max_date_result[0][0]}'"
            
            type_filter = ""
            if position_type:
                type_filter = f"AND position_type = '{position_type}'"
            
            query = f"""
            SELECT ticker, date, position_type, signal_score, rank
            FROM marts.positions
            WHERE 1=1 {date_filter} {type_filter}
            ORDER BY position_type, rank
            """
            
            results = db.query(query)
            
            if not results:
                return pd.DataFrame(
                    columns=["ticker", "date", "position_type", "signal_score", "rank", "company", "sector"]
                )
            
            df = pd.DataFrame(
                results, columns=["ticker", "date", "position_type", "signal_score", "rank"]
            )
        except Exception as e:
            logger.error(f"Error querying positions: {e}")
            return pd.DataFrame(
                columns=["ticker", "date", "position_type", "signal_score", "rank", "company", "sector"]
            )
    
    # Join with universe to get company and sector
    universe_df = load_universe_with_sectors()
    df = df.merge(universe_df, on="ticker", how="left")
    
    return df


def get_ticker_price_history(
    ticker: str,
    days: int = 60,
) -> pd.DataFrame:
    """
    Get price history for a ticker.
    
    Args:
        ticker: Ticker symbol
        days: Number of days of history to retrieve
        
    Returns:
        DataFrame with columns: date, open, high, low, close, volume
    """
    db_file = get_db_path()
    
    with DuckDBClient(db_file) as db:
        # Check if table exists
        if not _table_exists(db, "curated", "daily_prices"):
            logger.warning("Table curated.daily_prices does not exist yet. No data available.")
            return pd.DataFrame(
                columns=["date", "open", "high", "low", "close", "volume"]
            )
        
        try:
            query = f"""
            SELECT date, open, high, low, close, volume
            FROM curated.daily_prices
            WHERE ticker = '{ticker}'
            ORDER BY date DESC
            LIMIT {days}
            """
            
            results = db.query(query)
            
            if not results:
                return pd.DataFrame(
                    columns=["date", "open", "high", "low", "close", "volume"]
                )
            
            df = pd.DataFrame(
                results, columns=["date", "open", "high", "low", "close", "volume"]
            )
            # Reverse to get chronological order
            df = df.sort_values("date").reset_index(drop=True)
        except Exception as e:
            logger.error(f"Error querying price history: {e}")
            return pd.DataFrame(
                columns=["date", "open", "high", "low", "close", "volume"]
            )
    
    return df


def get_ticker_features(
    ticker: str,
    as_of_date: Optional[date] = None,
) -> pd.DataFrame:
    """
    Get feature breakdown for a ticker.
    
    This reconstructs the features from the signal scores by querying
    the curated price data and recalculating features.
    
    Args:
        ticker: Ticker symbol
        as_of_date: Date to get features for (defaults to latest)
        
    Returns:
        DataFrame with feature columns
    """
    from features.price_features import calculate_price_features
    
    db_file = get_db_path()
    
    # Check if tables exist
    with DuckDBClient(db_file) as db:
        if not _table_exists(db, "marts", "signal_scores"):
            logger.warning("Table marts.signal_scores does not exist yet. Cannot calculate features.")
            return pd.DataFrame()
        
        if not _table_exists(db, "curated", "daily_prices"):
            logger.warning("Table curated.daily_prices does not exist yet. Cannot calculate features.")
            return pd.DataFrame()
    
    if as_of_date is None:
        # Get latest date from signal scores
        with DuckDBClient(db_file) as db:
            try:
                result = db.query(
                    f"SELECT MAX(date) FROM marts.signal_scores WHERE ticker = '{ticker}'"
                )
                if result and result[0][0]:
                    as_of_date = pd.Timestamp(result[0][0]).date()
                else:
                    # Try to get latest date overall
                    result = db.query("SELECT MAX(date) FROM marts.signal_scores")
                    if result and result[0][0]:
                        as_of_date = pd.Timestamp(result[0][0]).date()
                    else:
                        return pd.DataFrame()
            except Exception as e:
                logger.error(f"Error getting latest date: {e}")
                return pd.DataFrame()
    
    # Calculate features for this ticker
    try:
        features_df = calculate_price_features(as_of_date)
        if features_df.empty:
            return pd.DataFrame()
        
        ticker_features = features_df[features_df["ticker"] == ticker].copy()
        return ticker_features
    except Exception as e:
        logger.error(f"Error calculating features: {e}")
        return pd.DataFrame()


def get_sector_exposure(
    as_of_date: Optional[date] = None,
) -> pd.DataFrame:
    """
    Calculate sector exposure from positions.
    
    Args:
        as_of_date: Date to calculate exposure for (defaults to latest)
        
    Returns:
        DataFrame with columns: sector, long_count, short_count, net_exposure
    """
    positions_df = get_latest_positions(as_of_date=as_of_date)
    
    if positions_df.empty:
        return pd.DataFrame(
            columns=["sector", "long_count", "short_count", "net_exposure"]
        )
    
    # Group by sector and position type
    exposure = (
        positions_df.groupby(["sector", "position_type"])
        .size()
        .unstack(fill_value=0)
    )
    
    # Ensure both long and short columns exist
    if "long" not in exposure.columns:
        exposure["long"] = 0
    if "short" not in exposure.columns:
        exposure["short"] = 0
    
    exposure = exposure.reset_index()
    exposure["net_exposure"] = exposure["long"] - exposure["short"]
    exposure = exposure.sort_values("net_exposure", ascending=False)
    
    return exposure[["sector", "long", "short", "net_exposure"]].rename(
        columns={"long": "long_count", "short": "short_count"}
    )


def get_available_dates() -> list[date]:
    """Get list of available dates in signal_scores."""
    db_file = get_db_path()
    
    with DuckDBClient(db_file) as db:
        # Check if table exists
        if not _table_exists(db, "marts", "signal_scores"):
            logger.debug("Table marts.signal_scores does not exist yet. Returning empty date list.")
            return []
        
        try:
            results = db.query(
                "SELECT DISTINCT date FROM marts.signal_scores ORDER BY date DESC"
            )
            
            if not results:
                return []
            
            return [pd.Timestamp(row[0]).date() for row in results]
        except Exception as e:
            logger.error(f"Error getting available dates: {e}")
            return []


def get_available_sectors() -> list[str]:
    """Get list of available sectors from universe."""
    universe_df = load_universe_with_sectors()
    return sorted(universe_df["sector"].dropna().unique().tolist())

