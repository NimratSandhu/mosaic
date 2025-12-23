from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

from config.settings import get_settings
from db.duckdb_client import DuckDBClient, get_db_path
from logging_utils.setup import logger
from utils.paths import ensure_dir


def generate_positions(
    signals_df: pd.DataFrame,
    as_of_date: date,
    n_longs: int = 10,
    n_shorts: int = 10,
    signal_col: str = "signal_score",
) -> pd.DataFrame:
    """
    Generate dummy portfolio positions based on signal scores.
    
    Selects top N longs (highest signal scores) and bottom N shorts (lowest signal scores).
    
    Args:
        signals_df: DataFrame with signal scores (must have 'ticker', 'date', and signal_col)
        as_of_date: Date to generate positions for
        n_longs: Number of long positions (default: 10)
        n_shorts: Number of short positions (default: 10)
        signal_col: Column name for signal score (default: "signal_score")
        
    Returns:
        DataFrame with columns: ticker, date, position_type, signal_score, rank
    """
    # Check if DataFrame has required columns
    if signals_df.empty or "date" not in signals_df.columns or signal_col not in signals_df.columns:
        logger.warning(f"No valid signals found for {as_of_date} (empty or missing columns)")
        return pd.DataFrame(columns=["ticker", "date", "position_type", signal_col, "rank"])
    
    # Filter to date and valid signals
    date_signals = signals_df[
        (signals_df["date"] == pd.Timestamp(as_of_date)) & (signals_df[signal_col].notna())
    ].copy()
    
    if date_signals.empty:
        logger.warning(f"No valid signals found for {as_of_date}")
        return pd.DataFrame()
    
    # Sort by signal score
    date_signals = date_signals.sort_values(signal_col, ascending=False).reset_index(drop=True)
    
    positions = []
    
    # Top N longs
    longs = date_signals.head(n_longs).copy()
    longs["position_type"] = "long"
    longs["rank"] = range(1, len(longs) + 1)
    positions.append(longs[["ticker", "date", "position_type", signal_col, "rank"]])
    
    # Bottom N shorts
    shorts = date_signals.tail(n_shorts).copy()
    shorts["position_type"] = "short"
    shorts["rank"] = range(1, len(shorts) + 1)
    positions.append(shorts[["ticker", "date", "position_type", signal_col, "rank"]])
    
    positions_df = pd.concat(positions, ignore_index=True)
    
    logger.info(
        f"Generated {len(longs)} long and {len(shorts)} short positions for {as_of_date}"
    )
    
    return positions_df


def save_positions(
    positions_df: pd.DataFrame,
    as_of_date: date,
    output_dir: Path | None = None,
) -> Path:
    """
    Save positions to Parquet file.
    
    Always creates the file, even if empty, to ensure the schema exists.
    
    Args:
        positions_df: DataFrame with positions
        as_of_date: Date of the positions
        output_dir: Optional output directory override
        
    Returns:
        Path to saved file
    """
    settings = get_settings()
    output_root = Path(output_dir) if output_dir else settings.marts_dir
    
    output_path = output_root / "positions" / f"{as_of_date:%Y-%m-%d}.parquet"
    ensure_dir(output_path.parent)
    
    # Always save, even if empty, to ensure schema exists
    if positions_df.empty:
        # Create empty DataFrame with expected schema
        positions_df = pd.DataFrame(columns=["ticker", "date", "position_type", "signal_score", "rank"])
        logger.warning(f"Saving empty positions file for {as_of_date}")
    
    positions_df.to_parquet(output_path, index=False)
    logger.info(f"Saved positions to {output_path} ({len(positions_df)} rows)")
    
    return output_path


def load_positions_from_db(
    as_of_date: date,
    db_path: Path | None = None,
) -> pd.DataFrame:
    """
    Load positions from DuckDB.
    
    Args:
        as_of_date: Date to load positions for
        db_path: Optional path to DuckDB database
        
    Returns:
        DataFrame with positions
    """
    db_file = db_path if db_path else get_db_path()
    
    with DuckDBClient(db_file) as db:
        query = f"""
        SELECT * FROM marts.positions
        WHERE date = '{as_of_date}'
        ORDER BY position_type, rank
        """
        
        positions = db.query(query)
        
        if not positions:
            logger.warning(f"No positions found in database for {as_of_date}")
            return pd.DataFrame()
        
        positions_df = pd.DataFrame(
            positions,
            columns=["ticker", "date", "position_type", "signal_score", "rank"],
        )
    
    return positions_df


__all__ = [
    "generate_positions",
    "save_positions",
    "load_positions_from_db",
]

