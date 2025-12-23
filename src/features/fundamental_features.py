from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

from db.duckdb_client import DuckDBClient, get_db_path
from logging_utils.setup import logger


def calculate_yoy_revenue_growth_proxy(
    as_of_date: date,
    db_path: Path | None = None,
) -> pd.DataFrame:
    """
    Calculate YoY revenue growth proxy based on available fundamental data.
    
    Note: Since we only have filing manifests (not parsed revenue data yet),
    this is a placeholder that returns empty DataFrame. In a full implementation,
    this would parse SEC filings to extract revenue and calculate YoY growth.
    
    Args:
        as_of_date: Date to calculate features as of
        db_path: Optional path to DuckDB database
        
    Returns:
        DataFrame with columns: ticker, date, yoy_revenue_growth_proxy
        (Currently returns empty DataFrame as placeholder)
    """
    # TODO: Parse SEC filings to extract revenue data
    # For now, return empty DataFrame as placeholder
    logger.info(
        f"Fundamental features not yet implemented - would calculate YoY revenue growth "
        f"by parsing SEC filings for {as_of_date}"
    )
    
    return pd.DataFrame(columns=["ticker", "date", "yoy_revenue_growth_proxy"])


__all__ = ["calculate_yoy_revenue_growth_proxy"]

