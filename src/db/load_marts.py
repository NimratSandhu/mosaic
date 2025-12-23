from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

from db.duckdb_client import DuckDBClient, get_db_path
from logging_utils.setup import logger


def load_signal_scores_to_db(
    as_of_date: date,
    signal_scores_file: Path | None = None,
    db_path: Path | None = None,
    if_exists: str = "replace",
) -> None:
    """
    Load signal scores Parquet file into DuckDB.
    
    Args:
        as_of_date: Date of the signal scores
        signal_scores_file: Optional path to signal scores Parquet file
        db_path: Optional override for DuckDB database path
        if_exists: What to do if table exists ('replace', 'append', 'fail')
    """
    from config.settings import get_settings
    
    settings = get_settings()
    db_file = db_path if db_path else get_db_path()
    
    if signal_scores_file is None:
        signal_scores_file = (
            settings.marts_dir / "signal_scores" / f"{as_of_date:%Y-%m-%d}.parquet"
        )
    
    table_name = "signal_scores"
    
    with DuckDBClient(db_file) as db:
        db.create_schema("marts")
        
        if not signal_scores_file.exists():
            logger.warning(f"Signal scores file not found: {signal_scores_file}, creating empty table")
            # Create empty table with expected schema
            db.execute(f"""
                CREATE TABLE IF NOT EXISTS marts.{table_name} (
                    ticker VARCHAR,
                    date DATE,
                    signal_score DOUBLE
                )
            """)
            if if_exists == "replace":
                db.execute(f"DELETE FROM marts.{table_name} WHERE date = '{as_of_date}'")
            logger.info(f"Created empty table 'marts.{table_name}'")
        else:
            # Load from Parquet file
            db.load_parquet_to_table(signal_scores_file, table_name, schema_name="marts", if_exists=if_exists)
            logger.info(f"Loaded signal scores for {as_of_date} into DuckDB table 'marts.{table_name}'")


def load_positions_to_db(
    as_of_date: date,
    positions_file: Path | None = None,
    db_path: Path | None = None,
    if_exists: str = "replace",
) -> None:
    """
    Load positions Parquet file into DuckDB.
    
    Args:
        as_of_date: Date of the positions
        positions_file: Optional path to positions Parquet file
        db_path: Optional override for DuckDB database path
        if_exists: What to do if table exists ('replace', 'append', 'fail')
    """
    from config.settings import get_settings
    
    settings = get_settings()
    db_file = db_path if db_path else get_db_path()
    
    if positions_file is None:
        positions_file = settings.marts_dir / "positions" / f"{as_of_date:%Y-%m-%d}.parquet"
    
    table_name = "positions"
    
    with DuckDBClient(db_file) as db:
        db.create_schema("marts")
        
        if not positions_file.exists():
            logger.warning(f"Positions file not found: {positions_file}, creating empty table")
            # Create empty table with expected schema
            db.execute(f"""
                CREATE TABLE IF NOT EXISTS marts.{table_name} (
                    ticker VARCHAR,
                    date DATE,
                    position_type VARCHAR,
                    signal_score DOUBLE,
                    rank INTEGER
                )
            """)
            if if_exists == "replace":
                db.execute(f"DELETE FROM marts.{table_name} WHERE date = '{as_of_date}'")
            logger.info(f"Created empty table 'marts.{table_name}'")
        else:
            # Load from Parquet file
            db.load_parquet_to_table(positions_file, table_name, schema_name="marts", if_exists=if_exists)
            logger.info(f"Loaded positions for {as_of_date} into DuckDB table 'marts.{table_name}'")


__all__ = ["load_signal_scores_to_db", "load_positions_to_db"]

