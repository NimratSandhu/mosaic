from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

from config.settings import get_settings
from db.duckdb_client import DuckDBClient, get_db_path
from logging_utils.setup import logger
from utils.dates import date_partition


def load_curated_prices_to_db(
    run_date: date,
    curated_dir: Path | None = None,
    db_path: Path | None = None,
    if_exists: str = "append",
) -> None:
    """
    Load curated daily prices Parquet file into DuckDB.
    
    Uses append mode by default to accumulate historical data for feature calculation.
    Duplicates are handled by using CREATE OR REPLACE for the date partition.
    
    Args:
        run_date: Date to load prices for
        curated_dir: Optional override for curated directory
        db_path: Optional override for DuckDB database path
        if_exists: What to do if table exists ('replace', 'append', 'fail'). Default: 'append'
    """
    settings = get_settings()
    curated_root = Path(curated_dir) if curated_dir else settings.curated_dir
    db_file = db_path if db_path else get_db_path()
    
    partition = date_partition(run_date)
    curated_file = curated_root / "daily_prices" / partition / f"{run_date:%Y-%m-%d}.parquet"
    
    if not curated_file.exists():
        logger.warning(f"Curated prices file not found: {curated_file}")
        return
    
    table_name = "daily_prices"
    
    with DuckDBClient(db_file) as db:
        db.create_schema("curated")
        
        # If table doesn't exist, create it
        result = db.query(
            f"SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_schema = 'curated' AND table_name = '{table_name}'"
        )
        
        if result[0][0] == 0:
            # Create table from first file
            db.load_parquet_to_table(curated_file, table_name, schema_name="curated", if_exists="replace")
            logger.info(f"Created table and loaded prices for {run_date}")
        else:
            # Append mode: delete existing data for this date, then insert new
            # This handles updates while preserving historical data
            db.execute(
                f"DELETE FROM curated.{table_name} WHERE date = '{run_date}'"
            )
            # Insert new data
            db.execute(
                f"INSERT INTO curated.{table_name} SELECT * FROM read_parquet('{curated_file}')"
            )
            logger.info(f"Upserted prices for {run_date} into DuckDB table 'curated.{table_name}'")


def load_curated_fundamentals_to_db(
    run_date: date,
    curated_dir: Path | None = None,
    db_path: Path | None = None,
    if_exists: str = "append",
) -> None:
    """
    Load curated quarterly fundamentals Parquet file into DuckDB.
    
    Args:
        run_date: Date to determine quarter for loading
        curated_dir: Optional override for curated directory
        db_path: Optional override for DuckDB database path
        if_exists: What to do if table exists ('replace', 'append', 'fail')
    """
    settings = get_settings()
    curated_root = Path(curated_dir) if curated_dir else settings.curated_dir
    db_file = db_path if db_path else get_db_path()
    
    year = f"{run_date:%Y}"
    quarter = f"Q{(run_date.month - 1) // 3 + 1}"
    curated_file = curated_root / "quarterly_fundamentals" / year / quarter / f"{year}_{quarter}.parquet"
    
    if not curated_file.exists():
        logger.warning(f"Curated fundamentals file not found: {curated_file}")
        return
    
    table_name = "quarterly_fundamentals"
    
    with DuckDBClient(db_file) as db:
        db.load_parquet_to_table(curated_file, table_name, if_exists=if_exists)
        logger.info(f"Loaded fundamentals for {year}/{quarter} into DuckDB table '{table_name}'")


__all__ = ["load_curated_prices_to_db", "load_curated_fundamentals_to_db"]

