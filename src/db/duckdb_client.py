from __future__ import annotations

from pathlib import Path
from typing import Optional

import duckdb
from loguru import logger

from config.settings import get_settings


class DuckDBClient:
    """Client for managing DuckDB database connections and operations."""

    def __init__(self, db_path: Path | str | None = None):
        """
        Initialize DuckDB client.
        
        Args:
            db_path: Optional path to DuckDB database file. If None, uses in-memory database.
        """
        self.db_path = Path(db_path) if db_path else None
        self.conn = duckdb.connect(str(self.db_path) if self.db_path else ":memory:")
        logger.info(f"Connected to DuckDB: {self.db_path or 'in-memory'}")
    
    def execute(self, query: str) -> duckdb.DuckDBPyConnection:
        """Execute a SQL query."""
        return self.conn.execute(query)
    
    def create_schema(self, schema_name: str = "curated") -> None:
        """Create a schema if it doesn't exist."""
        self.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        logger.info(f"Schema '{schema_name}' ready")
    
    def load_parquet_to_table(
        self,
        parquet_path: Path | str,
        table_name: str,
        schema_name: str = "curated",
        if_exists: str = "replace",
    ) -> None:
        """
        Load a Parquet file into a DuckDB table.
        
        Args:
            parquet_path: Path to Parquet file
            table_name: Name of the table to create
            schema_name: Schema name (default: 'curated')
            if_exists: What to do if table exists ('replace', 'append', 'fail')
        """
        parquet_path = Path(parquet_path)
        if not parquet_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {parquet_path}")
        
        self.create_schema(schema_name)
        
        full_table_name = f"{schema_name}.{table_name}"
        
        if if_exists == "replace":
            self.execute(f"DROP TABLE IF EXISTS {full_table_name}")
        elif if_exists == "fail":
            # Check if table exists
            result = self.execute(
                f"SELECT COUNT(*) FROM information_schema.tables "
                f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'"
            ).fetchone()
            if result[0] > 0:
                raise ValueError(f"Table {full_table_name} already exists")
        
        # Load Parquet file
        self.execute(
            f"CREATE TABLE {full_table_name} AS SELECT * FROM read_parquet('{parquet_path}')"
        )
        
        # Create indexes for common queries
        try:
            self.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_ticker ON {full_table_name}(ticker)")
            if self._has_column(full_table_name, "date"):
                self.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {full_table_name}(date)")
        except Exception as exc:
            logger.warning(f"Could not create indexes: {exc}")
        
        row_count = self.execute(f"SELECT COUNT(*) FROM {full_table_name}").fetchone()[0]
        logger.info(f"Loaded {row_count} rows into {full_table_name} from {parquet_path}")
    
    def _has_column(self, table_name: str, column_name: str) -> bool:
        """Check if a table has a specific column."""
        try:
            result = self.execute(
                f"SELECT COUNT(*) FROM information_schema.columns "
                f"WHERE table_name = '{table_name.split('.')[-1]}' "
                f"AND column_name = '{column_name}'"
            ).fetchone()
            return result[0] > 0
        except Exception:
            return False
    
    def query(self, sql: str) -> list:
        """Execute a query and return results as a list of tuples."""
        return self.conn.execute(sql).fetchall()
    
    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()
        logger.info("DuckDB connection closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def get_db_path() -> Path:
    """Get the default DuckDB database path from settings."""
    settings = get_settings()
    db_dir = settings.marts_dir / "duckdb"
    db_dir.mkdir(parents=True, exist_ok=True)
    return db_dir / "mosaic.duckdb"


__all__ = ["DuckDBClient", "get_db_path"]

