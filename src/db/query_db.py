"""Utility script to query and inspect DuckDB database."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from db.duckdb_client import DuckDBClient, get_db_path
from logging_utils.setup import configure_logging, logger


def list_tables(db_path: Path | None = None) -> None:
    """List all tables in the database."""
    db_file = db_path if db_path else get_db_path()
    
    if not db_file.exists():
        logger.error(f"Database file not found: {db_file}")
        return
    
    with DuckDBClient(db_file) as db:
        # List all tables in curated schema
        tables = db.query(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'curated' ORDER BY table_name"
        )
        
        if not tables:
            logger.info("No tables found in 'curated' schema")
            return
        
        logger.info(f"Found {len(tables)} table(s) in 'curated' schema:")
        for table in tables:
            table_name = table[0]
            row_count = db.query(f"SELECT COUNT(*) FROM curated.{table_name}")[0][0]
            logger.info(f"  - {table_name}: {row_count} rows")


def show_table_info(table_name: str, db_path: Path | None = None, limit: int = 10) -> None:
    """Show information about a specific table."""
    db_file = db_path if db_path else get_db_path()
    
    if not db_file.exists():
        logger.error(f"Database file not found: {db_file}")
        return
    
    with DuckDBClient(db_file) as db:
        # Check if table exists
        result = db.query(
            f"SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_schema = 'curated' AND table_name = '{table_name}'"
        )
        
        if result[0][0] == 0:
            logger.error(f"Table 'curated.{table_name}' not found")
            return
        
        # Get row count
        row_count = db.query(f"SELECT COUNT(*) FROM curated.{table_name}")[0][0]
        logger.info(f"Table: curated.{table_name}")
        logger.info(f"Total rows: {row_count}")
        
        if row_count == 0:
            logger.warning("Table is empty")
            return
        
        # Get column info
        columns = db.query(
            f"SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_schema = 'curated' AND table_name = '{table_name}' "
            f"ORDER BY ordinal_position"
        )
        
        logger.info("\nColumns:")
        for col_name, col_type in columns:
            logger.info(f"  - {col_name}: {col_type}")
        
        # Show sample data
        logger.info(f"\nSample data (first {limit} rows):")
        sample = db.query(f"SELECT * FROM curated.{table_name} LIMIT {limit}")
        
        if sample:
            # Get column names
            col_names = [col[0] for col in columns]
            logger.info("  " + " | ".join(col_names))
            logger.info("  " + "-" * 60)
            
            for row in sample:
                logger.info("  " + " | ".join(str(val) for val in row))


def run_query(query: str, db_path: Path | None = None) -> None:
    """Run a custom SQL query."""
    db_file = db_path if db_path else get_db_path()
    
    if not db_file.exists():
        logger.error(f"Database file not found: {db_file}")
        return
    
    with DuckDBClient(db_file) as db:
        try:
            results = db.query(query)
            
            if results:
                logger.info(f"Query returned {len(results)} row(s):")
                for row in results:
                    logger.info(f"  {row}")
            else:
                logger.info("Query returned no results")
        except Exception as exc:
            logger.error(f"Query failed: {exc}")


def main(argv: list[str] | None = None) -> None:
    """CLI entrypoint."""
    if argv is None:
        argv = sys.argv[1:]
    
    parser = argparse.ArgumentParser(description="Query and inspect DuckDB database")
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="Path to DuckDB database file (default: data/marts/duckdb/mosaic.duckdb)",
    )
    parser.add_argument(
        "--list-tables",
        action="store_true",
        help="List all tables in the database",
    )
    parser.add_argument(
        "--table",
        type=str,
        default=None,
        help="Show information about a specific table",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of sample rows to show (default: 10)",
    )
    parser.add_argument(
        "--query",
        type=str,
        default=None,
        help="Run a custom SQL query",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level (default: INFO)",
    )
    
    args = parser.parse_args(argv)
    
    configure_logging(log_level=args.log_level)
    
    db_path = Path(args.db_path) if args.db_path else None
    
    if args.list_tables:
        list_tables(db_path)
    elif args.table:
        show_table_info(args.table, db_path, args.limit)
    elif args.query:
        run_query(args.query, db_path)
    else:
        # Default: list tables
        list_tables(db_path)


if __name__ == "__main__":
    main()

