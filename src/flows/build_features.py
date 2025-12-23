from __future__ import annotations

import argparse
import sys
from typing import Optional

import pandas as pd
from prefect import flow, task

from db.load_marts import load_positions_to_db, load_signal_scores_to_db
from features.fundamental_features import calculate_yoy_revenue_growth_proxy
from features.position_generator import generate_positions, save_positions
from features.price_features import calculate_price_features
from features.signal_scorer import save_signal_scores, score_signals
from logging_utils.setup import configure_logging, logger
from utils.dates import parse_run_date


@task
def calculate_features(run_date_str: str):
    """Task to calculate price and fundamental features."""
    from datetime import date
    
    run_dt = parse_run_date(run_date_str)
    
    logger.info(f"Calculating features for {run_dt}")
    
    # Calculate price features
    price_features = calculate_price_features(run_dt)
    
    # Calculate fundamental features (placeholder for now)
    fundamental_features = calculate_yoy_revenue_growth_proxy(run_dt)
    
    return {"price": price_features, "fundamental": fundamental_features}


@task
def score_and_save_signals(
    price_features_df, fundamental_features_df, run_date_str: str
) -> None:
    """Task to score signals and save to marts layer."""
    from datetime import date
    
    run_dt = parse_run_date(run_date_str)
    
    logger.info(f"Scoring signals for {run_dt}")
    
    # Score signals
    signals_df = score_signals(
        price_features_df,
        fundamental_features_df if not fundamental_features_df.empty else None,
        as_of_date=run_dt,
    )
    
    # Ensure signals_df has the correct schema even if empty
    if signals_df.empty:
        signals_df = pd.DataFrame(columns=["ticker", "date", "signal_score"])
        # Add the date column value for consistency
        signals_df["date"] = pd.Series(dtype="datetime64[ns]")
        logger.warning(f"No signals to save for {run_dt}")
    else:
        # Ensure date column is datetime type
        signals_df["date"] = pd.to_datetime(signals_df["date"])
    
    # Always save to Parquet (even if empty) to ensure schema exists
    save_signal_scores(signals_df, run_dt)
    
    # Load into DuckDB (will create table if file doesn't exist)
    load_signal_scores_to_db(run_dt, if_exists="replace")
    
    logger.info(f"Completed signal scoring for {run_dt}")
    return signals_df


@task
def generate_and_save_positions(signals_df, run_date_str: str) -> None:
    """Task to generate positions and save to marts layer."""
    from datetime import date
    import pandas as pd
    
    run_dt = parse_run_date(run_date_str)
    
    logger.info(f"Generating positions for {run_dt}")
    
    # Handle None or empty DataFrame
    if signals_df is None or signals_df.empty:
        # Create empty DataFrame with correct schema
        signals_df = pd.DataFrame(columns=["ticker", "date", "signal_score"])
        signals_df["date"] = pd.Series(dtype="datetime64[ns]")
    
    # Ensure date column exists and is datetime
    if "date" not in signals_df.columns:
        signals_df["date"] = pd.to_datetime(run_dt)
    else:
        signals_df["date"] = pd.to_datetime(signals_df["date"])
    
    # Generate positions (may be empty if no signals)
    positions_df = generate_positions(signals_df, run_dt, n_longs=10, n_shorts=10)
    
    # Always save to Parquet (even if empty) to ensure schema exists
    save_positions(positions_df, run_dt)
    
    # Load into DuckDB (will create table if file doesn't exist)
    load_positions_to_db(run_dt, if_exists="replace")
    
    if positions_df.empty:
        logger.warning(f"No positions generated for {run_dt}")
    else:
        logger.info(f"Completed position generation for {run_dt}")


@flow(name="build_features")
def build_features(run_date: Optional[str] = None) -> None:
    """
    Main feature engine flow that calculates features, scores signals, and generates positions.
    
    Args:
        run_date: Date string in YYYY-MM-DD format. Defaults to today.
    """
    run_date_str = run_date or parse_run_date().strftime("%Y-%m-%d")
    
    logger.info(f"Starting feature engine flow for {run_date_str}")
    
    # Calculate features
    features = calculate_features(run_date_str)
    price_features_df = features["price"]
    fundamental_features_df = features["fundamental"]
    
    # Score signals and save (returns signals_df)
    signals_df = score_and_save_signals(price_features_df, fundamental_features_df, run_date_str)
    
    # Generate positions and save
    generate_and_save_positions(signals_df, run_date_str)
    
    logger.info(f"Completed feature engine flow for {run_date_str}")


def main(argv: Optional[list[str]] = None) -> None:
    """CLI entrypoint for feature engine flow."""
    if argv is None:
        argv = sys.argv[1:]
    
    parser = argparse.ArgumentParser(description="Build features, signals, and positions")
    parser.add_argument(
        "--run-date",
        type=str,
        default=None,
        help="Date to build features for (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level (default: INFO)",
    )
    
    args = parser.parse_args(argv)
    
    configure_logging(log_level=args.log_level)
    
    build_features(run_date=args.run_date)


if __name__ == "__main__":
    main()

