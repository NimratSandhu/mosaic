from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

from config.settings import get_settings
from logging_utils.setup import logger
from utils.paths import ensure_dir


def normalize_to_zscore(df: pd.DataFrame, value_col: str, group_col: str = "date") -> pd.Series:
    """
    Normalize a column to Z-scores within groups.
    
    Z-score = (value - group_mean) / group_std
    
    Args:
        df: DataFrame containing the data
        value_col: Column name to normalize
        group_col: Column to group by (default: "date" for cross-sectional normalization)
        
    Returns:
        Series with Z-score values
    """
    grouped = df.groupby(group_col)[value_col]
    zscores = (df[value_col] - grouped.transform("mean")) / grouped.transform("std")
    return zscores


def score_signals(
    price_features_df: pd.DataFrame,
    fundamental_features_df: pd.DataFrame | None = None,
    as_of_date: date | None = None,
) -> pd.DataFrame:
    """
    Create a standardized signal score table by normalizing features to Z-scores.
    
    Normalizes each feature across the universe (all tickers) for each date,
    creating interpretable signals that can be combined.
    
    Args:
        price_features_df: DataFrame with price features (must have 'date' and 'ticker' columns)
        fundamental_features_df: Optional DataFrame with fundamental features
        as_of_date: Optional date to filter to (if None, uses all dates in price_features_df)
        
    Returns:
        DataFrame with columns:
        - ticker, date
        - All original feature columns (normalized to Z-scores)
        - signal_score: Weighted average of normalized signals (equal weights by default)
    """
    if price_features_df.empty:
        logger.warning("Empty price features DataFrame provided")
        return pd.DataFrame()
    
    # Filter to specific date if provided
    if as_of_date:
        price_features_df = price_features_df[price_features_df["date"] == as_of_date].copy()
    
    if price_features_df.empty:
        logger.warning(f"No price features found for date {as_of_date}")
        return pd.DataFrame()
    
    # Get feature columns (exclude ticker and date)
    feature_cols = [col for col in price_features_df.columns if col not in ["ticker", "date"]]
    
    if not feature_cols:
        logger.warning("No feature columns found in price_features_df")
        return pd.DataFrame()
    
    # Normalize each feature to Z-score across universe per date
    signals_df = price_features_df[["ticker", "date"]].copy()
    
    for feature_col in feature_cols:
        # Only normalize non-null values
        mask = price_features_df[feature_col].notna()
        if mask.sum() == 0:
            logger.warning(f"All values are null for feature {feature_col}")
            signals_df[f"{feature_col}_zscore"] = None
            continue
        
        # Calculate Z-score within each date group
        zscore_col = f"{feature_col}_zscore"
        signals_df[zscore_col] = None
        signals_df.loc[mask, zscore_col] = normalize_to_zscore(
            price_features_df[mask], feature_col, group_col="date"
        )
    
    # Merge fundamental features if provided
    if fundamental_features_df is not None and not fundamental_features_df.empty:
        if as_of_date:
            fundamental_features_df = fundamental_features_df[
                fundamental_features_df["date"] == as_of_date
            ].copy()
        
        if not fundamental_features_df.empty:
            # Merge on ticker and date
            signals_df = signals_df.merge(
                fundamental_features_df,
                on=["ticker", "date"],
                how="left",
            )
            
            # Normalize fundamental features too
            fund_feature_cols = [
                col
                for col in fundamental_features_df.columns
                if col not in ["ticker", "date"]
            ]
            for feature_col in fund_feature_cols:
                mask = signals_df[feature_col].notna()
                if mask.sum() > 0:
                    zscore_col = f"{feature_col}_zscore"
                    signals_df[zscore_col] = None
                    signals_df.loc[mask, zscore_col] = normalize_to_zscore(
                        signals_df[mask], feature_col, group_col="date"
                    )
    
    # Calculate composite signal score (equal-weighted average of Z-scores)
    zscore_cols = [col for col in signals_df.columns if col.endswith("_zscore")]
    
    if zscore_cols:
        # Calculate signal score as average of available Z-scores per row
        signals_df["signal_score"] = signals_df[zscore_cols].mean(axis=1)
    else:
        logger.warning("No Z-score columns found, signal_score will be null")
        signals_df["signal_score"] = None
    
    logger.info(
        f"Created signal scores for {len(signals_df)} tickers "
        f"on {signals_df['date'].nunique()} date(s)"
    )
    
    return signals_df


def save_signal_scores(
    signals_df: pd.DataFrame,
    as_of_date: date,
    output_dir: Path | None = None,
) -> Path:
    """
    Save signal scores to Parquet file.
    
    Always creates the file, even if empty, to ensure the schema exists.
    
    Args:
        signals_df: DataFrame with signal scores
        as_of_date: Date of the signals
        output_dir: Optional output directory override
        
    Returns:
        Path to saved file
    """
    settings = get_settings()
    output_root = Path(output_dir) if output_dir else settings.marts_dir
    
    output_path = output_root / "signal_scores" / f"{as_of_date:%Y-%m-%d}.parquet"
    ensure_dir(output_path.parent)
    
    # Always save, even if empty, to ensure schema exists
    if signals_df.empty:
        # Create empty DataFrame with expected schema
        signals_df = pd.DataFrame(columns=["ticker", "date", "signal_score"])
        logger.warning(f"Saving empty signal scores file for {as_of_date}")
    
    signals_df.to_parquet(output_path, index=False)
    logger.info(f"Saved signal scores to {output_path} ({len(signals_df)} rows)")
    
    return output_path


__all__ = ["normalize_to_zscore", "score_signals", "save_signal_scores"]

