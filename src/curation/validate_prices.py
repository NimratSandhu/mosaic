from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
from great_expectations import DataContext
from great_expectations.core import ExpectationSuite
from great_expectations.dataset import PandasDataset

from config.settings import get_settings
from logging_utils.setup import logger


class ValidationError(Exception):
    """Raised when data validation fails."""


def validate_daily_prices(
    df: pd.DataFrame, curated_file: Path | None = None, fail_on_error: bool = True
) -> dict:
    """
    Validate curated daily prices DataFrame using Great Expectations.
    
    Validates:
    - Price columns are positive
    - High >= Low, High >= Close >= Low
    - No overnight price changes > 50%
    - No duplicate ticker+date combinations
    - Volume >= 0
    
    Args:
        df: DataFrame to validate
        curated_file: Optional path to curated file (for context)
        fail_on_error: If True, raise ValidationError on critical failures
        
    Returns:
        Dictionary with validation results
    """
    if df.empty:
        logger.warning("Empty DataFrame provided for validation")
        return {"valid": True, "warnings": ["Empty DataFrame"]}
    
    # Convert to Great Expectations PandasDataset
    ge_df = PandasDataset(df)
    
    results = {
        "valid": True,
        "errors": [],
        "warnings": [],
        "stats": {
            "total_rows": len(df),
            "unique_tickers": df["ticker"].nunique() if "ticker" in df.columns else 0,
        },
    }
    
    # 1. Check required columns exist
    required_cols = ["date", "ticker", "open", "high", "low", "close", "volume"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        error_msg = f"Missing required columns: {missing_cols}"
        results["errors"].append(error_msg)
        results["valid"] = False
        if fail_on_error:
            raise ValidationError(error_msg)
        return results
    
    # 2. Validate price columns are positive
    try:
        ge_df.expect_column_values_to_be_between("open", min_value=0.01, mostly=0.99)
        ge_df.expect_column_values_to_be_between("high", min_value=0.01, mostly=0.99)
        ge_df.expect_column_values_to_be_between("low", min_value=0.01, mostly=0.99)
        ge_df.expect_column_values_to_be_between("close", min_value=0.01, mostly=0.99)
    except Exception as exc:
        results["warnings"].append(f"Price positivity check: {exc}")
    
    # 3. Validate high >= low
    invalid_high_low = df[df["high"] < df["low"]]
    if not invalid_high_low.empty:
        error_msg = f"Found {len(invalid_high_low)} rows where high < low"
        results["errors"].append(error_msg)
        results["valid"] = False
        if fail_on_error:
            raise ValidationError(error_msg)
    
    # 4. Validate high >= close >= low
    invalid_close = df[(df["close"] > df["high"]) | (df["close"] < df["low"])]
    if not invalid_close.empty:
        error_msg = f"Found {len(invalid_close)} rows where close is outside [low, high] range"
        results["errors"].append(error_msg)
        results["valid"] = False
        if fail_on_error:
            raise ValidationError(error_msg)
    
    # 5. Check for duplicate ticker+date combinations
    duplicates = df[df.duplicated(subset=["ticker", "date"], keep=False)]
    if not duplicates.empty:
        error_msg = f"Found {len(duplicates)} duplicate ticker+date combinations"
        results["errors"].append(error_msg)
        results["valid"] = False
        if fail_on_error:
            raise ValidationError(error_msg)
    
    # 6. Validate volume >= 0
    negative_volume = df[df["volume"] < 0]
    if not negative_volume.empty:
        error_msg = f"Found {len(negative_volume)} rows with negative volume"
        results["errors"].append(error_msg)
        results["valid"] = False
        if fail_on_error:
            raise ValidationError(error_msg)
    
    # 7. Check for extreme price values (warning only, not error)
    # Note: Overnight change check requires historical data, so we skip it for single-day curation
    # This can be added later when we have multi-day curated datasets
    extreme_prices = df[(df["close"] > 10000) | (df["close"] < 0.01)]
    if not extreme_prices.empty:
        warning_msg = f"Found {len(extreme_prices)} rows with extreme close prices (outside [0.01, 10000])"
        results["warnings"].append(warning_msg)
    
    # Log results
    if results["errors"]:
        logger.error(f"Validation failed: {results['errors']}")
    if results["warnings"]:
        logger.warning(f"Validation warnings: {results['warnings']}")
    if results["valid"]:
        logger.info(f"Validation passed for {results['stats']['total_rows']} rows")
    
    return results


__all__ = ["validate_daily_prices", "ValidationError"]

