from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    data_root: Path = Path(os.getenv("DATA_ROOT", "./data")).resolve()
    raw_prices_dir: Path = Path(
        os.getenv("RAW_PRICES_DIR", "./data/raw/prices_stooq")
    ).resolve()
    raw_fundamentals_dir: Path = Path(
        os.getenv("RAW_FUNDAMENTALS_DIR", "./data/raw/fundamentals_sec")
    ).resolve()
    curated_dir: Path = Path(os.getenv("CURATED_DIR", "./data/curated")).resolve()
    marts_dir: Path = Path(os.getenv("MARTS_DIR", "./data/marts")).resolve()

    universe_file: Path = Path(
        os.getenv("UNIVERSE_FILE", "./config/universe/sp100.csv")
    ).resolve()

    alpha_vantage_api_key: Optional[str] = os.getenv("ALPHAVANTAGE_API_KEY")
    sec_edgar_user_agent: str = os.getenv(
        "SEC_EDGAR_USER_AGENT", "YourName your.email@example.com"
    )
    sec_edgar_company_name: Optional[str] = os.getenv(
        "SEC_EDGAR_COMPANY_NAME", "NimratSandhuProjectMosaic"
    )

    sec_edgar_user_email: str = os.getenv(
        "SEC_EDGAR_USER_EMAIL", "nimsandhu16@gmail.com"
    )

    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    # GCS configuration
    gcs_bucket_name: Optional[str] = os.getenv("GCS_BUCKET_NAME")
    gcs_marts_prefix: str = os.getenv("GCS_MARTS_PREFIX", "marts/")
    gcs_enabled: bool = os.getenv("GCS_ENABLED", "false").lower() == "true"


_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Return cached settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


__all__ = ["Settings", "get_settings"]

