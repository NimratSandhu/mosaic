from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import List

import pandas as pd
from sec_edgar_downloader import Downloader
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from config.settings import get_settings
from logging.setup import logger
from utils.paths import ensure_dir


class SecDownloadError(Exception):
    """Raised when SEC download fails after retries."""


def _quarter_from_date(dt: date) -> str:
    quarter = (dt.month - 1) // 3 + 1
    return f"Q{quarter}"


@retry(
    reraise=True,
    retry=retry_if_exception_type(Exception),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
)
def _download_filings(
    downloader: Downloader, filing_type: str, ticker: str, amount: int
) -> List[str]:
    return downloader.get(filing_type, ticker, amount=amount)


def download_fundamentals(
    ticker: str,
    run_date: date,
    filing_type: str = "10-Q",
    amount: int = 1,
    raw_dir: Path | None = None,
) -> pd.DataFrame:
    """Download recent fundamentals filings and write a manifest parquet in raw layer."""
    settings = get_settings()
    target_root = Path(raw_dir) if raw_dir else settings.raw_fundamentals_dir

    year = f"{run_date:%Y}"
    quarter = _quarter_from_date(run_date)
    partition_dir = target_root / year / quarter
    ensure_dir(partition_dir)

    downloader = Downloader(
        download_folder=str(partition_dir),
        user_agent=settings.sec_edgar_user_agent,
    )

    try:
        file_paths = _download_filings(downloader, filing_type, ticker, amount)
        if not file_paths and filing_type == "10-Q":
            logger.warning(f"No {filing_type} found for {ticker}, retrying with 10-K")
            filing_type = "10-K"
            file_paths = _download_filings(downloader, filing_type, ticker, amount)
    except Exception as exc:  # pragma: no cover - external dependency path
        logger.error(f"SEC download failed for {ticker}: {exc}")
        raise SecDownloadError from exc

    # Build simple manifest so raw layer is queryable
    manifest = pd.DataFrame(
        {
            "ticker": [ticker] * len(file_paths),
            "filing_type": [filing_type] * len(file_paths),
            "download_time": [datetime.utcnow()] * len(file_paths),
            "file_path": [str(Path(p).resolve()) for p in file_paths],
            "source": ["sec_edgar"] * len(file_paths),
        }
    )

    output_path = partition_dir / f"{ticker}.parquet"
    manifest.to_parquet(output_path, index=False)
    logger.info(f"Saved SEC manifest for {ticker} to {output_path}")
    return manifest


__all__ = ["download_fundamentals", "SecDownloadError"]

