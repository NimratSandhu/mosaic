from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd
from sec_edgar_downloader import Downloader
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from config.settings import get_settings
from logging_utils.setup import logger
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
) -> int:
    """Download filings and return number of filings downloaded."""
    return downloader.get(filing_type, ticker, limit=amount)


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

    # Downloader requires positional args: company_name, email_address, download_folder (optional)
    downloader = Downloader(
        settings.sec_edgar_company_name,
        settings.sec_edgar_user_email,
        download_folder=str(partition_dir),
    )

    try:
        num_downloaded = _download_filings(downloader, filing_type, ticker, amount)
        if num_downloaded == 0 and filing_type == "10-Q":
            logger.warning(f"No {filing_type} found for {ticker}, retrying with 10-K")
            filing_type = "10-K"
            num_downloaded = _download_filings(downloader, filing_type, ticker, amount)
    except ValueError as exc:
        # Handle invalid ticker (e.g., BRK.B) gracefully
        if "invalid" in str(exc).lower() or "cannot be mapped" in str(exc).lower():
            logger.warning(f"Skipping {ticker}: {exc}")
            # Return empty manifest
            manifest = pd.DataFrame(
                columns=["ticker", "filing_type", "download_time", "file_path", "source"]
            )
            output_path = partition_dir / f"{ticker}.parquet"
            manifest.to_parquet(output_path, index=False)
            return manifest
        raise SecDownloadError from exc
    except Exception as exc:  # pragma: no cover - external dependency path
        logger.error(f"SEC download failed for {ticker}: {exc}")
        raise SecDownloadError from exc

    # Find the actual downloaded files
    # Files are saved as: {partition_dir}/{ticker}/{filing_type}/...
    ticker_dir = partition_dir / ticker / filing_type
    if ticker_dir.exists():
        # Get all files recursively (excluding directories)
        file_paths = [str(p) for p in ticker_dir.rglob("*") if p.is_file()]
    else:
        file_paths = []
        if num_downloaded > 0:
            logger.warning(f"Expected {num_downloaded} files for {ticker} but none found in {ticker_dir}")

    # Build simple manifest so raw layer is queryable
    if file_paths:
        manifest = pd.DataFrame(
            {
                "ticker": [ticker] * len(file_paths),
                "filing_type": [filing_type] * len(file_paths),
                "download_time": [datetime.utcnow()] * len(file_paths),
                "file_path": [str(Path(p).resolve()) for p in file_paths],
                "source": ["sec_edgar"] * len(file_paths),
            }
        )
    else:
        # Create empty manifest if no files found
        manifest = pd.DataFrame(
            columns=["ticker", "filing_type", "download_time", "file_path", "source"]
        )

    output_path = partition_dir / f"{ticker}.parquet"
    manifest.to_parquet(output_path, index=False)
    logger.info(f"Saved SEC manifest for {ticker} to {output_path} ({len(file_paths)} files)")
    return manifest


__all__ = ["download_fundamentals", "SecDownloadError"]

