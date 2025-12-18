from __future__ import annotations

from datetime import date, datetime
from typing import Optional


def parse_run_date(run_date: Optional[str] = None) -> date:
    """Return a date object from YYYY-MM-DD string or today if None."""
    if run_date:
        return datetime.strptime(run_date, "%Y-%m-%d").date()
    return date.today()


def date_partition(dt: date) -> str:
    """Return partition string YYYY/MM/DD."""
    return f"{dt:%Y/%m/%d}"


__all__ = ["parse_run_date", "date_partition"]

