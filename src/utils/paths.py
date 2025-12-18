from __future__ import annotations

from pathlib import Path


def ensure_dir(path: Path) -> None:
    """Create directory if it does not exist."""
    path.mkdir(parents=True, exist_ok=True)


def resolve_path(path_str: str) -> Path:
    """Resolve a string path to absolute Path."""
    return Path(path_str).expanduser().resolve()


__all__ = ["ensure_dir", "resolve_path"]

