"""GCS storage abstraction for syncing data between local and cloud."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from loguru import logger

try:
    from google.cloud import storage
    from google.cloud.exceptions import NotFound
    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False
    logger.warning("google-cloud-storage not installed. GCS functions will not work.")


def upload_to_gcs(
    bucket_name: str,
    local_path: Path | str,
    gcs_path: str,
    client: Optional[storage.Client] = None,
) -> None:
    """
    Upload a file or directory to GCS.
    
    Args:
        bucket_name: Name of the GCS bucket
        local_path: Local file or directory path
        gcs_path: Destination path in GCS (without bucket name)
        client: Optional storage client (creates new if not provided)
    """
    if not GCS_AVAILABLE:
        raise ImportError("google-cloud-storage is required for GCS operations")
    
    local_path = Path(local_path)
    if not local_path.exists():
        raise FileNotFoundError(f"Local path does not exist: {local_path}")
    
    if client is None:
        client = storage.Client()
    
    bucket = client.bucket(bucket_name)
    
    if local_path.is_file():
        # Upload single file
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(str(local_path))
        logger.info(f"Uploaded {local_path} to gs://{bucket_name}/{gcs_path}")
    elif local_path.is_dir():
        # Upload directory recursively
        for file_path in local_path.rglob("*"):
            if file_path.is_file():
                # Preserve relative path structure
                relative_path = file_path.relative_to(local_path)
                blob_path = f"{gcs_path.rstrip('/')}/{relative_path.as_posix()}"
                blob = bucket.blob(blob_path)
                blob.upload_from_filename(str(file_path))
                logger.debug(f"Uploaded {file_path} to gs://{bucket_name}/{blob_path}")
        logger.info(f"Uploaded directory {local_path} to gs://{bucket_name}/{gcs_path}")
    else:
        raise ValueError(f"Path is neither a file nor directory: {local_path}")


def download_from_gcs(
    bucket_name: str,
    gcs_path: str,
    local_path: Path | str,
    client: Optional[storage.Client] = None,
) -> None:
    """
    Download a file or directory from GCS.
    
    Args:
        bucket_name: Name of the GCS bucket
        gcs_path: Source path in GCS (without bucket name)
        local_path: Local destination path
        client: Optional storage client (creates new if not provided)
    """
    if not GCS_AVAILABLE:
        raise ImportError("google-cloud-storage is required for GCS operations")
    
    local_path = Path(local_path)
    
    if client is None:
        client = storage.Client()
    
    bucket = client.bucket(bucket_name)
    
    try:
        # Try to get the blob (file)
        blob = bucket.blob(gcs_path)
        if blob.exists():
            # It's a file
            local_path.parent.mkdir(parents=True, exist_ok=True)
            blob.download_to_filename(str(local_path))
            logger.info(f"Downloaded gs://{bucket_name}/{gcs_path} to {local_path}")
        else:
            # Try as a directory prefix
            blobs = list(bucket.list_blobs(prefix=gcs_path.rstrip("/") + "/"))
            if not blobs:
                raise FileNotFoundError(f"No files found at gs://{bucket_name}/{gcs_path}")
            
            # Download all blobs with the prefix
            for blob in blobs:
                # Remove the prefix to get relative path
                relative_path = blob.name[len(gcs_path.rstrip("/") + "/"):]
                if not relative_path:  # Skip if it's the prefix itself
                    continue
                
                local_file = local_path / relative_path
                local_file.parent.mkdir(parents=True, exist_ok=True)
                blob.download_to_filename(str(local_file))
                logger.debug(f"Downloaded gs://{bucket_name}/{blob.name} to {local_file}")
            
            logger.info(f"Downloaded directory gs://{bucket_name}/{gcs_path} to {local_path}")
    except NotFound:
        raise FileNotFoundError(f"Path not found in GCS: gs://{bucket_name}/{gcs_path}")


def sync_marts_to_gcs(
    bucket_name: str,
    local_marts_dir: Path | str,
    gcs_prefix: str = "marts/",
    client: Optional[storage.Client] = None,
) -> None:
    """
    Sync marts data (signal_scores, positions, duckdb) to GCS.
    
    Args:
        bucket_name: Name of the GCS bucket
        local_marts_dir: Local marts directory path
        gcs_prefix: Prefix path in GCS (default: "marts/")
        client: Optional storage client
    """
    local_marts_dir = Path(local_marts_dir)
    
    if not local_marts_dir.exists():
        logger.warning(f"Marts directory does not exist: {local_marts_dir}")
        return
    
    # Sync signal_scores
    signal_scores_dir = local_marts_dir / "signal_scores"
    if signal_scores_dir.exists():
        upload_to_gcs(
            bucket_name,
            signal_scores_dir,
            f"{gcs_prefix}signal_scores/",
            client=client,
        )
    
    # Sync positions
    positions_dir = local_marts_dir / "positions"
    if positions_dir.exists():
        upload_to_gcs(
            bucket_name,
            positions_dir,
            f"{gcs_prefix}positions/",
            client=client,
        )
    
    # Sync DuckDB database
    duckdb_dir = local_marts_dir / "duckdb"
    if duckdb_dir.exists():
        upload_to_gcs(
            bucket_name,
            duckdb_dir,
            f"{gcs_prefix}duckdb/",
            client=client,
        )
    
    logger.info(f"Synced marts data from {local_marts_dir} to gs://{bucket_name}/{gcs_prefix}")


def sync_marts_from_gcs(
    bucket_name: str,
    local_marts_dir: Path | str,
    gcs_prefix: str = "marts/",
    client: Optional[storage.Client] = None,
) -> None:
    """
    Sync marts data from GCS to local filesystem.
    
    Args:
        bucket_name: Name of the GCS bucket
        local_marts_dir: Local marts directory path
        gcs_prefix: Prefix path in GCS (default: "marts/")
        client: Optional storage client
    """
    local_marts_dir = Path(local_marts_dir)
    local_marts_dir.mkdir(parents=True, exist_ok=True)
    
    if client is None:
        client = storage.Client()
    
    bucket = client.bucket(bucket_name)
    
    # Check if bucket exists
    try:
        if not bucket.exists():
            logger.warning(f"Bucket does not exist: {bucket_name}")
            return
    except Exception as e:
        logger.warning(f"Could not check bucket existence: {e}")
        return
    
    # Sync signal_scores
    signal_scores_local = local_marts_dir / "signal_scores"
    try:
        download_from_gcs(
            bucket_name,
            f"{gcs_prefix}signal_scores/",
            signal_scores_local,
            client=client,
        )
    except FileNotFoundError:
        logger.info(f"No signal_scores found in GCS, skipping")
    
    # Sync positions
    positions_local = local_marts_dir / "positions"
    try:
        download_from_gcs(
            bucket_name,
            f"{gcs_prefix}positions/",
            positions_local,
            client=client,
        )
    except FileNotFoundError:
        logger.info(f"No positions found in GCS, skipping")
    
    # Sync DuckDB database
    duckdb_local = local_marts_dir / "duckdb"
    try:
        download_from_gcs(
            bucket_name,
            f"{gcs_prefix}duckdb/",
            duckdb_local,
            client=client,
        )
    except FileNotFoundError:
        logger.info(f"No duckdb found in GCS, skipping")
    
    logger.info(f"Synced marts data from gs://{bucket_name}/{gcs_prefix} to {local_marts_dir}")


__all__ = [
    "upload_to_gcs",
    "download_from_gcs",
    "sync_marts_to_gcs",
    "sync_marts_from_gcs",
    "GCS_AVAILABLE",
]

