"""State persistence package and factory."""

from __future__ import annotations

from typing import Optional
from snowflake_adapter.config import Settings, get_settings
from snowflake_adapter.state.base import StateStore
from snowflake_adapter.state.local_file import LocalFileStateStore
from snowflake_adapter.state.minio_store import GarageStateStore, MinioStateStore, S3StateStore

__all__ = [
    "StateStore",
    "LocalFileStateStore",
    "S3StateStore",
    "GarageStateStore",
    "MinioStateStore",
    "get_state_store",
]


def get_state_store(settings: Optional[Settings] = None) -> StateStore:
    """Instantiate configured state store based on settings."""
    cfg = settings or get_settings()

    if cfg.sync_state_storage == "garage":
        return GarageStateStore(
            endpoint=cfg.garage_endpoint,
            access_key=cfg.garage_access_key,
            secret_key=cfg.garage_secret_key,
            bucket=cfg.garage_bucket,
            region_name=cfg.garage_region,
            secure=cfg.garage_secure,
        )
    elif cfg.sync_state_storage == "minio":
        return MinioStateStore(
            endpoint=cfg.minio_endpoint,
            access_key=cfg.minio_access_key,
            secret_key=cfg.minio_secret_key,
            bucket=cfg.minio_bucket,
            secure=cfg.minio_secure,
        )
    elif cfg.sync_state_storage == "local":
        return LocalFileStateStore(file_path=cfg.sync_state_file_path)
    else:
        return LocalFileStateStore(file_path=cfg.sync_state_file_path)
