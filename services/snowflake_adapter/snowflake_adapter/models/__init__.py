"""Data models for Snowflake Adapter."""

from __future__ import annotations

from snowflake_adapter.models.envelope import EventEnvelope
from snowflake_adapter.models.state import SyncState

__all__ = ["EventEnvelope", "SyncState"]
