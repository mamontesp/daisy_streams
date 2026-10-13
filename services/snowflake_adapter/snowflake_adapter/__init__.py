"""Snowflake Adapter Service package."""

from __future__ import annotations

__all__ = [
    "SnowflakeAdapter",
    "SyncResult",
    "SnowflakeClient",
    "SnowflakeConnectorClient",
    "MockSnowflakeClient",
]


def __getattr__(name: str):
    if name in ("SnowflakeAdapter", "SyncResult"):
        from snowflake_adapter.adapter import SnowflakeAdapter, SyncResult

        return locals()[name]
    if name in ("SnowflakeClient", "SnowflakeConnectorClient", "MockSnowflakeClient"):
        from snowflake_adapter.client import SnowflakeClient, SnowflakeConnectorClient, MockSnowflakeClient

        return locals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
