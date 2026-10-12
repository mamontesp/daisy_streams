"""Abstract base contract for sync state persistence."""

from __future__ import annotations

from typing import Optional, Protocol
from snowflake_adapter.models.state import SyncState


class StateStore(Protocol):
    """Protocol defining sync state persistence capabilities."""

    def load_state(self, source_table: str) -> Optional[SyncState]:
        """Load the persisted sync state for the given source table, or None if not found."""
        ...

    def save_state(self, state: SyncState) -> None:
        """Persist the sync state durably."""
        ...
