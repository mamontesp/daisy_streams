"""Atomic local file state store implementation."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Optional
from snowflake_adapter.logging import get_logger
from snowflake_adapter.models.state import SyncState

logger = get_logger(__name__)


class LocalFileStateStore:
    """Persists sync state into a local JSON file using atomic file replacement."""

    def __init__(self, file_path: str | Path = ".daisy_sync_state.json") -> None:
        self.file_path = Path(file_path).resolve()

    def _read_all_states(self) -> dict[str, dict]:
        """Read the root JSON object containing all table states."""
        if not self.file_path.exists():
            return {}
        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if not content:
                    return {}
                return json.loads(content)
        except Exception as e:
            logger.warning("Failed to read existing state file %s: %s", self.file_path, e)
            return {}

    def load_state(self, source_table: str) -> Optional[SyncState]:
        """Load sync state for table."""
        states = self._read_all_states()
        table_key = source_table.lower()
        if table_key in states:
            return SyncState(**states[table_key])
        return None

    def save_state(self, state: SyncState) -> None:
        """Persist state atomically using write-to-temp and atomic rename."""
        states = self._read_all_states()
        states[state.source_table.lower()] = state.to_contract_dict()
        if state.last_entity_key:
            states[state.source_table.lower()]["last_entity_key"] = state.last_entity_key

        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        dir_path = self.file_path.parent
        with tempfile.NamedTemporaryFile("w", dir=dir_path, delete=False, encoding="utf-8") as tf:
            json.dump(states, tf, indent=2)
            temp_name = tf.name

        os.replace(temp_name, self.file_path)
        logger.debug("Successfully saved state for table %s to %s", state.source_table, self.file_path)
