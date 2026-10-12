"""State store contract model conforming strictly to Section 3 of spec-data-snowflake-adapter.md."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4
from pydantic import BaseModel, Field


class SyncState(BaseModel):
    """Sync state schema matching State Store Contract."""

    source_table: str = Field(description="Name of source table being synchronized")
    last_watermark: Optional[str] = Field(
        default=None,
        description="High watermark value (ISO timestamp string) marking last synced record",
    )
    last_batch_id: Optional[str] = Field(
        default=None,
        description="UUIDv4 identifier of last completed sync batch",
    )
    last_synced_at: Optional[str] = Field(
        default=None,
        description="ISO timestamp when last sync occurred",
    )
    records_synced: int = Field(
        default=0,
        description="Cumulative or batch total records synchronized",
    )
    last_entity_key: Optional[str] = Field(
        default=None,
        description="Secondary tie-breaker cursor for identical timestamps",
    )

    def to_contract_dict(self) -> dict[str, Any]:
        """Return dict representation matching the exact specification schema."""
        return {
            "source_table": self.source_table,
            "last_watermark": self.last_watermark,
            "last_batch_id": self.last_batch_id,
            "last_synced_at": self.last_synced_at,
            "records_synced": self.records_synced,
        }

    def to_json(self) -> str:
        """Serialize state to formatted JSON string."""
        data = self.to_contract_dict()
        if self.last_entity_key is not None:
            data["last_entity_key"] = self.last_entity_key
        return json.dumps(data, indent=2)

    @classmethod
    def from_json(cls, raw: str | bytes) -> SyncState:
        """Parse state from JSON string or bytes."""
        data = json.loads(raw)
        return cls(**data)

    @classmethod
    def initialize_empty(cls, source_table: str) -> SyncState:
        """Create empty state for a new table sync."""
        return cls(
            source_table=source_table,
            last_watermark=None,
            last_batch_id=str(uuid4()),
            last_synced_at=datetime.now(timezone.utc).isoformat(),
            records_synced=0,
        )
