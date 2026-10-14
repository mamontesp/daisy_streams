"""FastAPI route handler for the GET /events endpoint."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query

from services.api.api.db.repository import APIRepository
from services.api.api.dependencies import get_repository
from services.api.api.models import EventListResponse

router = APIRouter(tags=["Events"])


@router.get(
    "/events",
    response_model=EventListResponse,
    summary="List paginated stream events",
    description=(
        "Retrieve paginated raw events from the append-only ledger filtered "
        "by entity_key, type, and temporal boundaries (REQ-C04)."
    ),
)
def list_events(
    entity_key: Optional[str] = Query(
        default=None,
        description="Filter by entity identifier (e.g., order ID)",
    ),
    type: Optional[str] = Query(
        default=None,
        description="Filter by event action type (e.g., insert, update)",
    ),
    event_type: Optional[str] = Query(
        default=None,
        description="Alias for 'type' filter parameter",
    ),
    start: Optional[datetime] = Query(
        default=None,
        description="Filter events occurred on or after timestamp (ISO 8601)",
    ),
    start_time: Optional[datetime] = Query(
        default=None,
        description="Alias for 'start' filter parameter",
    ),
    end: Optional[datetime] = Query(
        default=None,
        description="Filter events occurred on or before timestamp (ISO 8601)",
    ),
    end_time: Optional[datetime] = Query(
        default=None,
        description="Alias for 'end' filter parameter",
    ),
    limit: int = Query(
        default=50,
        ge=1,
        le=1000,
        description="Maximum number of records to return per page",
    ),
    offset: int = Query(
        default=0,
        ge=0,
        description="Number of records to skip for pagination",
    ),
    repo: APIRepository = Depends(get_repository),
) -> EventListResponse:
    """Execute paginated query over the event_log table."""
    resolved_type = type if type is not None else event_type
    resolved_start = start if start is not None else start_time
    resolved_end = end if end is not None else end_time

    return repo.get_events(
        entity_key=entity_key,
        event_type=resolved_type,
        start_time=resolved_start,
        end_time=resolved_end,
        limit=limit,
        offset=offset,
    )
