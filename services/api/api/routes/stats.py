"""FastAPI route handler for the GET /stats stream aggregation endpoint."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from services.api.api.db.repository import APIRepository
from services.api.api.dependencies import get_repository
from services.api.api.models import StreamStatsResponse

router = APIRouter(tags=["Stats"])


@router.get(
    "/stats",
    response_model=StreamStatsResponse,
    summary="Retrieve stream processing and health metrics",
    description=(
        "Returns aggregated event counts by type, pipeline latency lag metrics, "
        "dead-letter queue (DLQ) counts, active entity count, and high watermarks (REQ-C04)."
    ),
)
def get_stream_stats(
    repo: APIRepository = Depends(get_repository),
) -> StreamStatsResponse:
    """Compute and return real-time pipeline metrics."""
    return repo.get_stats()
