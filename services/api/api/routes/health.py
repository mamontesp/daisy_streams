"""FastAPI route handler for liveness and readiness health checks."""

from __future__ import annotations

from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse

from services.api.api.config import APISettings
from services.api.api.db.repository import APIRepository
from services.api.api.dependencies import get_api_settings, get_repository
from services.api.api.models import HealthResponse

router = APIRouter(tags=["Health"])


@router.get(
    "/healthz",
    response_model=HealthResponse,
    summary="Service health probe",
    description="Check API runtime status and PostgreSQL database connectivity.",
)
def health_check(
    settings: APISettings = Depends(get_api_settings),
    repo: APIRepository = Depends(get_repository),
) -> JSONResponse:
    """Verify service health and database connection."""
    db_ok = repo.check_health()
    if db_ok:
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "status": "healthy",
                "database": "connected",
                "environment": settings.environment,
            },
        )
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={
            "status": "degraded",
            "database": "disconnected",
            "environment": settings.environment,
        },
    )
