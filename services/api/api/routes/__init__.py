"""API route definitions."""

from services.api.api.routes.events import router as events_router
from services.api.api.routes.health import router as health_router
from services.api.api.routes.stats import router as stats_router

__all__ = ["events_router", "health_router", "stats_router"]
