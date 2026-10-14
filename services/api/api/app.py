"""FastAPI application factory, middleware, and route registration."""

from __future__ import annotations

import uuid
from typing import Callable

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from services.api.api.config import APISettings
from services.api.api.logging import request_id_ctx, setup_logging
from services.api.api.routes import events_router, health_router, stats_router


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Assigns or propagates a unique correlation ID for every request."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        req_id = request.headers.get("X-Request-ID") or request.headers.get("X-Correlation-ID")
        if not req_id:
            req_id = str(uuid.uuid4())

        token = request_id_ctx.set(req_id)
        try:
            response = await call_next(request)
            response.headers["X-Request-ID"] = req_id
            return response
        finally:
            request_id_ctx.reset(token)


def create_app(settings: APISettings | None = None) -> FastAPI:
    """Application factory initializing FastAPI with configured middleware and routes."""
    cfg = settings or APISettings()
    setup_logging(cfg.log_level)

    application = FastAPI(
        title="Daisy Streams Event-Inspection API",
        description=(
            "Inspection service for Daisy Streams PostgreSQL sink, serving "
            "paginated event logs and real-time pipeline latency/health metrics (REQ-C04)."
        ),
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # CORS Middleware
    application.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Correlation ID Tracking Middleware
    application.add_middleware(CorrelationIdMiddleware)

    # Mount Routers
    application.include_router(health_router)
    application.include_router(events_router)
    application.include_router(stats_router)

    return application


# Global application instance for ASGI servers (e.g. uvicorn)
app = create_app()
