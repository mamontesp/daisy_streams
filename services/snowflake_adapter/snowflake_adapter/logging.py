"""Structured JSON logging with correlation ID context."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Optional
from contextvars import ContextVar

correlation_batch_id: ContextVar[Optional[str]] = ContextVar("correlation_batch_id", default=None)
correlation_event_id: ContextVar[Optional[str]] = ContextVar("correlation_event_id", default=None)


class JSONFormatter(logging.Formatter):
    """Custom formatter outputting logs as structured JSON."""

    def format(self, record: logging.LogRecord) -> str:
        log_payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        batch_id = correlation_batch_id.get()
        if batch_id:
            log_payload["batch_id"] = batch_id

        event_id = correlation_event_id.get()
        if event_id:
            log_payload["event_id"] = event_id

        for key in ("batch_id", "event_id", "component", "source_table", "records_count", "watermark"):
            if hasattr(record, key):
                log_payload[key] = getattr(record, key)

        if record.exc_info:
            log_payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_payload)


def configure_logging(level: str = "INFO") -> None:
    """Configure root logger with structured JSON formatting."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())

    root_logger = logging.getLogger()
    root_logger.setLevel(level.upper())
    root_logger.handlers = [handler]


def get_logger(name: str) -> logging.Logger:
    """Get named logger instance."""
    return logging.getLogger(name)
