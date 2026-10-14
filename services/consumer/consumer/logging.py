"""Structured JSON logging with correlation ID context for Kafka Consumer Service."""

from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any, Optional

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


_s3_log_handler: Optional[Any] = None


def configure_logging(level: str = "INFO", enable_s3: Optional[bool] = None) -> None:
    """Configure root logger with structured JSON formatting and S3 log persistence."""
    global _s3_log_handler
    handler = logging.StreamHandler(sys.stdout)
    formatter = JSONFormatter()
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(level.upper())
    handlers: list[logging.Handler] = [handler]

    try:
        from services.common.s3_log_handler import S3BufferedLogHandler
        s3_handler = S3BufferedLogHandler(
            service_name="consumer",
            enabled=enable_s3,
        )
        s3_handler.setFormatter(formatter)
        if s3_handler.enabled:
            handlers.append(s3_handler)
            _s3_log_handler = s3_handler
    except Exception as exc:
        sys.stderr.write(f"[configure_logging] S3 logging handler not attached: {exc}\n")

    root_logger.handlers = handlers


def get_s3_log_handler() -> Optional[Any]:
    """Get active S3 logging handler instance if configured."""
    return _s3_log_handler


def get_logger(name: str) -> logging.Logger:
    """Get named logger instance."""
    return logging.getLogger(name)
