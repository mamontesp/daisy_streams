"""Structured JSON logging with request correlation IDs."""

from __future__ import annotations

import contextvars
import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

request_id_ctx: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "request_id", default=None
)


class JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        log_obj: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        req_id = request_id_ctx.get()
        if req_id:
            log_obj["request_id"] = req_id

        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_obj)


_s3_log_handler: Optional[Any] = None


def setup_logging(level: str = "INFO", enable_s3: Optional[bool] = None) -> None:
    """Configure root logger with JSON formatting and S3 log persistence."""
    global _s3_log_handler
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Avoid duplicate handlers if re-initialized
    for handler in list(root.handlers):
        root.removeHandler(handler)

    formatter = JsonFormatter()
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    try:
        from services.common.s3_log_handler import S3BufferedLogHandler
        s3_handler = S3BufferedLogHandler(
            service_name="api",
            enabled=enable_s3,
        )
        s3_handler.setFormatter(formatter)
        if s3_handler.enabled:
            root.addHandler(s3_handler)
            _s3_log_handler = s3_handler
    except Exception as exc:
        sys.stderr.write(f"[setup_logging] S3 logging handler not attached: {exc}\n")


def get_s3_log_handler() -> Optional[Any]:
    """Get active S3 logging handler instance if configured."""
    return _s3_log_handler
