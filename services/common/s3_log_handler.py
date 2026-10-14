"""Shared S3-compatible buffered logging handler for Daisy Streams services.

Persists structured JSON logs into Garage / MinIO / AWS S3 buckets as partitioned
JSON Lines (.jsonl) files with batch_id and timestamp correlation.
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

try:
    import boto3
    import botocore.client
    from botocore.exceptions import ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None  # type: ignore[assignment]
    botocore = None  # type: ignore[assignment]
    ClientError = Exception  # type: ignore[assignment,misc]


class S3BufferedLogHandler(logging.Handler):
    """
    Thread-safe buffered logging handler that flushes structured log lines
    to an S3-compatible object storage bucket (e.g. Garage S3) as JSON Lines (.jsonl).
    """

    def __init__(
        self,
        service_name: str,
        bucket: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region_name: str = "garage",
        buffer_size: int = 50,
        flush_interval_sec: float = 30.0,
        enabled: Optional[bool] = None,
        s3_client: Optional[Any] = None,
        level: int = logging.NOTSET,
    ) -> None:
        super().__init__(level=level)
        self.service_name = service_name
        self.bucket = bucket or os.getenv("GARAGE_LOGS_BUCKET", "daisy-logs")
        
        # Resolve endpoint URL
        endpoint_env = endpoint_url or os.getenv("GARAGE_ENDPOINT", "localhost:3900")
        if not endpoint_env.startswith("http://") and not endpoint_env.startswith("https://"):
            self.endpoint_url = f"http://{endpoint_env}"
        else:
            self.endpoint_url = endpoint_env

        self.access_key = access_key or os.getenv("GARAGE_ACCESS_KEY", "GKdaisystreamskey01234")
        self.secret_key = secret_key or os.getenv(
            "GARAGE_SECRET_KEY", "daisystreamssecretkey0123456789abcdef0123456789abcdef"
        )
        self.region_name = region_name
        self.buffer_size = buffer_size
        self.flush_interval_sec = flush_interval_sec

        # Enabled toggle (default True unless explicitly disabled)
        if enabled is not None:
            self.enabled = enabled
        else:
            env_val = os.getenv("LOG_STORE_TO_S3", "true").lower()
            self.enabled = env_val not in ("false", "0", "no", "off")

        self._lock = threading.Lock()
        self._buffer: list[str] = []
        self._last_flush_time = time.monotonic()
        self._bucket_checked = False

        if s3_client is not None:
            self._client = s3_client
            self._bucket_checked = True
        elif self.enabled and BOTO3_AVAILABLE:
            try:
                self._client = boto3.client(
                    "s3",
                    endpoint_url=self.endpoint_url,
                    aws_access_key_id=self.access_key,
                    aws_secret_access_key=self.secret_key,
                    region_name=self.region_name,
                    config=botocore.client.Config(
                        s3={"addressing_style": "path"},
                        signature_version="s3v4",
                    ),
                )
            except Exception as exc:
                sys.stderr.write(f"[S3BufferedLogHandler] Failed to initialize S3 client: {exc}\n")
                self._client = None
                self.enabled = False
        else:
            self._client = None
            if not BOTO3_AVAILABLE:
                self.enabled = False

    def _ensure_bucket_exists(self) -> None:
        """Verify target bucket exists in Garage; create it if missing."""
        if self._bucket_checked or not self.enabled or self._client is None:
            return

        try:
            self._client.head_bucket(Bucket=self.bucket)
            self._bucket_checked = True
        except ClientError as e:
            error_code = str(e.response.get("Error", {}).get("Code", ""))
            if error_code in ("404", "NoSuchBucket"):
                try:
                    self._client.create_bucket(Bucket=self.bucket)
                    self._bucket_checked = True
                except Exception as create_exc:
                    sys.stderr.write(
                        f"[S3BufferedLogHandler] Warning: Could not create bucket '{self.bucket}': {create_exc}\n"
                    )
            else:
                self._bucket_checked = True
        except Exception as e:
            sys.stderr.write(
                f"[S3BufferedLogHandler] Warning: Could not check bucket '{self.bucket}': {e}\n"
            )

    def emit(self, record: logging.LogRecord) -> None:
        """Buffer formatted log record and flush when threshold or interval reached."""
        if not self.enabled:
            return

        try:
            formatted_msg = self.format(record)
            should_flush = False
            batch_id: Optional[str] = getattr(record, "batch_id", None)

            with self._lock:
                self._buffer.append(formatted_msg)
                elapsed = time.monotonic() - self._last_flush_time
                if len(self._buffer) >= self.buffer_size or elapsed >= self.flush_interval_sec:
                    should_flush = True

            if should_flush:
                self.flush(batch_id=batch_id)
        except Exception:
            self.handleError(record)

    def flush(
        self,
        custom_key: Optional[str] = None,
        batch_id: Optional[str] = None,
    ) -> Optional[str]:
        """
        Atomically flush current buffer into S3 as a .jsonl file.
        Returns the object key if flushed, or None if buffer was empty.
        """
        if not self.enabled or self._client is None:
            return None

        records_to_upload: list[str] = []
        with self._lock:
            if not self._buffer:
                return None
            records_to_upload = list(self._buffer)
            self._buffer.clear()
            self._last_flush_time = time.monotonic()

        self._ensure_bucket_exists()

        now = datetime.now(timezone.utc)
        if custom_key:
            object_key = custom_key
        else:
            date_prefix = f"{self.service_name}/year={now.year}/month={now.month:02d}/day={now.day:02d}"
            timestamp_str = now.strftime("%Y%m%dT%H%M%SZ")
            unique_suffix = str(uuid4())[:8]

            if batch_id:
                filename = f"batch_{batch_id}_{timestamp_str}_{unique_suffix}.jsonl"
            else:
                filename = f"{self.service_name}_{timestamp_str}_{unique_suffix}.jsonl"
            object_key = f"{date_prefix}/{filename}"

        payload_bytes = ("\n".join(records_to_upload) + "\n").encode("utf-8")

        try:
            self._client.put_object(
                Bucket=self.bucket,
                Key=object_key,
                Body=payload_bytes,
                ContentType="application/x-ndjson",
            )
            return object_key
        except Exception as exc:
            sys.stderr.write(
                f"[S3BufferedLogHandler] Failed to flush {len(records_to_upload)} logs to s3://{self.bucket}/{object_key}: {exc}\n"
            )
            # Re-enqueue failed records under lock (capped at 2x buffer_size to prevent runaway memory)
            with self._lock:
                if len(self._buffer) < self.buffer_size * 2:
                    self._buffer = records_to_upload + self._buffer
            return None

    def close(self) -> None:
        """Flush remaining log buffer and close handler."""
        try:
            self.flush()
        finally:
            super().close()
