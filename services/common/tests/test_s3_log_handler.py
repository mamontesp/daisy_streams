"""Unit tests for shared S3BufferedLogHandler."""

from __future__ import annotations

import io
import json
import logging
from typing import Any
import pytest
from botocore.exceptions import ClientError

from services.common.s3_log_handler import S3BufferedLogHandler


class MockS3Client:
    """Mock boto3 S3 client for testing S3BufferedLogHandler."""

    def __init__(self) -> None:
        self.buckets: set[str] = set()
        self.objects: dict[str, dict[str, bytes]] = {}
        self.fail_put = False

    def head_bucket(self, Bucket: str) -> dict[str, Any]:
        if Bucket not in self.buckets:
            raise ClientError(
                error_response={"Error": {"Code": "404", "Message": "Not Found"}},
                operation_name="HeadBucket",
            )
        return {}

    def create_bucket(self, Bucket: str) -> dict[str, Any]:
        self.buckets.add(Bucket)
        if Bucket not in self.objects:
            self.objects[Bucket] = {}
        return {}

    def put_object(self, Bucket: str, Key: str, Body: bytes, ContentType: str = "application/x-ndjson") -> dict[str, Any]:
        if self.fail_put:
            raise RuntimeError("Simulated S3 network failure")
        if Bucket not in self.buckets:
            self.create_bucket(Bucket)
        self.objects[Bucket][Key] = Body
        return {}


def test_s3_log_handler_buffers_and_flushes() -> None:
    """Verify that S3BufferedLogHandler buffers records and flushes to S3 on flush() call."""
    mock_client = MockS3Client()
    mock_client.create_bucket("daisy-logs")

    handler = S3BufferedLogHandler(
        service_name="test-service",
        bucket="daisy-logs",
        buffer_size=10,
        s3_client=mock_client,
    )
    handler.setFormatter(logging.Formatter('{"message": "%(message)s"}'))

    logger = logging.getLogger("test_logger_1")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    logger.info("First message")
    logger.info("Second message")

    # Buffer should hold 2 messages without flushing yet
    assert len(handler._buffer) == 2

    # Explicit flush
    flushed_key = handler.flush()
    assert flushed_key is not None
    assert flushed_key.startswith("test-service/year=")
    assert flushed_key.endswith(".jsonl")

    # Verify object content in S3 mock
    body_bytes = mock_client.objects["daisy-logs"][flushed_key]
    lines = body_bytes.decode("utf-8").strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0])["message"] == "First message"
    assert json.loads(lines[1])["message"] == "Second message"


def test_s3_log_handler_threshold_auto_flush() -> None:
    """Verify that S3BufferedLogHandler flushes automatically when buffer_size is reached."""
    mock_client = MockS3Client()
    mock_client.create_bucket("daisy-logs")

    handler = S3BufferedLogHandler(
        service_name="auto-service",
        bucket="daisy-logs",
        buffer_size=3,
        s3_client=mock_client,
    )
    handler.setFormatter(logging.Formatter('{"msg": "%(message)s"}'))

    logger = logging.getLogger("test_logger_auto")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    logger.info("msg 1")
    logger.info("msg 2")
    assert len(mock_client.objects.get("daisy-logs", {})) == 0

    # 3rd message hits buffer_size=3 -> triggers flush
    logger.info("msg 3")
    assert len(mock_client.objects.get("daisy-logs", {})) == 1


def test_s3_log_handler_batch_id_naming() -> None:
    """Verify that flushing with a batch_id includes batch_{batch_id} in the key."""
    mock_client = MockS3Client()
    mock_client.create_bucket("daisy-logs")

    handler = S3BufferedLogHandler(
        service_name="adapter",
        bucket="daisy-logs",
        s3_client=mock_client,
    )
    handler.setFormatter(logging.Formatter("%(message)s"))

    record = logging.LogRecord(
        name="test", level=logging.INFO, pathname="", lineno=0, msg="batch record", args=(), exc_info=None
    )
    handler.emit(record)

    flushed_key = handler.flush(batch_id="uuid-1234")
    assert flushed_key is not None
    assert "adapter/year=" in flushed_key
    assert "batch_uuid-1234_" in flushed_key
    assert flushed_key.endswith(".jsonl")


def test_s3_log_handler_error_isolation() -> None:
    """Verify that S3 upload errors do not raise exceptions or crash the caller."""
    mock_client = MockS3Client()
    mock_client.fail_put = True

    handler = S3BufferedLogHandler(
        service_name="failing-service",
        bucket="daisy-logs",
        buffer_size=1,
        s3_client=mock_client,
    )
    handler.setFormatter(logging.Formatter("%(message)s"))

    logger = logging.getLogger("test_logger_fail")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    # Should not raise exception
    logger.info("Should not crash")


def test_s3_log_handler_ensure_bucket_creation() -> None:
    """Verify handler creates bucket if it doesn't already exist."""
    mock_client = MockS3Client()
    assert "daisy-logs" not in mock_client.buckets

    handler = S3BufferedLogHandler(
        service_name="init-service",
        bucket="daisy-logs",
        s3_client=mock_client,
    )
    handler._bucket_checked = False  # Force check
    handler._ensure_bucket_exists()

    assert "daisy-logs" in mock_client.buckets
