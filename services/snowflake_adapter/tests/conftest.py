"""Pytest fixtures and mock services for Snowflake Adapter."""

from __future__ import annotations

import io
from typing import Any
import pytest
from botocore.exceptions import ClientError

from snowflake_adapter.adapter import SnowflakeAdapter
from snowflake_adapter.client import MockSnowflakeClient
from snowflake_adapter.config import Settings
from snowflake_adapter.state.local_file import LocalFileStateStore
from snowflake_adapter.state.minio_store import MinioStateStore
from snowflake_adapter.transport.memory_producer import InMemoryEventProducer


class MockBotoS3Client:
    """Mock boto3 S3 client storing buckets and objects in memory."""

    def __init__(self) -> None:
        self.buckets: set[str] = set()
        self.objects: dict[str, dict[str, bytes]] = {}

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

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        if Bucket not in self.buckets or Key not in self.objects.get(Bucket, {}):
            raise ClientError(
                error_response={"Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist."}},
                operation_name="GetObject",
            )
        data = self.objects[Bucket][Key]
        return {"Body": io.BytesIO(data)}

    def put_object(self, Bucket: str, Key: str, Body: bytes, ContentType: str = "application/json") -> dict[str, Any]:
        if Bucket not in self.buckets:
            self.create_bucket(Bucket)
        self.objects[Bucket][Key] = Body
        return {}


@pytest.fixture
def test_settings() -> Settings:
    """Settings fixture configured for local unit testing."""
    return Settings(
        _env_file=None,
        SNOWFLAKE_DATABASE="daisy_streams",
        SNOWFLAKE_SCHEMA="ingestion",
        SNOWFLAKE_TABLE="ORDERS",
        SNOWFLAKE_WATERMARK_COLUMN="O_ORDERDATE",
        SNOWFLAKE_ENTITY_KEY_COLUMN="O_ORDERKEY",
        SYNC_INITIAL_BOUND=10000,
        SYNC_BATCH_SIZE=5000,
        SYNC_STATE_STORAGE="local",
        SYNC_STATE_FILE_PATH=".test_sync_state.json",
        MINIO_BUCKET="test-state-bucket",
    )


@pytest.fixture
def mock_s3_client() -> MockBotoS3Client:
    """Mock S3 client fixture."""
    return MockBotoS3Client()


@pytest.fixture
def minio_state_store(mock_s3_client: MockBotoS3Client) -> MinioStateStore:
    """MinioStateStore backed by in-memory mock client."""
    return MinioStateStore(
        bucket="test-state-bucket",
        s3_client=mock_s3_client,
    )


@pytest.fixture
def local_state_store(tmp_path: Any) -> LocalFileStateStore:
    """LocalFileStateStore fixture writing to temporary directory."""
    state_file = tmp_path / "test_state.json"
    return LocalFileStateStore(file_path=state_file)


@pytest.fixture
def memory_producer() -> InMemoryEventProducer:
    """In-memory event producer fixture."""
    return InMemoryEventProducer()


@pytest.fixture
def sample_orders_dataset() -> list[dict[str, Any]]:
    """Synthetic orders dataset conforming to TPCH_SF1.ORDERS joined with CUSTOMER."""
    rows = []
    for i in range(1, 10001):
        day = (i % 28) + 1
        month = ((i // 28) % 12) + 1
        year = 1992 + (i // 336)
        date_str = f"{year:04d}-{month:02d}-{day:02d}"

        rows.append({
            "O_ORDERKEY": i,
            "O_CUSTKEY": 1000 + (i % 100),
            "O_ORDERSTATUS": "O" if i % 2 == 0 else "F",
            "O_TOTALPRICE": 100.0 + (i * 1.5),
            "O_ORDERDATE": date_str,
            "O_ORDERPRIORITY": "1-URGENT",
            "O_CLERK": f"Clerk#00000{i % 50:03d}",
            "O_SHIPPRIORITY": 0,
            "O_COMMENT": f"Sample order row #{i}",
            "C_NAME": f"Customer#{1000 + (i % 100)}",
            "C_PHONE": "123-456-7890",
            "C_ACCTBAL": 500.0,
            "C_MKTSEGMENT": "BUILDING",
        })
    return rows


@pytest.fixture
def mock_snowflake_client(sample_orders_dataset: list[dict[str, Any]]) -> MockSnowflakeClient:
    """Mock Snowflake client populated with synthetic orders."""
    return MockSnowflakeClient(initial_records=sample_orders_dataset)
