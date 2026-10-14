"""Unit tests for MinIO and LocalFile state store persistence and restart recovery."""

from __future__ import annotations

import json
from uuid import uuid4
from snowflake_adapter.models.state import SyncState
from snowflake_adapter.state.local_file import LocalFileStateStore
from snowflake_adapter.state.minio_store import MinioStateStore


def test_local_file_state_store_persistence(tmp_path) -> None:
    """Test LocalFileStateStore save, load, and restart recovery."""
    state_file = tmp_path / "subdir" / "sync_state.json"
    store = LocalFileStateStore(file_path=state_file)

    assert store.load_state("orders") is None

    batch_id = str(uuid4())
    state = SyncState(
        source_table="orders",
        last_watermark="1992-05-01T00:00:00Z",
        last_batch_id=batch_id,
        last_synced_at="2026-10-12T12:00:00Z",
        records_synced=15000,
    )
    store.save_state(state)
    assert state_file.exists()

    restart_store = LocalFileStateStore(file_path=state_file)
    recovered = restart_store.load_state("orders")

    assert recovered is not None
    assert recovered.source_table == "orders"
    assert recovered.last_watermark == "1992-05-01T00:00:00Z"
    assert recovered.last_batch_id == batch_id
    assert recovered.records_synced == 15000


def test_minio_state_store_persistence(mock_s3_client) -> None:
    """Test MinioStateStore bucket creation, object checkpointing, and restart recovery."""
    bucket = "daisy-state"
    store = MinioStateStore(bucket=bucket, s3_client=mock_s3_client)

    assert bucket in mock_s3_client.buckets
    assert store.load_state("orders") is None

    batch_id = str(uuid4())
    state = SyncState(
        source_table="orders",
        last_watermark="2026-10-12T00:00:00Z",
        last_batch_id=batch_id,
        last_synced_at="2026-10-12T11:00:00Z",
        records_synced=25000,
    )
    store.save_state(state)

    key = "orders/state.json"
    assert key in mock_s3_client.objects[bucket]
    raw_bytes = mock_s3_client.objects[bucket][key]
    data = json.loads(raw_bytes.decode("utf-8"))
    assert data["last_watermark"] == "2026-10-12T00:00:00Z"
    assert data["records_synced"] == 25000

    restart_store = MinioStateStore(bucket=bucket, s3_client=mock_s3_client)
    recovered = restart_store.load_state("orders")

    assert recovered is not None
    assert recovered.source_table == "orders"
    assert recovered.last_watermark == "2026-10-12T00:00:00Z"
    assert recovered.last_batch_id == batch_id
    assert recovered.records_synced == 25000


def test_garage_state_store_persistence(mock_s3_client) -> None:
    """Test GarageStateStore bucket creation, object checkpointing, and restart recovery."""
    from snowflake_adapter.state.minio_store import GarageStateStore
    bucket = "daisy-state"
    store = GarageStateStore(bucket=bucket, s3_client=mock_s3_client)

    assert bucket in mock_s3_client.buckets
    assert store.load_state("orders") is None

    batch_id = str(uuid4())
    state = SyncState(
        source_table="orders",
        last_watermark="2026-10-12T00:00:00Z",
        last_batch_id=batch_id,
        last_synced_at="2026-10-12T11:00:00Z",
        records_synced=10000,
    )
    store.save_state(state)

    key = "orders/state.json"
    assert key in mock_s3_client.objects[bucket]

    restart_store = GarageStateStore(bucket=bucket, s3_client=mock_s3_client)
    recovered = restart_store.load_state("orders")
    assert recovered is not None
    assert recovered.records_synced == 10000


def test_state_store_factory_garage(mock_s3_client, monkeypatch) -> None:
    """Test get_state_store factory instantiates GarageStateStore when configured."""
    import boto3
    from snowflake_adapter.state import get_state_store
    from snowflake_adapter.config import Settings
    from snowflake_adapter.state.minio_store import S3StateStore

    monkeypatch.setattr(boto3, "client", lambda *args, **kwargs: mock_s3_client)

    settings = Settings(
        _env_file=None,
        SYNC_STATE_STORAGE="garage",
        GARAGE_BUCKET="daisy-state",
    )
    store = get_state_store(settings)
    assert isinstance(store, S3StateStore)
