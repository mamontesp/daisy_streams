"""Integration tests verifying AC-A01, AC-A02, AC-A03, and AC-A04 compliance."""

from __future__ import annotations

from snowflake_adapter.adapter import SnowflakeAdapter
from snowflake_adapter.client import MockSnowflakeClient
from snowflake_adapter.config import Settings
from snowflake_adapter.state.minio_store import MinioStateStore
from snowflake_adapter.transport.memory_producer import InMemoryEventProducer


def test_ac_a01_initial_bounded_sync(
    test_settings: Settings,
    sample_orders_dataset: list[dict],
    minio_state_store: MinioStateStore,
    memory_producer: InMemoryEventProducer,
) -> None:
    """AC-A01: Given initial state, when adapter runs, it ingests 10k-50k rows and records watermark."""
    client = MockSnowflakeClient(initial_records=sample_orders_dataset)
    adapter = SnowflakeAdapter(
        settings=test_settings,
        client=client,
        state_store=minio_state_store,
        producer=memory_producer,
    )

    result = adapter.run_sync(table="ORDERS", batch_size=10000)

    assert result.is_initial_sync is True
    assert result.records_captured == 10000
    assert result.total_records_synced == 10000
    assert result.prior_watermark is None
    assert result.current_watermark is not None
    assert len(memory_producer.events) == 10000

    for ev in memory_producer.events:
        assert ev.event_type == "insert"
        assert ev.source == "snowflake.daisy_streams.ingestion.orders"
        assert ev.entity_key is not None

    persisted = minio_state_store.load_state("orders")
    assert persisted is not None
    assert persisted.last_watermark == result.current_watermark
    assert persisted.records_synced == 10000


def test_ac_a02_unmodified_source_emits_zero_events(
    test_settings: Settings,
    sample_orders_dataset: list[dict],
    minio_state_store: MinioStateStore,
    memory_producer: InMemoryEventProducer,
) -> None:
    """AC-A02: Given un-modified source, when adapter re-runs, zero new events are emitted."""
    client = MockSnowflakeClient(initial_records=sample_orders_dataset)
    adapter = SnowflakeAdapter(
        settings=test_settings,
        client=client,
        state_store=minio_state_store,
        producer=memory_producer,
    )

    res1 = adapter.run_sync(table="ORDERS", batch_size=10000)
    assert res1.records_captured == 10000
    watermark_after_first = res1.current_watermark

    memory_producer.clear()

    res2 = adapter.run_sync(table="ORDERS")

    assert res2.is_initial_sync is False
    assert res2.records_captured == 0
    assert res2.total_records_synced == 10000
    assert res2.current_watermark == watermark_after_first
    assert len(memory_producer.events) == 0


def test_ac_a03_change_simulation_delta_capture(
    test_settings: Settings,
    sample_orders_dataset: list[dict],
    minio_state_store: MinioStateStore,
    memory_producer: InMemoryEventProducer,
) -> None:
    """AC-A03: Given change simulator updates rows, when adapter runs, only updated/inserted rows are captured."""
    client = MockSnowflakeClient(initial_records=sample_orders_dataset)
    adapter = SnowflakeAdapter(
        settings=test_settings,
        client=client,
        state_store=minio_state_store,
        producer=memory_producer,
    )

    res1 = adapter.run_sync(table="ORDERS", batch_size=10000)
    assert res1.records_captured == 10000
    memory_producer.clear()

    future_date = "2026-10-12"
    simulated_changes = [
        {
            "O_ORDERKEY": 99901,
            "O_CUSTKEY": 1001,
            "O_ORDERSTATUS": "O",
            "O_TOTALPRICE": 500.0,
            "O_ORDERDATE": future_date,
            "O_ORDERPRIORITY": "1-URGENT",
            "O_CLERK": "Clerk#000000999",
            "O_SHIPPRIORITY": 0,
            "O_COMMENT": "Simulated new order 1",
            "EVENT_TYPE": "insert",
        },
        {
            "O_ORDERKEY": 99902,
            "O_CUSTKEY": 1002,
            "O_ORDERSTATUS": "F",
            "O_TOTALPRICE": 1200.0,
            "O_ORDERDATE": future_date,
            "O_ORDERPRIORITY": "2-HIGH",
            "O_CLERK": "Clerk#000000999",
            "O_SHIPPRIORITY": 0,
            "O_COMMENT": "Simulated new order 2",
            "EVENT_TYPE": "update",
        },
    ]

    client.records.extend(simulated_changes)

    res2 = adapter.run_sync(table="ORDERS")

    assert res2.records_captured == 2
    assert res2.total_records_synced == 10002
    assert len(memory_producer.events) == 2

    captured_keys = {ev.entity_key for ev in memory_producer.events}
    assert captured_keys == {"99901", "99902"}


def test_ac_a04_crash_restart_resumes_from_persisted_watermark(
    test_settings: Settings,
    sample_orders_dataset: list[dict],
    mock_s3_client,
    memory_producer: InMemoryEventProducer,
) -> None:
    """AC-A04: Given an adapter crash/restart, it resumes strictly from persisted watermark without re-emitting."""
    state_store_1 = MinioStateStore(bucket="daisy-state", s3_client=mock_s3_client)
    client_1 = MockSnowflakeClient(initial_records=sample_orders_dataset)
    adapter_1 = SnowflakeAdapter(
        settings=test_settings,
        client=client_1,
        state_store=state_store_1,
        producer=memory_producer,
    )

    res1 = adapter_1.run_sync(table="ORDERS", batch_size=10000)
    assert res1.records_captured == 10000
    saved_watermark = res1.current_watermark

    del adapter_1
    del state_store_1
    del client_1
    memory_producer.clear()

    sample_orders_dataset.append({
        "O_ORDERKEY": 88888,
        "O_CUSTKEY": 2000,
        "O_ORDERSTATUS": "O",
        "O_TOTALPRICE": 999.0,
        "O_ORDERDATE": "2026-10-12",
        "O_ORDERPRIORITY": "1-URGENT",
        "O_CLERK": "Clerk#000000888",
        "O_SHIPPRIORITY": 0,
        "O_COMMENT": "Post-restart order",
    })

    state_store_2 = MinioStateStore(bucket="daisy-state", s3_client=mock_s3_client)
    client_2 = MockSnowflakeClient(initial_records=sample_orders_dataset)
    adapter_2 = SnowflakeAdapter(
        settings=test_settings,
        client=client_2,
        state_store=state_store_2,
        producer=memory_producer,
    )

    loaded_state = state_store_2.load_state("orders")
    assert loaded_state is not None
    assert loaded_state.last_watermark == saved_watermark

    res2 = adapter_2.run_sync(table="ORDERS")

    assert res2.records_captured == 1
    assert res2.total_records_synced == 10001
    assert len(memory_producer.events) == 1
    assert memory_producer.events[0].entity_key == "88888"


def test_adapter_extracts_partition_key_o_custkey(
    test_settings: Settings,
    sample_orders_dataset: list[dict],
    minio_state_store: MinioStateStore,
    memory_producer: InMemoryEventProducer,
) -> None:
    """Verify adapter populates partition_key with O_CUSTKEY."""
    client = MockSnowflakeClient(initial_records=sample_orders_dataset)
    adapter = SnowflakeAdapter(
        settings=test_settings,
        client=client,
        state_store=minio_state_store,
        producer=memory_producer,
    )

    result = adapter.run_sync(table="ORDERS", batch_size=10000)
    assert result.records_captured == 10000
    assert len(memory_producer.events) == 10000

    for ev in memory_producer.events:
        assert ev.entity_key is not None
        assert ev.partition_key is not None
        assert ev.partition_key == str(ev.payload.get("O_CUSTKEY"))


def test_adapter_blue_green_migration_and_full_refresh(
    test_settings: Settings,
    sample_orders_dataset: list[dict],
    minio_state_store: MinioStateStore,
    memory_producer: InMemoryEventProducer,
) -> None:
    """Verify blue/green migration topic isolation and full refresh."""
    client = MockSnowflakeClient(initial_records=sample_orders_dataset)
    adapter = SnowflakeAdapter(
        settings=test_settings,
        client=client,
        state_store=minio_state_store,
        producer=memory_producer,
    )

    # 1. Normal sync on v1
    res1 = adapter.run_sync(table="ORDERS", batch_size=10000)
    assert res1.records_captured == 10000
    state_v1 = minio_state_store.load_state("orders")
    assert state_v1 is not None

    memory_producer.clear()

    # 2. Blue/Green full refresh on v2
    res2 = adapter.run_sync(
        table="ORDERS",
        batch_size=10000,
        full_refresh=True,
        topic="switch.orders.events.v2",
        state_key="orders_v2",
    )
    assert res2.is_initial_sync is True
    assert res2.records_captured == 10000
    assert len(memory_producer.events) == 10000

    # Verify state isolation: v1 state was unaffected, v2 state created
    state_v2 = minio_state_store.load_state("orders_v2")
    assert state_v2 is not None
    assert state_v1.source_table == "orders"
    assert state_v2.source_table == "orders_v2"

