#!/usr/bin/env python3
"""CLI entrypoint for running the Snowflake Ingestion Adapter."""

from __future__ import annotations

import argparse
from pathlib import Path
import signal
import sys
import time

# Ensure service directory is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from snowflake_adapter.adapter import SnowflakeAdapter
from snowflake_adapter.client import MockSnowflakeClient
from snowflake_adapter.config import get_settings
from snowflake_adapter.logging import configure_logging, get_logger

logger = get_logger("run_adapter")

_running = True


def _signal_handler(sig: int, frame: Any) -> None:
    """Handle graceful shutdown signal."""
    global _running
    logger.info("Termination signal %s received. Stopping adapter gracefully...", sig)
    _running = False


def main() -> None:
    """Execute adapter sync job."""
    global _running
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    parser = argparse.ArgumentParser(description="Daisy Streams - Snowflake Ingestion Adapter")
    parser.add_argument("--table", default=None, help="Snowflake table name (default from settings)")
    parser.add_argument("--batch-size", type=int, default=None, help="Batch size for sync slice")
    parser.add_argument("--loop", action="store_true", help="Run continuously in a scheduled loop")
    parser.add_argument("--interval-sec", type=int, default=30, help="Interval between sync iterations in loop mode")
    parser.add_argument("--mock", action="store_true", help="Run with in-memory mock Snowflake data")
    parser.add_argument("--storage", choices=["garage", "minio", "local"], default=None, help="State storage backend (garage | minio | local)")
    parser.add_argument("--topic", default=None, help="Target Kafka topic (e.g., switch.orders.events.v2)")
    parser.add_argument("--full-refresh", action="store_true", help="Bypass prior watermark and perform full initial refresh")
    parser.add_argument("--state-key", default=None, help="Isolated state persistence key (default: based on table or custom topic)")

    args = parser.parse_args()

    settings = get_settings()
    configure_logging(level=settings.log_level)

    storage_mode = args.storage or ("local" if args.mock else settings.sync_state_storage)

    logger.info(
        "Starting Snowflake Adapter service (table: %s, storage: %s, loop: %s, topic: %s, full_refresh: %s)",
        args.table or settings.snowflake_table,
        storage_mode,
        args.loop,
        args.topic or settings.kafka_topic,
        args.full_refresh,
    )

    client = None
    state_store = None
    producer = None

    if args.mock:
        logger.info("Using MockSnowflakeClient with synthetic dataset for local verification.")
        mock_records = [
            {
                "O_ORDERKEY": i,
                "O_CUSTKEY": 1000 + (i % 50),
                "O_ORDERSTATUS": "O",
                "O_TOTALPRICE": 150.50 + i,
                "O_ORDERDATE": f"1992-01-{(i % 28) + 1:02d}",
                "O_ORDERPRIORITY": "1-URGENT",
                "O_CLERK": f"Clerk#000000{i:03d}",
                "O_SHIPPRIORITY": 0,
                "O_COMMENT": "Mock order",
            }
            for i in range(1, 10001)
        ]
        client = MockSnowflakeClient(initial_records=mock_records)

    if storage_mode == "local":
        from snowflake_adapter.state.local_file import LocalFileStateStore
        state_store = LocalFileStateStore(file_path=settings.sync_state_file_path)

    if args.mock:
        from snowflake_adapter.transport.memory_producer import InMemoryEventProducer
        producer = InMemoryEventProducer()

    try:
        adapter = SnowflakeAdapter(
            settings=settings,
            client=client,
            state_store=state_store,
            producer=producer,
        )
    except Exception as e:
        logger.error("Failed to initialize SnowflakeAdapter: %s", e)
        sys.exit(1)

    # Determine isolated state key if topic override is specified
    default_state_key = args.state_key
    if not default_state_key and args.topic:
        clean_topic_suffix = args.topic.replace(".", "_").replace("-", "_")
        default_state_key = f"{args.table or settings.snowflake_table}_{clean_topic_suffix}"

    first_run = True
    while _running:
        try:
            should_full_refresh = args.full_refresh if first_run else False
            result = adapter.run_sync(
                table=args.table,
                batch_size=args.batch_size,
                full_refresh=should_full_refresh,
                topic=args.topic,
                state_key=default_state_key,
            )
            first_run = False
            logger.info(
                "Sync cycle finished: %d records captured, current watermark: %s",
                result.records_captured,
                result.current_watermark,
            )
        except Exception as e:
            logger.error("Sync run encountered error: %s", e, exc_info=True)

        if not args.loop:
            break

        logger.info("Sleeping for %d seconds before next sync iteration...", args.interval_sec)
        sleep_elapsed = 0
        while _running and sleep_elapsed < args.interval_sec:
            time.sleep(1)
            sleep_elapsed += 1

    logger.info("Snowflake Adapter service exited.")


if __name__ == "__main__":
    main()
