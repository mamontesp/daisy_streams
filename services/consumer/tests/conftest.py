"""Pytest fixtures for Kafka Consumer tests."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, List, Optional
import pytest

# Ensure root and service paths are on sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))
sys.path.insert(0, str(ROOT_DIR / "services" / "snowflake_adapter"))
sys.path.insert(0, str(ROOT_DIR / "services" / "postgres"))
sys.path.insert(0, str(ROOT_DIR / "services" / "consumer"))

from services.postgres.db import SQLiteTestDB
from services.postgres.repository import PostgresRepository
from services.consumer.scripts.run_consumer import MockKafkaConsumer, MockKafkaMessage


@pytest.fixture
def test_repo() -> PostgresRepository:
    """Fixture providing isolated in-memory PostgresRepository."""
    db = SQLiteTestDB(":memory:")
    return PostgresRepository(db=db)


@pytest.fixture
def mock_kafka_consumer() -> MockKafkaConsumer:
    """Fixture providing in-memory mock Kafka consumer."""
    return MockKafkaConsumer()
