"""Pytest fixtures for PostgreSQL layer testing."""

from __future__ import annotations

import sys
from pathlib import Path
import pytest

# Ensure services directories are on PYTHONPATH
ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

from services.postgres.db import SQLiteTestDB
from services.postgres.repository import PostgresRepository


@pytest.fixture
def test_repo() -> PostgresRepository:
    """Fixture providing an isolated in-memory test repository."""
    db = SQLiteTestDB(":memory:")
    repo = PostgresRepository(db=db)
    return repo
