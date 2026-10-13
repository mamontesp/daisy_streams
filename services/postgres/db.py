"""Database connection management and abstraction supporting PostgreSQL and test drivers."""

from __future__ import annotations

import json
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generator, Optional, Sequence, Union
from uuid import UUID

import logging

from services.postgres.config import PostgresSettings

logger = logging.getLogger(__name__)

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg.types.json import Jsonb, JsonbDumper

    PSYCOPG_AVAILABLE = True
    try:
        psycopg.adapters.register_dumper(dict, JsonbDumper)
    except Exception:
        pass
except ImportError:
    PSYCOPG_AVAILABLE = False
    psycopg = None
    dict_row = None
    Jsonb = None
    JsonbDumper = None


class BaseDB:
    """Base interface for database adapters."""

    def execute(self, query: str, params: Optional[Sequence[Any]] = None) -> Any:
        raise NotImplementedError

    def fetchone(self, query: str, params: Optional[Sequence[Any]] = None) -> Optional[dict[str, Any]]:
        raise NotImplementedError

    def fetchall(self, query: str, params: Optional[Sequence[Any]] = None) -> list[dict[str, Any]]:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


class PostgresDB(BaseDB):
    """PostgreSQL connection manager powered by psycopg v3."""

    def __init__(self, settings: Optional[PostgresSettings] = None, connection_uri: Optional[str] = None) -> None:
        if not PSYCOPG_AVAILABLE:
            raise RuntimeError("psycopg is not installed. Install psycopg[binary] to use PostgresDB.")

        self.settings = settings or PostgresSettings()
        self.uri = connection_uri or self.settings.connection_uri
        self._conn: Optional[Any] = None

    def connect(self) -> Any:
        """Establish connection with dict_row factory and autocommit."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(
                self.uri,
                row_factory=dict_row,
                autocommit=True,
                connect_timeout=self.settings.postgres_connect_timeout,
            )
            if JsonbDumper is not None:
                try:
                    self._conn.adapters.register_dumper(dict, JsonbDumper)
                except Exception:
                    pass
        return self._conn

    def _format_params(self, params: Optional[Sequence[Any]]) -> Optional[Sequence[Any]]:
        """Ensure dict payloads are wrapped in Jsonb for PostgreSQL."""
        if not params:
            return params
        formatted: list[Any] = []
        for p in params:
            if isinstance(p, dict):
                formatted.append(Jsonb(p) if Jsonb is not None else json.dumps(p))
            else:
                formatted.append(p)
        return tuple(formatted)

    @contextmanager
    def transaction(self) -> Generator[Any, None, None]:
        """Context manager for atomic transaction block."""
        conn = self.connect()
        with conn.transaction():
            with conn.cursor() as cur:
                yield cur

    def execute(self, query: str, params: Optional[Sequence[Any]] = None) -> Any:
        """Execute query within auto-commit or immediate transaction."""
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                res = cur.execute(query, self._format_params(params) or ())
                if not conn.autocommit:
                    conn.commit()
                return res
        except Exception:
            if not conn.autocommit:
                conn.rollback()
            raise

    def fetchone(self, query: str, params: Optional[Sequence[Any]] = None) -> Optional[dict[str, Any]]:
        """Execute and fetch a single dict row."""
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(query, self._format_params(params) or ())
                return cur.fetchone()
        except Exception:
            if not conn.autocommit:
                conn.rollback()
            raise

    def fetchall(self, query: str, params: Optional[Sequence[Any]] = None) -> list[dict[str, Any]]:
        """Execute and fetch all dict rows."""
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(query, self._format_params(params) or ())
                return cur.fetchall()
        except Exception:
            if not conn.autocommit:
                conn.rollback()
            raise

    def close(self) -> None:
        """Close connection if active."""
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
            self._conn = None


class SQLiteTestDB(BaseDB):
    """SQLite in-memory / file adapter for deterministic offline unit testing."""

    def __init__(self, database_path: str = ":memory:") -> None:
        self.database_path = database_path
        self._conn = sqlite3.connect(database_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_sqlite_schema()

    def _init_sqlite_schema(self) -> None:
        """Initialize compatible tables in SQLite."""
        cur = self._conn.cursor()
        cur.executescript("""
        CREATE TABLE IF NOT EXISTS event_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            source TEXT NOT NULL,
            entity_key TEXT NOT NULL,
            partition_key TEXT,
            payload TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            captured_at TEXT NOT NULL,
            processed_at TEXT NOT NULL DEFAULT (datetime('now')),
            schema_version TEXT NOT NULL DEFAULT '1.0.0'
        );

        CREATE INDEX IF NOT EXISTS idx_event_log_event_id ON event_log (event_id);
        CREATE INDEX IF NOT EXISTS idx_event_log_entity_key ON event_log (entity_key);
        CREATE INDEX IF NOT EXISTS idx_event_log_occurred_at ON event_log (occurred_at);

        CREATE TABLE IF NOT EXISTS current_state (
            entity_key TEXT PRIMARY KEY,
            latest_event_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            state TEXT NOT NULL,
            last_occurred_at TEXT NOT NULL,
            last_updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS quarantined_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT,
            entity_key TEXT,
            error_code TEXT NOT NULL,
            error_message TEXT NOT NULL,
            raw_payload TEXT NOT NULL,
            quarantined_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """)
        self._conn.commit()

    def _convert_query_and_params(self, query: str, params: Optional[Sequence[Any]]) -> tuple[str, list[Any]]:
        """Translate PostgreSQL query and params to SQLite format."""
        # Translate %s placeholder to ?
        sqlite_query = query.replace("%s", "?")
        
        # Replace NOW() with datetime('now')
        sqlite_query = re.sub(r"\bNOW\(\)", "datetime('now')", sqlite_query, flags=re.IGNORECASE)

        # Simplify lag calculation expressions if present
        # EXTRACT(EPOCH FROM (processed_at - occurred_at)) -> (strftime('%s', processed_at) - strftime('%s', occurred_at))
        sqlite_query = re.sub(
            r"EXTRACT\s*\(\s*EPOCH\s+FROM\s+\(\s*processed_at\s*-\s*occurred_at\s*\)\s*\)",
            "(strftime('%s', processed_at) - strftime('%s', occurred_at))",
            sqlite_query,
            flags=re.IGNORECASE,
        )

        formatted_params: list[Any] = []
        if params:
            for p in params:
                if hasattr(p, "obj"):
                    formatted_params.append(json.dumps(p.obj))
                elif isinstance(p, (dict, list)):
                    formatted_params.append(json.dumps(p))
                elif isinstance(p, UUID):
                    formatted_params.append(str(p))
                elif isinstance(p, datetime):
                    formatted_params.append(p.isoformat())
                else:
                    formatted_params.append(p)
        return sqlite_query, formatted_params

    @contextmanager
    def transaction(self) -> Generator[Any, None, None]:
        """SQLite transaction context."""
        cur = self._conn.cursor()
        try:
            yield cur
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def execute(self, query: str, params: Optional[Sequence[Any]] = None) -> Any:
        sql, formatted_params = self._convert_query_and_params(query, params)
        cur = self._conn.cursor()
        res = cur.execute(sql, formatted_params)
        self._conn.commit()
        return res

    def fetchone(self, query: str, params: Optional[Sequence[Any]] = None) -> Optional[dict[str, Any]]:
        sql, formatted_params = self._convert_query_and_params(query, params)
        cur = self._conn.cursor()
        cur.execute(sql, formatted_params)
        row = cur.fetchone()
        if row is None:
            return None
        res = dict(row)
        for key in ("payload", "state", "raw_payload"):
            if key in res and isinstance(res[key], str):
                try:
                    res[key] = json.loads(res[key])
                except Exception:
                    pass
        return res

    def fetchall(self, query: str, params: Optional[Sequence[Any]] = None) -> list[dict[str, Any]]:
        sql, formatted_params = self._convert_query_and_params(query, params)
        cur = self._conn.cursor()
        cur.execute(sql, formatted_params)
        rows = cur.fetchall()
        result: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            for key in ("payload", "state", "raw_payload"):
                if key in d and isinstance(d[key], str):
                    try:
                        d[key] = json.loads(d[key])
                    except Exception:
                        pass
            result.append(d)
        return result

    def close(self) -> None:
        self._conn.close()


def get_db(settings: Optional[PostgresSettings] = None, use_sqlite_fallback: bool = False) -> BaseDB:
    """Factory creating appropriate database adapter."""
    if use_sqlite_fallback:
        return SQLiteTestDB()
    cfg = settings or PostgresSettings()
    return PostgresDB(settings=cfg)
