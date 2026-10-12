"""Snowflake database client interface and implementations."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional, Protocol
from snowflake_adapter.logging import get_logger

logger = get_logger(__name__)

try:
    import snowflake.connector
    from snowflake.connector import DictCursor
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    snowflake = None
try:
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization
    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False
    default_backend = None
    serialization = None


def load_private_key_bytes(
    path: Optional[str] = None,
    content: Optional[str] = None,
    passphrase: Optional[str] = None,
) -> bytes:
    """Load PKCS#8 PEM private key from file path or raw string, returning DER bytes.

    Snowflake Python connector requires private_key to be passed as DER encoded bytes:
    p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    """
    if not CRYPTOGRAPHY_AVAILABLE:
        raise RuntimeError("The 'cryptography' library is required for Snowflake key-pair authentication.")

    if not path and not content:
        raise ValueError(
            "Key-pair authentication requires either 'private_key_path' or 'private_key_content'."
        )

    key_bytes: bytes
    if path:
        target_path = Path(path)
        if not target_path.exists():
            for alt in (
                Path("/secrets") / target_path.name,
                Path("/app/secrets") / target_path.name,
                Path("secrets") / target_path.name,
            ):
                if alt.exists():
                    target_path = alt
                    break
        with open(target_path, "rb") as f:
            key_bytes = f.read()
    else:
        assert content is not None
        key_bytes = content.strip().encode("utf-8")

    clean_passphrase = None
    if passphrase:
        p_str = passphrase.strip().strip("'\"")
        if " #" in p_str:
            p_str = p_str.split(" #")[0].strip().strip("'\"")
        if p_str:
            clean_passphrase = p_str

    password_bytes = clean_passphrase.encode("utf-8") if clean_passphrase else None

    private_key = serialization.load_pem_private_key(
        key_bytes,
        password=password_bytes,
        backend=default_backend(),
    )

    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


class SnowflakeClient(Protocol):
    """Protocol for Snowflake database client interactions."""

    def execute_query(self, sql: str, params: tuple | dict | None = None) -> list[dict[str, Any]]:
        """Execute a SELECT query and return rows as dictionaries."""
        ...

    def execute_statement(self, sql: str, params: tuple | dict | None = None) -> int:
        """Execute a DDL or DML statement and return affected rows."""
        ...

    def close(self) -> None:
        """Close connection and clean up resources."""
        ...


class SnowflakeConnectorClient:
    """Production Snowflake client supporting key_pair and externalbrowser authentication."""

    def __init__(
        self,
        account: str,
        user: str,
        authenticator: str = "key_pair",
        private_key_path: Optional[str] = None,
        private_key_content: Optional[str] = None,
        private_key_passphrase: Optional[str] = None,
        role: str = "ACCOUNTADMIN",
        warehouse: str = "COMPUTE_WH",
        database: str = "daisy_streams",
        schema: str = "ingestion",
    ) -> None:
        if not SNOWFLAKE_AVAILABLE:
            raise RuntimeError("snowflake-connector-python is not installed.")

        if authenticator not in ("key_pair", "externalbrowser"):
            raise ValueError(
                f"Unsupported authenticator '{authenticator}'. Only 'key_pair' and 'externalbrowser' are permitted."
            )

        self.account = account
        self.user = user
        self.authenticator = authenticator
        self.role = role
        self.warehouse = warehouse
        self.database = database
        self.schema = schema

        conn_params: dict[str, Any] = {
            "account": account,
            "user": user,
            "role": role,
            "warehouse": warehouse,
            "database": database,
            "schema": schema,
        }

        if authenticator == "externalbrowser":
            logger.info(
                "Connecting to Snowflake via externalbrowser SSO (account: %s, user: %s)",
                account,
                user,
            )
            conn_params["authenticator"] = "externalbrowser"
        elif authenticator == "key_pair":
            logger.info(
                "Connecting to Snowflake via Key-Pair authentication (account: %s, user: %s)",
                account,
                user,
            )
            pk_bytes = load_private_key_bytes(
                path=private_key_path,
                content=private_key_content,
                passphrase=private_key_passphrase,
            )
            conn_params["private_key"] = pk_bytes

        self._conn = snowflake.connector.connect(**conn_params)

    def execute_query(self, sql: str, params: tuple | dict | None = None) -> list[dict[str, Any]]:
        """Execute a SELECT query returning a list of dicts with column names as keys."""
        with self._conn.cursor(DictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [dict(row) for row in rows]

    def execute_statement(self, sql: str, params: tuple | dict | None = None) -> int:
        """Execute an INSERT/UPDATE/DELETE statement returning affected rows count."""
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount or 0

    def close(self) -> None:
        """Close the underlying Snowflake connection."""
        if self._conn:
            self._conn.close()


class MockSnowflakeClient:
    """In-memory mock Snowflake client for offline testing and verification."""

    def __init__(self, initial_records: Optional[list[dict[str, Any]]] = None) -> None:
        self.records: list[dict[str, Any]] = list(initial_records or [])
        self.statements_executed: list[str] = []

    def set_records(self, records: list[dict[str, Any]]) -> None:
        """Replace records in mock client."""
        self.records = list(records)

    def execute_query(self, sql: str, params: tuple | dict | None = None) -> list[dict[str, Any]]:
        """Mock execution: handles filters on watermark and limit."""
        self.statements_executed.append(sql)
        filtered = list(self.records)

        if params:
            watermark_val = None
            if isinstance(params, dict):
                watermark_val = params.get("watermark")
            elif isinstance(params, (list, tuple)) and len(params) > 0:
                watermark_val = params[0]

            if watermark_val is not None:
                def _to_ts(val: Any) -> float:
                    from datetime import date, datetime, timezone
                    if isinstance(val, datetime):
                        dt = val if val.tzinfo is not None else val.replace(tzinfo=timezone.utc)
                        return dt.timestamp()
                    if isinstance(val, date):
                        return datetime(val.year, val.month, val.day, tzinfo=timezone.utc).timestamp()
                    if isinstance(val, str):
                        try:
                            if len(val) == 10 and val.count("-") == 2:
                                d = date.fromisoformat(val)
                                return datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp()
                            return datetime.fromisoformat(val.replace("Z", "+00:00")).timestamp()
                        except ValueError:
                            pass
                    return 0.0

                target_ts = _to_ts(watermark_val)
                filtered = [
                    r for r in filtered
                    if _to_ts(r.get("O_ORDERDATE") or r.get("UPDATED_AT") or "") > target_ts
                ]

        limit = None
        sql_upper = sql.upper()
        if "LIMIT " in sql_upper:
            parts = sql_upper.split("LIMIT ")
            try:
                limit = int(parts[1].split()[0])
            except (IndexError, ValueError):
                pass

        if limit is not None:
            filtered = filtered[:limit]

        return filtered

    def execute_statement(self, sql: str, params: tuple | dict | None = None) -> int:
        """Execute statement in mock."""
        self.statements_executed.append(sql)
        return 1

    def close(self) -> None:
        """No-op for mock client."""
        pass
