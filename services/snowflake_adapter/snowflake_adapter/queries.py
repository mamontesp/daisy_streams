"""SQL query builders for Snowflake initial and incremental extractions."""

from __future__ import annotations


def build_initial_sync_query(
    database: str,
    schema: str,
    table: str,
    watermark_column: str = "O_ORDERDATE",
    entity_key_column: str = "O_ORDERKEY",
    limit: int = 50000,
    join_customer: bool = True,
) -> str:
    """Build SQL for bounded initial synchronization."""
    if join_customer and table.upper() == "ORDERS":
        return f"""
SELECT
    o.O_ORDERKEY,
    o.O_CUSTKEY,
    o.O_ORDERSTATUS,
    o.O_TOTALPRICE,
    o.O_ORDERDATE,
    o.O_ORDERPRIORITY,
    o.O_CLERK,
    o.O_SHIPPRIORITY,
    o.O_COMMENT,
    c.C_NAME,
    c.C_PHONE,
    c.C_ACCTBAL,
    c.C_MKTSEGMENT
FROM {database}.{schema}.{table} o
LEFT JOIN {database}.{schema}.CUSTOMER c
    ON o.O_CUSTKEY = c.C_CUSTKEY
ORDER BY o.{watermark_column} ASC, o.{entity_key_column} ASC
LIMIT {limit}
""".strip()
    else:
        return f"""
SELECT *
FROM {database}.{schema}.{table}
ORDER BY {watermark_column} ASC, {entity_key_column} ASC
LIMIT {limit}
""".strip()


def build_incremental_sync_query(
    database: str,
    schema: str,
    table: str,
    watermark_column: str = "O_ORDERDATE",
    entity_key_column: str = "O_ORDERKEY",
    limit: int = 10000,
    join_customer: bool = True,
) -> str:
    """Build SQL for incremental synchronization querying delta rows above watermark."""
    if join_customer and table.upper() == "ORDERS":
        return f"""
SELECT
    o.O_ORDERKEY,
    o.O_CUSTKEY,
    o.O_ORDERSTATUS,
    o.O_TOTALPRICE,
    o.O_ORDERDATE,
    o.O_ORDERPRIORITY,
    o.O_CLERK,
    o.O_SHIPPRIORITY,
    o.O_COMMENT,
    c.C_NAME,
    c.C_PHONE,
    c.C_ACCTBAL,
    c.C_MKTSEGMENT
FROM {database}.{schema}.{table} o
LEFT JOIN {database}.{schema}.CUSTOMER c
    ON o.O_CUSTKEY = c.C_CUSTKEY
WHERE o.{watermark_column} > %(watermark)s
ORDER BY o.{watermark_column} ASC, o.{entity_key_column} ASC
LIMIT {limit}
""".strip()
    else:
        return f"""
SELECT *
FROM {database}.{schema}.{table}
WHERE {watermark_column} > %(watermark)s
ORDER BY {watermark_column} ASC, {entity_key_column} ASC
LIMIT {limit}
""".strip()
