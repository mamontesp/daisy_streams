#!/usr/bin/env python3
"""Change Simulation Script for Snowflake Orders (REQ-A06).

Executes verifiable INSERT and UPDATE operations in Snowflake to prove
incremental capture and high-watermark progression without modifying
read-only sample databases.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import random
import sys
from datetime import datetime, timezone
from typing import Any

# Ensure service directory is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from snowflake_adapter.client import SnowflakeConnectorClient, SnowflakeClient
from snowflake_adapter.config import get_settings
from snowflake_adapter.logging import configure_logging, get_logger

logger = get_logger("simulate_changes")


def generate_simulated_inserts(
    start_key: int,
    count: int = 5,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Generate deterministic verifiable order INSERT records with current timestamps."""
    ts = now or datetime.now(timezone.utc)
    date_str = ts.strftime("%Y-%m-%d")
    ts_str = ts.isoformat()

    records = []
    statuses = ["O", "F", "P"]
    priorities = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]

    for i in range(count):
        order_key = start_key + i
        records.append({
            "O_ORDERKEY": order_key,
            "O_CUSTKEY": random.randint(1001, 9999),
            "O_ORDERSTATUS": random.choice(statuses),
            "O_TOTALPRICE": round(random.uniform(50.0, 5000.0), 2),
            "O_ORDERDATE": date_str,
            "O_ORDERPRIORITY": random.choice(priorities),
            "O_CLERK": f"Clerk#{random.randint(100, 999):09d}",
            "O_SHIPPRIORITY": 0,
            "O_COMMENT": f"Simulated delta order insert batch {order_key}",
            "UPDATED_AT": ts_str,
            "EVENT_TYPE": "insert",
        })
    return records


def generate_simulated_updates(
    target_keys: list[int],
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Generate verifiable order UPDATE modifications with updated timestamps."""
    ts = now or datetime.now(timezone.utc)
    ts_str = ts.isoformat()

    updates = []
    for key in target_keys:
        updates.append({
            "O_ORDERKEY": key,
            "O_ORDERSTATUS": "F",
            "O_TOTALPRICE": round(random.uniform(1000.0, 9000.0), 2),
            "UPDATED_AT": ts_str,
            "EVENT_TYPE": "update",
        })
    return updates


def setup_mutable_table(client: SnowflakeClient, database: str, schema: str, table: str) -> None:
    """Create a mutable orders table cloned or seeded from TPCH_SF1.ORDERS."""
    logger.info("Setting up mutable working table %s.%s.%s...", database, schema, table)
    ddl = f"""
CREATE TABLE IF NOT EXISTS {database}.{schema}.{table} (
    O_ORDERKEY NUMBER(38, 0) PRIMARY KEY,
    O_CUSTKEY NUMBER(38, 0),
    O_ORDERSTATUS VARCHAR(1),
    O_TOTALPRICE NUMBER(12, 2),
    O_ORDERDATE DATE,
    O_ORDERPRIORITY VARCHAR(15),
    O_CLERK VARCHAR(15),
    O_SHIPPRIORITY NUMBER(38, 0),
    O_COMMENT VARCHAR(79),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    EVENT_TYPE VARCHAR(10) DEFAULT 'insert'
);
""".strip()
    client.execute_statement(ddl)

    count_sql = f"SELECT COUNT(*) AS CNT FROM {database}.{schema}.{table}"
    res = client.execute_query(count_sql)
    cnt = res[0].get("CNT", 0) if res else 0

    if cnt == 0:
        logger.info("Seeding initial 10,000 records into %s.%s.%s...", database, schema, table)
        seeded = False
        try:
            seed_sql = f"""
INSERT INTO {database}.{schema}.{table} (
    O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE,
    O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT, UPDATED_AT, EVENT_TYPE
)
SELECT
    O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE,
    O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT,
    CURRENT_TIMESTAMP(), 'insert'
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
ORDER BY O_ORDERDATE ASC, O_ORDERKEY ASC
LIMIT 10000;
""".strip()
            client.execute_statement(seed_sql)
            seeded = True
            logger.info("Seeding from SNOWFLAKE_SAMPLE_DATA complete.")
        except Exception as e:
            logger.info("SNOWFLAKE_SAMPLE_DATA not accessible (%s). Seeding directly with synthetic orders...", e)

        if not seeded:
            # Pure synthetic seed without external table dependencies
            synth_sql = f"""
INSERT INTO {database}.{schema}.{table} (
    O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE,
    O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT, UPDATED_AT, EVENT_TYPE
)
SELECT
    SEQ4() + 1 AS O_ORDERKEY,
    MOD(SEQ4(), 1000) + 1 AS O_CUSTKEY,
    CASE WHEN MOD(SEQ4(), 3) = 0 THEN 'O' WHEN MOD(SEQ4(), 3) = 1 THEN 'F' ELSE 'P' END AS O_ORDERSTATUS,
    ROUND(UNIFORM(100.0, 5000.0, RANDOM()), 2) AS O_TOTALPRICE,
    DATEADD(day, -MOD(SEQ4(), 365), CURRENT_DATE()) AS O_ORDERDATE,
    '1-URGENT' AS O_ORDERPRIORITY,
    'Clerk#000000001' AS O_CLERK,
    0 AS O_SHIPPRIORITY,
    'Initial synthetic order' AS O_COMMENT,
    CURRENT_TIMESTAMP() AS UPDATED_AT,
    'insert' AS EVENT_TYPE
FROM TABLE(GENERATOR(ROWCOUNT => 10000));
""".strip()
            client.execute_statement(synth_sql)
            logger.info("Synthetic seeding of 10,000 records complete.")


def execute_simulation(
    client: SnowflakeClient | None,
    table: str,
    database: str,
    schema: str,
    num_inserts: int = 5,
    num_updates: int = 5,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Execute or simulate verifiable INSERT and UPDATE operations."""
    now = datetime.now(timezone.utc)
    base_order_key = int(now.timestamp()) * 1000

    inserts = generate_simulated_inserts(start_key=base_order_key, count=num_inserts, now=now)
    insert_keys = [r["O_ORDERKEY"] for r in inserts]

    update_keys = [base_order_key - (i + 1) * 10 for i in range(num_updates)]
    updates = generate_simulated_updates(target_keys=update_keys, now=now)

    receipt = {
        "timestamp": now.isoformat(),
        "database": database,
        "schema": schema,
        "table": table,
        "dry_run": dry_run,
        "inserts": {
            "count": len(inserts),
            "keys": insert_keys,
            "sample": inserts[0] if inserts else None,
        },
        "updates": {
            "count": len(updates),
            "keys": update_keys,
            "sample": updates[0] if updates else None,
        },
    }

    if dry_run:
        logger.info("DRY-RUN mode enabled. Generated %d INSERTs and %d UPDATEs without executing.", len(inserts), len(updates))
        return receipt

    if client is None:
        raise ValueError("SnowflakeClient required when not in dry_run mode.")

    logger.info("Executing %d verifiable INSERTs into %s.%s.%s...", len(inserts), database, schema, table)
    for row in inserts:
        insert_sql = f"""
INSERT INTO {database}.{schema}.{table} (
    O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE,
    O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY,
    O_COMMENT, UPDATED_AT, EVENT_TYPE
) VALUES (
    {row['O_ORDERKEY']}, {row['O_CUSTKEY']}, '{row['O_ORDERSTATUS']}', {row['O_TOTALPRICE']},
    '{row['O_ORDERDATE']}', '{row['O_ORDERPRIORITY']}', '{row['O_CLERK']}', {row['O_SHIPPRIORITY']},
    '{row['O_COMMENT']}', CURRENT_TIMESTAMP(), 'insert'
);
""".strip()
        client.execute_statement(insert_sql)

    logger.info("Executing %d verifiable UPDATEs into %s.%s.%s...", len(updates), database, schema, table)
    for upd in updates:
        update_sql = f"""
UPDATE {database}.{schema}.{table}
SET O_ORDERSTATUS = '{upd['O_ORDERSTATUS']}',
    O_TOTALPRICE = {upd['O_TOTALPRICE']},
    UPDATED_AT = CURRENT_TIMESTAMP(),
    EVENT_TYPE = 'update'
WHERE O_ORDERKEY = {upd['O_ORDERKEY']};
""".strip()
        client.execute_statement(update_sql)

    logger.info("Successfully executed simulation: %d inserted, %d updated.", len(inserts), len(updates))
    return receipt


def main() -> None:
    """CLI Entrypoint for change simulator."""
    configure_logging()
    settings = get_settings()

    parser = argparse.ArgumentParser(description="Snowflake Change Simulator (REQ-A06)")
    parser.add_argument("--table", default=settings.snowflake_table, help="Target table name")
    parser.add_argument("--database", default=settings.snowflake_database, help="Target database")
    parser.add_argument("--schema", default=settings.snowflake_schema, help="Target schema")
    parser.add_argument("--num-inserts", type=int, default=5, help="Number of rows to insert")
    parser.add_argument("--num-updates", type=int, default=5, help="Number of rows to update")
    parser.add_argument("--setup-mutable-table", action="store_true", help="Initialize mutable table schema")
    parser.add_argument("--dry-run", action="store_true", help="Print simulation without executing queries")

    args = parser.parse_args()

    client: SnowflakeClient | None = None
    if not args.dry_run:
        try:
            client = SnowflakeConnectorClient(
                account=settings.snowflake_account,
                user=settings.snowflake_user,
                authenticator=settings.snowflake_authenticator,
                private_key_path=settings.snowflake_private_key_path,
                private_key_content=settings.snowflake_private_key_content,
                private_key_passphrase=settings.snowflake_private_key_passphrase,
                role=settings.snowflake_role,
                warehouse=settings.snowflake_warehouse,
                database=args.database,
                schema=args.schema,
            )
        except Exception as e:
            logger.error("Failed to connect to Snowflake: %s. Use --dry-run for offline verification.", e)
            sys.exit(1)

    if args.setup_mutable_table and client is not None:
        setup_mutable_table(client, args.database, args.schema, args.table)

    receipt = execute_simulation(
        client=client,
        table=args.table,
        database=args.database,
        schema=args.schema,
        num_inserts=args.num_inserts,
        num_updates=args.num_updates,
        dry_run=args.dry_run,
    )

    print(json.dumps(receipt, indent=2))
    if client:
        client.close()


if __name__ == "__main__":
    main()
