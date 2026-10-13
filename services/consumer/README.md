# Kafka Consumer Service (Part C)

Subscribes to streaming events from Apache Kafka, executes Data Quality validations, and materializes events idempotently into PostgreSQL.

---

## 1. Scope & Objective

Fulfills **Part C** of [`spec-tool-consumer-and-api.md`](../../spec/spec-tool-consumer-and-api.md):
- **Dynamic Topic Subscription**: Binds dynamically to `KAFKA_TOPIC` defined in `.env` (no hardcoded topic strings).
- **At-Least-Once Delivery**: Manual offset commits after successful transactional persistence in PostgreSQL (`enable.auto.commit = False`).
- **Data Quality Filtering**: Non-conforming records (negative prices, malformed schema, duplicate event IDs) routed to `quarantined_events` without crashing the consumer.
- **Offset 0 Replay**: Rewinds topic partitions to offset 0 and converges to identical `current_state` (**AC-C02**).

---

## 2. Directory Structure

```
services/consumer/
├── Dockerfile                   # Isolated container image definition
├── requirements.txt             # Service dependencies
├── README.md                    # Documentation
├── consumer/
│   ├── __init__.py
│   ├── config.py                # ConsumerSettings (reads KAFKA_TOPIC, broker, group)
│   ├── consumer.py              # KafkaConsumerService (poll, process, commit, loop)
│   └── replay.py                # Offset 0 partition seek utilities
├── scripts/
│   └── run_consumer.py          # Entrypoint CLI (--loop, --replay-offset-0, --mock)
└── tests/
    ├── conftest.py
    └── test_consumer.py         # Unit tests with mock consumer
```

---

## 3. Configuration (`.env`)

| Variable | Default | Description |
| :--- | :--- | :--- |
| `KAFKA_TOPIC` | `switch.orders.events` | Target Kafka topic to subscribe to |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker address (or `kafka:29092` in Docker) |
| `KAFKA_CONSUMER_GROUP` | `daisy-streams-consumer-group` | Consumer group for offset tracking |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` | Starting offset when no commit exists |
| `POSTGRES_HOST` | `localhost` | PostgreSQL host (`postgres` in Docker) |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB` | `daisy_streams` | PostgreSQL database name |

---

## 4. CLI Usage

```bash
# Continuous ingestion loop (normal mode)
python services/consumer/scripts/run_consumer.py --loop

# Process bounded batch (e.g. 50 messages)
python services/consumer/scripts/run_consumer.py --max-messages 50

# Offset 0 replay mode (AC-C02)
python services/consumer/scripts/run_consumer.py --replay-offset-0

# Offline verification mode (dry-run without live Kafka/Postgres)
python services/consumer/scripts/run_consumer.py --mock --max-messages 10

# Offline replay verification
python services/consumer/scripts/run_consumer.py --mock --replay-offset-0
```

---

## 5. Docker Compose & Makefile

```bash
# Start consumer container in Docker Compose
make up-consumer

# Follow consumer container logs
make logs-consumer

# Run local consumer test suite
make test-consumer
```
