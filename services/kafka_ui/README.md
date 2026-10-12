# Kafka UI Service

[Kafka UI](https://github.com/provectus/kafka-ui) by Provectus Labs is a free, open-source web UI for monitoring and managing Apache Kafka clusters.

In Daisy Streams, Kafka UI provides a visual dashboard for observing topics, inspecting JSON event envelopes produced by the Snowflake Adapter, and monitoring consumer group lag and partition distribution.

---

## 1. Service Configuration

- **Docker Image**: `provectuslabs/kafka-ui:latest`
- **Container Name**: `daisy-kafka-ui`
- **Dashboard URL**: `http://localhost:8080` (or `http://localhost:${KAFKA_UI_PORT}`)
- **Container Port**: `8080` (mapped to `${KAFKA_UI_PORT:-8080}:8080`)
- **Bootstrap Servers**: `kafka:29092` (via internal Docker network `daisy-network`)
- **Cluster Name**: `daisy-kafka`
- **Dynamic Config**: `DYNAMIC_CONFIG_ENABLED=true`

---

## 2. Key Capabilities in Daisy Streams

1. **Topic Inspection (`switch.orders.events`)**:
   - Inspect topic partitions, replication factor, and message rates.
   - Browse published `EventEnvelope` JSON records in real-time.
   - Filter messages by timestamp, partition, or offset.

2. **Consumer Group Monitoring**:
   - Track consumer lag for `daisy-streams-consumer-group`.
   - Monitor partition assignments and committed offsets.

3. **Broker & Cluster Health**:
   - View broker node status, KRaft controller metadata, and partition in-sync replicas (ISR).

---

## 3. Operational Guide

### 3.1 Starting the Service

```bash
# Start all services including Kafka UI
make up

# Or start Kafka UI independently (requires Kafka running)
make up-ui
# or: docker compose up -d kafka-ui
```

### 3.2 Inspecting Logs

```bash
# Follow live Kafka UI container logs
make logs-ui
# or: docker compose logs -f kafka-ui
```

### 3.3 Accessing the Dashboard

Once started, navigate to:
```
http://localhost:8080
```
