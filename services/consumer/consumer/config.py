"""Configuration settings for Kafka Consumer Service."""

from __future__ import annotations

from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConsumerSettings(BaseSettings):
    """Configuration for Kafka Consumer Service reading strictly from environment/.env."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Kafka Topic to subscribe to (strictly from KAFKA_TOPIC in .env)
    kafka_topic: str = Field(
        default="switch.orders.events",
        alias="KAFKA_TOPIC",
        description="Kafka topic name to subscribe to",
    )

    # Kafka Broker Configuration
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        alias="KAFKA_BOOTSTRAP_SERVERS",
        description="Kafka broker bootstrap server list",
    )
    kafka_consumer_group: str = Field(
        default="daisy-streams-consumer-group",
        alias="KAFKA_CONSUMER_GROUP",
        description="Consumer group identifier for partition balancing and offset tracking",
    )
    kafka_client_id: str = Field(
        default="daisy-streams-consumer",
        alias="KAFKA_CLIENT_ID",
        description="Client identifier for logging and broker metrics",
    )
    kafka_auto_offset_reset: str = Field(
        default="earliest",
        alias="KAFKA_AUTO_OFFSET_RESET",
        description="Offset reset position when no committed offset exists (earliest | latest)",
    )

    # Consumer Ingestion Loop Settings
    consumer_poll_timeout_sec: float = Field(
        default=1.0,
        alias="CONSUMER_POLL_TIMEOUT_SEC",
        description="Timeout in seconds when polling broker for messages",
    )
    consumer_batch_size: int = Field(
        default=100,
        alias="CONSUMER_BATCH_SIZE",
        description="Maximum batch size for chunked consumption",
    )
