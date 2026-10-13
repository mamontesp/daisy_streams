"""Configuration settings for Snowflake Adapter Service."""

from __future__ import annotations

from typing import Literal, Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration for Snowflake Adapter Service."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Snowflake Source Configuration
    snowflake_account: str = Field(default="xy12345.us-east-1", alias="SNOWFLAKE_ACCOUNT")
    snowflake_user: str = Field(default="DAISY_SERVICE_USER", alias="SNOWFLAKE_USER")
    snowflake_authenticator: Literal["key_pair", "externalbrowser"] = Field(
        default="key_pair", alias="SNOWFLAKE_AUTHENTICATOR"
    )
    snowflake_private_key_path: Optional[str] = Field(
        default=None, alias="SNOWFLAKE_PRIVATE_KEY_PATH"
    )
    snowflake_private_key_content: Optional[str] = Field(
        default=None, alias="SNOWFLAKE_PRIVATE_KEY_CONTENT"
    )
    snowflake_private_key_passphrase: Optional[str] = Field(
        default=None, alias="SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"
    )
    snowflake_role: str = Field(default="DAISY_INGESTION_ROLE", alias="SNOWFLAKE_ROLE")
    snowflake_warehouse: str = Field(default="COMPUTE_WH", alias="SNOWFLAKE_WAREHOUSE")
    snowflake_database: str = Field(default="daisy_streams", alias="SNOWFLAKE_DATABASE")
    snowflake_schema: str = Field(default="ingestion", alias="SNOWFLAKE_SCHEMA")
    snowflake_table: str = Field(default="ORDERS", alias="SNOWFLAKE_TABLE")
    snowflake_watermark_column: str = Field(default="O_ORDERDATE", alias="SNOWFLAKE_WATERMARK_COLUMN")
    snowflake_entity_key_column: str = Field(default="O_CUSTKEY", alias="SNOWFLAKE_ENTITY_KEY_COLUMN")
    snowflake_partition_key_column: str = Field(default="O_CUSTKEY", alias="SNOWFLAKE_PARTITION_KEY_COLUMN")

    # Ingestion / Sync Settings
    sync_batch_size: int = Field(default=10000, alias="SYNC_BATCH_SIZE")
    sync_initial_bound: int = Field(default=50000, alias="SYNC_INITIAL_BOUND")
    sync_state_storage: Literal["garage", "minio", "local"] = Field(
        default="garage", alias="SYNC_STATE_STORAGE"
    )
    sync_state_file_path: str = Field(default=".daisy_sync_state.json", alias="SYNC_STATE_FILE_PATH")

    # Garage / S3 Object Storage State Persistence Configuration
    garage_endpoint: str = Field(default="localhost:3900", alias="GARAGE_ENDPOINT")
    garage_access_key: str = Field(default="GKdaisystreamskey01234", alias="GARAGE_ACCESS_KEY")
    garage_secret_key: str = Field(
        default="daisystreamssecretkey0123456789abcdef0123456789abcdef",
        alias="GARAGE_SECRET_KEY",
    )
    garage_bucket: str = Field(default="daisy-state", alias="GARAGE_BUCKET")
    garage_region: str = Field(default="garage", alias="GARAGE_REGION")
    garage_secure: bool = Field(default=False, alias="GARAGE_SECURE")

    # MinIO Fallback Compatibility
    minio_endpoint: str = Field(default="localhost:9000", alias="MINIO_ENDPOINT")
    minio_access_key: str = Field(default="minioadmin", alias="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field(default="minioadmin", alias="MINIO_SECRET_KEY")
    minio_bucket: str = Field(default="daisy-state", alias="MINIO_BUCKET")
    minio_secure: bool = Field(default=False, alias="MINIO_SECURE")

    # Apache Kafka Broker Configuration
    kafka_bootstrap_servers: str = Field(default="localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic: str = Field(default="switch.orders.events", alias="KAFKA_TOPIC")
    kafka_client_id: str = Field(default="daisy-streams-adapter", alias="KAFKA_CLIENT_ID")

    # Observability
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    environment: str = Field(default="development", alias="ENVIRONMENT")


_settings_instance: Optional[Settings] = None


def get_settings() -> Settings:
    """Return cached singleton settings instance or instantiate new one."""
    global _settings_instance
    if _settings_instance is None:
        _settings_instance = Settings()
    return _settings_instance
