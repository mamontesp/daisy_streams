"""MinIO / S3 object storage state store implementation."""

from __future__ import annotations

from typing import Any, Optional
import boto3
import botocore.client
from botocore.exceptions import ClientError
from snowflake_adapter.logging import get_logger
from snowflake_adapter.models.state import SyncState

logger = get_logger(__name__)


class S3StateStore:
    """Persists sync state into an S3-compatible object store (Garage, MinIO, AWS S3) as JSON objects."""

    def __init__(
        self,
        endpoint: str = "localhost:3900",
        access_key: str = "GKdaisystreamskey01234",
        secret_key: str = "daisystreamssecretkey0123456789abcdef0123456789abcdef",
        bucket: str = "daisy-state",
        region_name: str = "garage",
        secure: bool = False,
        s3_client: Optional[Any] = None,
    ) -> None:
        self.bucket = bucket
        if s3_client is not None:
            self.client = s3_client
        else:
            protocol = "https" if secure else "http"
            endpoint_url = endpoint if endpoint.startswith("http") else f"{protocol}://{endpoint}"
            self.client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region_name,
                config=botocore.client.Config(
                    s3={"addressing_style": "path"},
                    signature_version="s3v4",
                ),
            )
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self) -> None:
        """Create bucket if it does not already exist."""
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError as e:
            error_code = str(e.response.get("Error", {}).get("Code", ""))
            if error_code in ("404", "NoSuchBucket"):
                logger.info("Bucket '%s' not found. Creating it...", self.bucket)
                self.client.create_bucket(Bucket=self.bucket)
            else:
                logger.warning("Could not head bucket '%s': %s", self.bucket, e)

    def _object_key(self, source_table: str) -> str:
        """Generate object key for table state."""
        return f"{source_table.lower()}/state.json"

    def load_state(self, source_table: str) -> Optional[SyncState]:
        """Load sync state from MinIO bucket."""
        key = self._object_key(source_table)
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
            body = response["Body"].read().decode("utf-8")
            state = SyncState.from_json(body)
            logger.info("Loaded sync state from MinIO: s3://%s/%s (watermark: %s)", self.bucket, key, state.last_watermark)
            return state
        except ClientError as e:
            error_code = str(e.response.get("Error", {}).get("Code", ""))
            if error_code in ("404", "NoSuchKey"):
                logger.info("No prior state found at s3://%s/%s. Starting fresh.", self.bucket, key)
                return None
            logger.error("Error reading state from s3://%s/%s: %s", self.bucket, key, e)
            raise
        except Exception as e:
            logger.error("Unexpected error reading state from MinIO: %s", e)
            raise

    def save_state(self, state: SyncState) -> None:
        """Persist sync state to MinIO bucket."""
        key = self._object_key(state.source_table)
        data = state.to_json().encode("utf-8")
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=data,
                ContentType="application/json",
            )
            logger.info(
                "Persisted sync state to MinIO: s3://%s/%s (records: %d, watermark: %s)",
                self.bucket,
                key,
                state.records_synced,
                state.last_watermark,
            )
        except Exception as e:
            logger.error("Failed to persist state to S3/Garage at s3://%s/%s: %s", self.bucket, key, e)
            raise


# Backward compatibility and convenient aliases
MinioStateStore = S3StateStore
GarageStateStore = S3StateStore
