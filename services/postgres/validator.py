"""Data Quality (DQ) validation rules conforming strictly to REQ-C03 using Pydantic models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Union
from uuid import UUID
from pydantic import ValidationError

from snowflake_adapter.models.envelope import EventEnvelope
from services.postgres.models import ErrorCode, OrderPayload


@dataclass
class ValidationResult:
    """Outcome of Data Quality validation checks."""

    is_valid: bool
    envelope: Optional[EventEnvelope] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    event_id: Optional[UUID] = None
    entity_key: Optional[str] = None
    raw_payload: Optional[dict[str, Any]] = None


def validate_event(
    event_input: Union[dict[str, Any], EventEnvelope],
    event_id_checker: Optional[Any] = None,
) -> ValidationResult:
    """
    Execute mandatory Data Quality checks:
      1. DQ-1: Envelope schema conformance (valid UUID, required fields).
      2. DQ-2: Business logic validity (price/amount > 0 and non-null via OrderPayload).
      3. DQ-3: Duplicate event ID detection.
    """
    envelope: Optional[EventEnvelope] = None

    # Step 1: DQ-1 - Envelope schema conformance
    if isinstance(event_input, EventEnvelope):
        envelope = event_input
    elif isinstance(event_input, dict):
        try:
            envelope = EventEnvelope.model_validate(event_input)
        except Exception as e:
            raw_event_id = None
            if "event_id" in event_input:
                try:
                    raw_event_id = UUID(str(event_input["event_id"]))
                except Exception:
                    pass
            entity_k = str(event_input.get("entity_key")) if event_input.get("entity_key") else None
            return ValidationResult(
                is_valid=False,
                error_code=ErrorCode.SCHEMA_VIOLATION.value,
                error_message=f"Envelope schema validation failed: {str(e)}",
                event_id=raw_event_id,
                entity_key=entity_k,
                raw_payload=event_input,
            )
    else:
        return ValidationResult(
            is_valid=False,
            error_code=ErrorCode.SCHEMA_VIOLATION.value,
            error_message="Unsupported event input type.",
            raw_payload={"input": str(event_input)},
        )

    # Step 2: DQ-2 - Business logic rule check using OrderPayload validation
    payload = envelope.payload if isinstance(envelope.payload, dict) else {}
    price_field_found = False
    for price_key in ("O_TOTALPRICE", "o_totalprice", "total_price", "TOTAL_PRICE", "amount", "AMOUNT", "price", "PRICE"):
        if price_key in payload:
            price_field_found = True
            raw_val = payload[price_key]
            if raw_val is None:
                return ValidationResult(
                    is_valid=False,
                    envelope=envelope,
                    error_code=ErrorCode.BUSINESS_RULE_VIOLATION.value,
                    error_message="Business rule violated: order price/amount cannot be null.",
                    event_id=envelope.event_id,
                    entity_key=envelope.entity_key,
                    raw_payload=payload,
                )
            try:
                # Validate using OrderPayload or numeric bound
                OrderPayload.validate_positive_price(float(raw_val))
            except (ValueError, TypeError) as err:
                return ValidationResult(
                    is_valid=False,
                    envelope=envelope,
                    error_code=ErrorCode.BUSINESS_RULE_VIOLATION.value,
                    error_message=f"Business rule violated: {str(err)}",
                    event_id=envelope.event_id,
                    entity_key=envelope.entity_key,
                    raw_payload=payload,
                )
            break

    # Step 3: DQ-3 - Duplicate event ID check
    if event_id_checker is not None:
        try:
            is_duplicate = event_id_checker(envelope.event_id)
        except Exception:
            is_duplicate = False

        if is_duplicate:
            return ValidationResult(
                is_valid=False,
                envelope=envelope,
                error_code=ErrorCode.DUPLICATE_EVENT_ID.value,
                error_message=f"Duplicate event ID detected: {envelope.event_id} already exists in event_log.",
                event_id=envelope.event_id,
                entity_key=envelope.entity_key,
                raw_payload=payload,
            )

    return ValidationResult(
        is_valid=True,
        envelope=envelope,
        event_id=envelope.event_id,
        entity_key=envelope.entity_key,
        raw_payload=payload,
    )
