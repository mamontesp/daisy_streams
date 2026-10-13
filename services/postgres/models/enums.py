"""Error codes for Data Quality violations."""

from enum import Enum


class ErrorCode(str, Enum):
    """Categorized error codes for Data Quality violations."""

    SCHEMA_VIOLATION = "SCHEMA_VIOLATION"
    BUSINESS_RULE_VIOLATION = "BUSINESS_RULE_VIOLATION"
    DUPLICATE_EVENT_ID = "DUPLICATE_EVENT_ID"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"
