"""Structured exception hierarchy for the ingestion layer.

All exceptions carry a machine-readable ``code`` string so upstream
API handlers can map them to HTTP status codes without string parsing.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class IngestionError(Exception):
    """Base class for all ingestion exceptions."""

    message: str
    code: str = "INGESTION_ERROR"
    details: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return self.message

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "details": self.details}


@dataclass
class UnsupportedFormatError(IngestionError):
    """Raised when the file extension or MIME type is not supported."""

    code: str = "UNSUPPORTED_FORMAT"


@dataclass
class EncodingDetectionError(IngestionError):
    """Raised when encoding cannot be detected with sufficient confidence."""

    code: str = "ENCODING_DETECTION_FAILED"


@dataclass
class ParseError(IngestionError):
    """Raised when the file cannot be parsed into a DataFrame at all."""

    code: str = "PARSE_FAILED"


@dataclass
class EmptyDatasetError(IngestionError):
    """Raised when the parsed DataFrame has zero rows or zero columns."""

    code: str = "EMPTY_DATASET"


@dataclass
class CorruptRowsError(IngestionError):
    """
    Raised when too many rows were dropped due to corruption.
    Carries the recovered DataFrame in ``details["dataframe"]`` so
    callers can decide whether to proceed with partial data.
    """

    code: str = "CORRUPT_ROWS_EXCEEDED_THRESHOLD"


@dataclass
class ValidationError(IngestionError):
    """Raised when the DataFrame violates size / shape constraints."""

    code: str = "VALIDATION_FAILED"


@dataclass
class SchemaError(IngestionError):
    """Raised when JSON/Excel structure is incompatible (e.g. nested non-tabular)."""

    code: str = "SCHEMA_ERROR"
