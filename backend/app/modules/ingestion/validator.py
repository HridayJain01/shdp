"""Ingestion validator: enforce size / shape constraints.

All failures raise :class:`~app.modules.ingestion.exceptions.ValidationError`
with a structured ``code`` and ``details`` dict so API handlers can give
precise, actionable error messages without inspecting message strings.
"""
from __future__ import annotations

import logging

import pandas as pd

from app.core.config import settings
from app.modules.ingestion.exceptions import ValidationError

logger = logging.getLogger(__name__)


def validate(df: pd.DataFrame, file_size_bytes: int) -> None:
    """
    Validate *df* and *file_size_bytes* against configured platform limits.

    Args:
        df:               Already-parsed DataFrame (post-normalisation).
        file_size_bytes:  Raw file size in bytes from the upload layer.

    Raises:
        :exc:`ValidationError` with a descriptive ``code`` on any violation.
    """
    max_bytes = settings.MAX_UPLOAD_MB * 1024 * 1024

    if file_size_bytes > max_bytes:
        raise ValidationError(
            message=(
                f"File size {file_size_bytes / 1e6:.1f} MB exceeds the "
                f"{settings.MAX_UPLOAD_MB} MB upload limit."
            ),
            code="FILE_TOO_LARGE",
            details={
                "file_size_mb": round(file_size_bytes / 1e6, 2),
                "limit_mb": settings.MAX_UPLOAD_MB,
            },
        )

    if df.empty:
        raise ValidationError(
            message="Dataset is empty — no rows to process.",
            code="EMPTY_DATASET",
            details={},
        )

    if len(df) > settings.MAX_ROWS:
        raise ValidationError(
            message=(
                f"Row count {len(df):,} exceeds the platform limit of "
                f"{settings.MAX_ROWS:,} rows."
            ),
            code="TOO_MANY_ROWS",
            details={
                "row_count": len(df),
                "limit": settings.MAX_ROWS,
            },
        )

    if len(df.columns) > settings.MAX_COLUMNS:
        raise ValidationError(
            message=(
                f"Column count {len(df.columns)} exceeds the platform limit of "
                f"{settings.MAX_COLUMNS} columns."
            ),
            code="TOO_MANY_COLUMNS",
            details={
                "column_count": len(df.columns),
                "limit": settings.MAX_COLUMNS,
            },
        )

    logger.debug(
        "validation_passed",
        rows=len(df),
        columns=len(df.columns),
        size_mb=round(file_size_bytes / 1e6, 3),
    )
