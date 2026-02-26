"""Pydantic models for the dataset profiling layer.

Hierarchy
─────────
DatasetProfile
  └─ ColumnProfile  (one per column)
       ├─ NumericStats      (only when dtype_category == "numeric")
       ├─ StringStats       (only when dtype_category == "text" | "categorical")
       ├─ DatetimeStats     (only when dtype_category == "datetime")
       ├─ TopValue × N      (top-5 frequent values)
       ├─ HistogramBucket × N (numeric / datetime distributions)
       └─ InferredConstraints
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


# ── Sub-models ──────────────────────────────────────────────────────────────

class TopValue(BaseModel):
    value: str
    count: int
    pct: float          # fraction of total rows (including nulls)


class HistogramBucket(BaseModel):
    bucket_start: float
    bucket_end: float
    count: int


class NumericStats(BaseModel):
    min: float
    max: float
    mean: float
    median: float
    std: float
    variance: float
    q1: float           # 25th percentile
    q3: float           # 75th percentile
    iqr: float          # interquartile range
    skewness: float
    kurtosis: float
    zeros: int          # count of exactly-zero values
    negatives: int      # count of negative values
    inf_count: int      # count of ±inf values


class StringStats(BaseModel):
    min_length: int
    max_length: int
    mean_length: float
    blank_count: int    # non-null but empty / whitespace-only


class DatetimeStats(BaseModel):
    min: str            # ISO-8601
    max: str            # ISO-8601
    range_days: float


class InferredConstraints(BaseModel):
    is_nullable: bool               # any nulls present
    is_unique: bool                 # all non-null values are unique
    is_constant: bool               # single distinct value
    min_value: float | str | None = None    # numeric / date lower bound
    max_value: float | str | None = None    # numeric / date upper bound
    allowed_values: list[str] | None = None # set for low-cardinality columns
    min_length: int | None = None           # string min non-empty length
    max_length: int | None = None           # string max length
    pattern: str | None = None             # regex inferred from semantic type


# ── Main column profile ─────────────────────────────────────────────────────

class ColumnProfile(BaseModel):
    # Identity
    name: str
    position: int                   # 0-based column index

    # Type information
    dtype: str                      # raw pandas dtype string
    dtype_category: str             # "numeric" | "categorical" | "datetime" | "boolean" | "text" | "unknown"
    semantic_type: str | None = None  # email | phone | date | url | ip | uuid | currency | numeric_id | categorical | text | boolean | datetime

    # Null / completeness
    null_count: int
    null_pct: float                 # percentage 0–100
    non_null_count: int

    # Cardinality
    unique_count: int
    unique_pct: float               # percentage of non-null values

    # Distribution (always present)
    top_values: list[TopValue] = Field(default_factory=list)
    histogram: list[HistogramBucket] = Field(default_factory=list)

    # Type-specific stats (None when not applicable)
    numeric_stats: NumericStats | None = None
    string_stats: StringStats | None = None
    datetime_stats: DatetimeStats | None = None

    # Inferred constraints
    constraints: InferredConstraints = Field(
        default_factory=lambda: InferredConstraints(
            is_nullable=True, is_unique=False, is_constant=False
        )
    )

    # Sample values (up to 5 non-null examples)
    sample_values: list[Any] = Field(default_factory=list)


# ── Dataset-level profile ────────────────────────────────────────────────────

class DatasetProfile(BaseModel):
    dataset_id: UUID

    # Shape
    row_count: int
    column_count: int
    memory_mb: float

    # Dataset-wide quality signals
    duplicate_rows: int
    duplicate_pct: float
    complete_rows: int              # rows with zero nulls
    complete_row_pct: float

    # Column breakdown by type
    numeric_column_count: int
    categorical_column_count: int
    datetime_column_count: int
    boolean_column_count: int
    text_column_count: int

    # Per-column profiles
    columns: list[ColumnProfile]
