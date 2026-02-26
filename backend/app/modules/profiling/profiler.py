"""Statistical and semantic profiling of a pandas DataFrame.

Public API
──────────
    profile_dataset(df, dataset_id) -> DatasetProfile

For each column this computes:
    • dtype + dtype_category + semantic_type
    • null_count / null_pct / non_null_count
    • unique_count / unique_pct
    • top-5 most frequent values (with count + % of total rows)
    • histogram buckets (numeric / datetime columns)
    • NumericStats: min, max, mean, median, std, variance, q1, q3, iqr,
                    skewness, kurtosis, zeros, negatives, inf_count
    • StringStats:  min_length, max_length, mean_length, blank_count
    • DatetimeStats: min, max, range_days
    • InferredConstraints: nullable, unique, constant, min/max value,
                           allowed_values (≤30 distinct), min/max_length, pattern
    • sample_values: up to 5 non-null examples

Dataset-level profile additionally carries:
    • duplicate_rows / duplicate_pct
    • complete_rows / complete_row_pct
    • column-type breakdown (numeric / categorical / datetime / boolean / text)
"""
from __future__ import annotations

import math
import uuid
from typing import Any

import numpy as np
import pandas as pd

from app.models.profile import (
    ColumnProfile,
    DatasetProfile,
    DatetimeStats,
    HistogramBucket,
    InferredConstraints,
    NumericStats,
    StringStats,
    TopValue,
)
from app.modules.profiling.schema_detector import SemanticTypeResult, detect_semantic_type

# ── Constants ────────────────────────────────────────────────────────────────

_TOP_N = 5              # top frequent values to keep
_HISTOGRAM_BINS = 20    # bins for numeric / datetime histograms
_SAMPLE_ROWS = 5        # sample values to store

# Semantic types that imply a specific regex constraint
_SEMANTIC_PATTERNS: dict[str, str] = {
    "email":    r"^[a-zA-Z0-9_.+%-]+@[a-zA-Z0-9-]+\.[a-zA-Z]{2,}$",
    "phone":    r"^\+?[\d\s\-().]{7,20}$",
    "uuid":     r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    "url":      r"^https?://[^\s/$.?#].[^\s]*$",
    "ip_address": r"^(?:\d{1,3}\.){3}\d{1,3}$",
    "zipcode":  r"^\d{5}(?:-\d{4})?$",
    "date":     r"^\d{4}-\d{2}-\d{2}$",
    "datetime": r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}",
    "currency": r"^[£€$¥₹]?[\d,]+(\.\d{1,4})?$",
    "percentage": r"^-?\d+(\.\d+)?\s?%$",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _safe_float(value: Any) -> float | None:
    """Convert value to float; return None if not finite or not numeric."""
    try:
        v = float(value)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _top_values(series: pd.Series, n: int = _TOP_N) -> list[TopValue]:
    total = len(series)
    vc = series.value_counts(dropna=True).head(n)
    return [
        TopValue(
            value=str(k),
            count=int(v),
            pct=round(v / total * 100, 2) if total else 0.0,
        )
        for k, v in vc.items()
    ]


def _histogram_numeric(series: pd.Series, bins: int = _HISTOGRAM_BINS) -> list[HistogramBucket]:
    clean = series.dropna().replace([np.inf, -np.inf], np.nan).dropna()
    if len(clean) < 2:
        return []
    try:
        counts, edges = np.histogram(clean, bins=bins)
        return [
            HistogramBucket(
                bucket_start=float(round(edges[i], 6)),
                bucket_end=float(round(edges[i + 1], 6)),
                count=int(counts[i]),
            )
            for i in range(len(counts))
        ]
    except Exception:
        return []


def _histogram_datetime(series: pd.Series, bins: int = _HISTOGRAM_BINS) -> list[HistogramBucket]:
    clean = series.dropna()
    if len(clean) < 2:
        return []
    try:
        timestamps = clean.astype("int64")
        counts, edges = np.histogram(timestamps, bins=bins)
        return [
            HistogramBucket(
                bucket_start=float(pd.Timestamp(edges[i]).timestamp()),
                bucket_end=float(pd.Timestamp(edges[i + 1]).timestamp()),
                count=int(counts[i]),
            )
            for i in range(len(counts))
        ]
    except Exception:
        return []


def _dtype_category(series: pd.Series, semantic: str) -> str:
    if pd.api.types.is_bool_dtype(series) or semantic == "boolean":
        return "boolean"
    if pd.api.types.is_datetime64_any_dtype(series) or semantic in ("datetime", "date"):
        return "datetime"
    if pd.api.types.is_numeric_dtype(series) or semantic in ("numeric", "numeric_id", "currency", "percentage"):
        return "numeric"
    if semantic == "categorical":
        return "categorical"
    if semantic in ("email", "phone", "uuid", "url", "ip_address", "zipcode", "json_string", "text"):
        return "text"
    return "unknown"


def _sample_values(series: pd.Series, n: int = _SAMPLE_ROWS) -> list[Any]:
    clean = series.dropna()
    if clean.empty:
        return []
    sample = clean.sample(min(n, len(clean)), random_state=0)
    return [v.item() if hasattr(v, "item") else v for v in sample]


# ── Per-column builders ──────────────────────────────────────────────────────

def _build_numeric_stats(series: pd.Series) -> NumericStats | None:
    clean = series.dropna().replace([np.inf, -np.inf], np.nan)
    finite = clean.dropna()
    if len(finite) == 0:
        return None
    q1 = float(finite.quantile(0.25))
    q3 = float(finite.quantile(0.75))
    return NumericStats(
        min=float(finite.min()),
        max=float(finite.max()),
        mean=float(finite.mean()),
        median=float(finite.median()),
        std=float(finite.std()),
        variance=float(finite.var()),
        q1=q1,
        q3=q3,
        iqr=round(q3 - q1, 6),
        skewness=float(finite.skew()),
        kurtosis=float(finite.kurtosis()),
        zeros=int((finite == 0).sum()),
        negatives=int((finite < 0).sum()),
        inf_count=int(series.replace([np.inf, -np.inf], np.nan).isna().sum() - series.isna().sum()),
    )


def _build_string_stats(series: pd.Series) -> StringStats | None:
    obj = series.dropna().astype(str)
    if obj.empty:
        return None
    lengths = obj.str.len()
    blank_count = int(obj.str.strip().eq("").sum())
    # min_length from non-blank values only
    non_blank = obj[obj.str.strip() != ""]
    min_len = int(non_blank.str.len().min()) if not non_blank.empty else 0
    return StringStats(
        min_length=min_len,
        max_length=int(lengths.max()),
        mean_length=round(float(lengths.mean()), 2),
        blank_count=blank_count,
    )


def _build_datetime_stats(series: pd.Series) -> DatetimeStats | None:
    clean = series.dropna()
    if clean.empty:
        return None
    try:
        if not pd.api.types.is_datetime64_any_dtype(clean):
            clean = pd.to_datetime(clean, infer_datetime_format=True, errors="coerce").dropna()
        if clean.empty:
            return None
        min_dt = clean.min()
        max_dt = clean.max()
        range_days = (max_dt - min_dt).days
        return DatetimeStats(
            min=min_dt.isoformat(),
            max=max_dt.isoformat(),
            range_days=float(range_days),
        )
    except Exception:
        return None


def _build_constraints(
    series: pd.Series,
    semantic_result: SemanticTypeResult,
    dtype_cat: str,
    numeric_stats: NumericStats | None,
    datetime_stats: DatetimeStats | None,
    string_stats: StringStats | None,
) -> InferredConstraints:
    n_non_null = int(series.count())
    n_unique = int(series.nunique(dropna=True))

    is_nullable = bool(series.isna().any())
    is_unique = n_non_null > 0 and n_unique == n_non_null
    is_constant = n_unique == 1

    min_value: float | str | None = None
    max_value: float | str | None = None
    allowed_values: list[str] | None = None
    min_length: int | None = None
    max_length: int | None = None
    pattern: str | None = None

    if numeric_stats:
        min_value = numeric_stats.min
        max_value = numeric_stats.max
    elif datetime_stats:
        min_value = datetime_stats.min
        max_value = datetime_stats.max

    # Allowed values for low-cardinality columns (≤30 distinct)
    if n_unique <= 30 and n_non_null > 0:
        allowed_values = [str(v) for v in series.dropna().unique().tolist()]

    if string_stats:
        min_length = string_stats.min_length
        max_length = string_stats.max_length

    pattern = _SEMANTIC_PATTERNS.get(semantic_result.type)

    return InferredConstraints(
        is_nullable=is_nullable,
        is_unique=is_unique,
        is_constant=is_constant,
        min_value=min_value,
        max_value=max_value,
        allowed_values=allowed_values,
        min_length=min_length,
        max_length=max_length,
        pattern=pattern,
    )


# ── Main column profiler ─────────────────────────────────────────────────────

def profile_column(series: pd.Series, position: int = 0) -> ColumnProfile:
    """Return a fully-populated :class:`ColumnProfile` for *series*."""
    n = len(series)
    null_count = int(series.isna().sum())
    non_null_count = n - null_count
    unique_count = int(series.nunique(dropna=True))

    semantic_result = detect_semantic_type(series)
    dtype_cat = _dtype_category(series, semantic_result.type)

    # Type-specific stats
    numeric_stats: NumericStats | None = None
    string_stats: StringStats | None = None
    datetime_stats: DatetimeStats | None = None
    histogram: list[HistogramBucket] = []

    if dtype_cat == "numeric":
        numeric_stats = _build_numeric_stats(series)
        histogram = _histogram_numeric(series)
    elif dtype_cat == "datetime":
        datetime_stats = _build_datetime_stats(series)
        # Try converting to datetime for histogram
        dt_series = series
        if not pd.api.types.is_datetime64_any_dtype(series):
            dt_series = pd.to_datetime(series, infer_datetime_format=True, errors="coerce")
        histogram = _histogram_datetime(dt_series)
    elif dtype_cat in ("text", "categorical"):
        string_stats = _build_string_stats(series)

    constraints = _build_constraints(
        series, semantic_result, dtype_cat,
        numeric_stats, datetime_stats, string_stats,
    )

    return ColumnProfile(
        name=str(series.name),
        position=position,
        dtype=str(series.dtype),
        dtype_category=dtype_cat,
        semantic_type=semantic_result.type,
        null_count=null_count,
        null_pct=round(null_count / n * 100, 2) if n else 0.0,
        non_null_count=non_null_count,
        unique_count=unique_count,
        unique_pct=round(unique_count / non_null_count * 100, 2) if non_null_count else 0.0,
        top_values=_top_values(series),
        histogram=histogram,
        numeric_stats=numeric_stats,
        string_stats=string_stats,
        datetime_stats=datetime_stats,
        constraints=constraints,
        sample_values=_sample_values(series),
    )


# ── Dataset profiler ─────────────────────────────────────────────────────────

def profile_dataset(df: pd.DataFrame, dataset_id: uuid.UUID) -> DatasetProfile:
    """Profile every column in *df* and return a :class:`DatasetProfile`.

    The returned object is fully JSON-serialisable via ``.model_dump()``.
    """
    columns = [profile_column(df[col], pos) for pos, col in enumerate(df.columns)]

    dup_rows = int(df.duplicated().sum())
    n = len(df)
    null_per_row = df.isna().sum(axis=1)
    complete_rows = int((null_per_row == 0).sum())

    type_counter = {
        "numeric": 0,
        "categorical": 0,
        "datetime": 0,
        "boolean": 0,
        "text": 0,
    }
    for col in columns:
        key = col.dtype_category if col.dtype_category in type_counter else "text"
        type_counter[key] += 1

    return DatasetProfile(
        dataset_id=dataset_id,
        row_count=n,
        column_count=len(df.columns),
        memory_mb=round(df.memory_usage(deep=True).sum() / 1_048_576, 3),
        duplicate_rows=dup_rows,
        duplicate_pct=round(dup_rows / n * 100, 2) if n else 0.0,
        complete_rows=complete_rows,
        complete_row_pct=round(complete_rows / n * 100, 2) if n else 0.0,
        numeric_column_count=type_counter["numeric"],
        categorical_column_count=type_counter["categorical"],
        datetime_column_count=type_counter["datetime"],
        boolean_column_count=type_counter["boolean"],
        text_column_count=type_counter["text"],
        columns=columns,
    )
