"""
Quality metrics — one function per dimension, each returning
    (ratio_or_fraction_0_to_1, issue_count_int, affected_column_names_list)

The raw value semantics differ per dimension:
  missing_ratio     → ratio of missing cells   (0 = perfect)
  duplicate_ratio   → ratio of duplicate rows  (0 = perfect)
  outlier_ratio_iqr → ratio of outlier values  (0 = perfect)
  format_validity_score   → fraction of valid  (1 = perfect)
  schema_consistency_score → fraction consistent (1 = perfect)

Legacy one-liners at the bottom keep backward compatibility with scorer.py.
"""
from __future__ import annotations

import re
from typing import Any

import numpy as np
import pandas as pd


# ─── 1. Missing ratio ─────────────────────────────────────────────────────────

def missing_ratio(df: pd.DataFrame) -> tuple[float, int, list[str]]:
    """Returns (missing_cell_fraction, missing_cell_count, columns_with_nulls)."""
    total = df.size
    if total == 0:
        return 0.0, 0, []
    null_counts = df.isnull().sum()
    affected = [str(col) for col, cnt in null_counts.items() if cnt > 0]
    missing_cells = int(null_counts.sum())
    return float(missing_cells / total), missing_cells, affected


# ─── 2. Duplicate ratio ───────────────────────────────────────────────────────

def duplicate_ratio(df: pd.DataFrame) -> tuple[float, int, list[str]]:
    """Returns (dup_row_fraction, dup_row_count, [])."""
    n = len(df)
    if n == 0:
        return 0.0, 0, []
    dup_count = int(df.duplicated(keep="first").sum())
    return float(dup_count / n), dup_count, []


# ─── 3. Outlier ratio (IQR) ───────────────────────────────────────────────────

def outlier_ratio_iqr(
    df: pd.DataFrame,
    iqr_factor: float = 1.5,
) -> tuple[float, int, list[str]]:
    """
    Fraction of numeric cell values that are IQR-outliers.
    Returns (outlier_fraction_of_numeric_cells, outlier_count, affected_columns).
    Columns with zero IQR (constant) are skipped.
    """
    num_cols = df.select_dtypes(include="number").columns.tolist()
    if not num_cols:
        return 0.0, 0, []

    total_numeric = 0
    total_outliers = 0
    affected: list[str] = []

    for col in num_cols:
        series = df[col].dropna()
        if series.empty:
            continue
        q1 = float(series.quantile(0.25))
        q3 = float(series.quantile(0.75))
        iqr = q3 - q1
        if iqr == 0.0:
            # constant column — no meaningful outliers
            total_numeric += len(series)
            continue
        lower = q1 - iqr_factor * iqr
        upper = q3 + iqr_factor * iqr
        n_out = int(((series < lower) | (series > upper)).sum())
        total_numeric += len(series)
        total_outliers += n_out
        if n_out > 0:
            affected.append(str(col))

    if total_numeric == 0:
        return 0.0, 0, []
    return float(total_outliers / total_numeric), total_outliers, affected


# ─── 4. Format validity ───────────────────────────────────────────────────────

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_BOOL_VALUES = {"true", "false", "yes", "no", "1", "0", "t", "f", "y", "n"}


def _invalid_email(series: pd.Series) -> int:
    non_null = series.dropna().astype(str).str.strip()
    return int((~non_null.str.match(_EMAIL_RE.pattern)).sum())


def _invalid_numeric(series: pd.Series) -> int:
    non_null = series.dropna()
    coerced = pd.to_numeric(non_null, errors="coerce")
    return int(coerced.isna().sum())


def _invalid_date(series: pd.Series) -> int:
    non_null = series.dropna()
    parsed = pd.to_datetime(non_null, errors="coerce", infer_datetime_format=True)
    return int(parsed.isna().sum())


def _invalid_bool(series: pd.Series) -> int:
    non_null = series.dropna().astype(str).str.strip().str.lower()
    return int((~non_null.isin(_BOOL_VALUES)).sum())


_SEM_CHECKERS: dict[str, Any] = {
    "email":      _invalid_email,
    "numeric":    _invalid_numeric,
    "numeric_id": _invalid_numeric,
    "datetime":   _invalid_date,
    "date":       _invalid_date,
    "boolean":    _invalid_bool,
}


def format_validity_score(
    df: pd.DataFrame,
    profile: Any = None,
) -> tuple[float, int, list[str]]:
    """
    Fraction of non-null values that match the expected format for their column.
    Uses ``profile.columns[*].semantic_type`` when available; falls back to
    dtype-based heuristics (Inf checks for numeric, Inf-in-string for object).
    Returns (valid_fraction 0–1, invalid_count, affected_columns).
    """
    # Build semantic-type map from profile
    sem_map: dict[str, str] = {}
    if profile is not None:
        try:
            for cp in profile.columns:
                if cp.semantic_type:
                    sem_map[str(cp.name)] = str(cp.semantic_type).lower()
        except Exception:
            pass

    total_checked = 0
    total_invalid = 0
    affected: list[str] = []

    for col in df.columns:
        series = df[col]
        n_non_null = int(series.notna().sum())
        if n_non_null == 0:
            continue

        sem = sem_map.get(str(col), "")
        checker = _SEM_CHECKERS.get(sem)

        if checker is not None:
            invalid = checker(series)
        elif series.dtype == object:
            # Heuristic: count Inf-like strings and values that look
            # like numbers but are actually mixing strings/non-strings
            try:
                coerced = pd.to_numeric(series.dropna(), errors="coerce")
                invalid = int(np.isinf(coerced.dropna()).sum())
            except Exception:
                invalid = 0
        else:
            # Pure numeric — flag Inf values
            try:
                invalid = int(np.isinf(series.dropna()).sum())
            except Exception:
                invalid = 0

        total_checked += n_non_null
        total_invalid += invalid
        if invalid > 0:
            affected.append(str(col))

    if total_checked == 0:
        return 1.0, 0, []
    return float((total_checked - total_invalid) / total_checked), total_invalid, affected


# ─── 5. Schema consistency ────────────────────────────────────────────────────

_NULL_LIKE = {"none", "nan", "null", "na", "n/a", "nil"}


def schema_consistency_score(
    df: pd.DataFrame,
    profile: Any = None,
) -> tuple[float, int, list[str]]:
    """
    Fraction of columns that are type-consistent.
    A column is flagged as inconsistent when:
    - Its dtype is ``object`` and contains more than one distinct Python type
      among non-null values, OR
    - Its dtype is ``object``, all values are strings, but some hold null-like
      sentinel strings ("none", "nan", etc.), OR
    - Its dtype is float/float32 and contains ``Inf`` values.
    Returns (consistent_fraction 0–1, inconsistent_count, affected_columns).
    """
    total = len(df.columns)
    if total == 0:
        return 1.0, 0, []

    inconsistent: list[str] = []
    for col in df.columns:
        series = df[col]
        if series.dtype == object:
            non_null = series.dropna()
            if non_null.empty:
                continue
            types = non_null.map(type).unique()
            if len(types) > 1:
                inconsistent.append(str(col))
                continue
            # All same Python type — check for null-like string sentinels
            if len(types) == 1 and types[0] is str:
                if non_null.str.strip().str.lower().isin(_NULL_LIKE).any():
                    inconsistent.append(str(col))
        elif series.dtype in (float, np.float32, np.float64):
            if np.isinf(series.dropna()).any():
                inconsistent.append(str(col))

    n_bad = len(inconsistent)
    return float((total - n_bad) / total), n_bad, inconsistent


# ─── Legacy aliases (kept for backward compat) ────────────────────────────────

def completeness(df: pd.DataFrame) -> float:
    ratio, _, _ = missing_ratio(df)
    return 1.0 - ratio


def uniqueness(df: pd.DataFrame) -> float:
    ratio, _, _ = duplicate_ratio(df)
    return 1.0 - ratio


def validity(df: pd.DataFrame) -> float:
    score, _, _ = format_validity_score(df)
    return score


def consistency(df: pd.DataFrame) -> float:
    score, _, _ = schema_consistency_score(df)
    return score


def timeliness(df: pd.DataFrame) -> float:
    """
    Returns fraction of rows with timestamp/date column within the last 365 days.
    Falls back to 1.0 when no date-like column is present.
    """
    candidates = [
        c for c in df.columns
        if any(k in str(c).lower() for k in ("updated", "created", "timestamp", "date"))
    ]
    if not candidates:
        return 1.0
    col = candidates[0]
    try:
        parsed = pd.to_datetime(df[col], errors="coerce")
        cutoff = pd.Timestamp.utcnow().tz_localize(None) - pd.Timedelta(days=365)
        return float((parsed.dt.tz_localize(None) >= cutoff).mean())
    except Exception:
        return 1.0
