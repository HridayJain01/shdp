"""Statistical anomaly detectors: Z-score, IQR, missing-rate, type issues."""
from __future__ import annotations

import uuid

import pandas as pd
import numpy as np

from app.models.anomaly import Anomaly, AnomalyType, Severity


def _make_id() -> str:
    return str(uuid.uuid4())[:8]


def check_missing_values(df: pd.DataFrame, threshold: float = 0.05) -> list[Anomaly]:
    anomalies = []
    for col in df.columns:
        rate = df[col].isna().mean()
        if rate > 0:
            sev = (
                Severity.CRITICAL if rate > 0.5
                else Severity.HIGH if rate > 0.3
                else Severity.MEDIUM if rate > threshold
                else Severity.LOW
            )
            anomalies.append(Anomaly(
                id=_make_id(),
                column=col,
                anomaly_type=AnomalyType.MISSING_VALUES,
                severity=sev,
                affected_rows=int(df[col].isna().sum()),
                affected_rate=round(rate, 4),
                description=f"Column '{col}' has {rate:.1%} missing values.",
                sample_values=[],
            ))
    return anomalies


def check_outliers_zscore(df: pd.DataFrame, z_thresh: float = 3.0) -> list[Anomaly]:
    anomalies = []
    num_cols = df.select_dtypes(include="number").columns
    for col in num_cols:
        s = df[col].dropna()
        if s.std() == 0:
            continue
        z = (s - s.mean()) / s.std()
        outlier_mask = z.abs() > z_thresh
        count = int(outlier_mask.sum())
        if count:
            rate = count / len(df)
            sev = Severity.HIGH if rate > 0.05 else Severity.MEDIUM
            samples = df.loc[s[outlier_mask].index, col].head(5).tolist()
            anomalies.append(Anomaly(
                id=_make_id(),
                column=col,
                anomaly_type=AnomalyType.OUTLIER,
                severity=sev,
                affected_rows=count,
                affected_rate=round(rate, 4),
                description=f"Column '{col}' has {count} outliers (|Z| > {z_thresh}).",
                sample_values=samples,
            ))
    return anomalies


def check_duplicate_rows(df: pd.DataFrame) -> list[Anomaly]:
    dup_count = int(df.duplicated().sum())
    if dup_count == 0:
        return []
    rate = dup_count / len(df)
    sev = Severity.HIGH if rate > 0.1 else Severity.MEDIUM
    return [Anomaly(
        id=_make_id(),
        column=None,
        anomaly_type=AnomalyType.DUPLICATE_ROWS,
        severity=sev,
        affected_rows=dup_count,
        affected_rate=round(rate, 4),
        description=f"{dup_count} duplicate rows detected ({rate:.1%} of data).",
    )]


def check_constant_columns(df: pd.DataFrame) -> list[Anomaly]:
    anomalies = []
    for col in df.columns:
        if df[col].nunique(dropna=True) <= 1:
            anomalies.append(Anomaly(
                id=_make_id(),
                column=col,
                anomaly_type=AnomalyType.CONSTANT_COLUMN,
                severity=Severity.LOW,
                affected_rows=len(df),
                affected_rate=1.0,
                description=f"Column '{col}' has only one unique value — may be useless.",
            ))
    return anomalies
