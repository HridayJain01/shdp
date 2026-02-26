"""Orchestrates all anomaly detectors and merges results."""
from __future__ import annotations

import uuid

import pandas as pd

from app.models.anomaly import Anomaly, AnomalyReport, Severity
from app.modules.anomaly.statistical import (
    check_missing_values,
    check_outliers_zscore,
    check_duplicate_rows,
    check_constant_columns,
)
from app.modules.anomaly.ml_detector import detect_multivariate_outliers


def detect(df: pd.DataFrame, dataset_id: uuid.UUID) -> AnomalyReport:
    anomalies: list[Anomaly] = []
    anomalies += check_missing_values(df)
    anomalies += check_outliers_zscore(df)
    anomalies += check_duplicate_rows(df)
    anomalies += check_constant_columns(df)
    anomalies += detect_multivariate_outliers(df)

    # Sort: critical first
    _order = {Severity.CRITICAL: 0, Severity.HIGH: 1, Severity.MEDIUM: 2, Severity.LOW: 3}
    anomalies.sort(key=lambda a: _order[a.severity])

    def _count(sev: Severity) -> int:
        return sum(1 for a in anomalies if a.severity == sev)

    return AnomalyReport(
        dataset_id=dataset_id,
        total_anomalies=len(anomalies),
        critical_count=_count(Severity.CRITICAL),
        high_count=_count(Severity.HIGH),
        medium_count=_count(Severity.MEDIUM),
        low_count=_count(Severity.LOW),
        anomalies=anomalies,
    )
