"""ML-based anomaly detection: IsolationForest + LocalOutlierFactor."""
from __future__ import annotations

import uuid

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn.preprocessing import StandardScaler

from app.models.anomaly import Anomaly, AnomalyType, Severity


def _make_id() -> str:
    return str(uuid.uuid4())[:8]


def detect_multivariate_outliers(
    df: pd.DataFrame,
    contamination: float = 0.05,
    min_columns: int = 2,
) -> list[Anomaly]:
    """Run IsolationForest + LOF on numeric columns; flag rows predicted as outliers by both."""
    num_df = df.select_dtypes(include="number").dropna(axis=1, thresh=int(len(df) * 0.5))
    if len(num_df.columns) < min_columns:
        return []

    X = num_df.fillna(num_df.median())
    X_scaled = StandardScaler().fit_transform(X)

    iso = IsolationForest(contamination=contamination, random_state=42, n_jobs=-1)
    iso_preds = iso.fit_predict(X_scaled)           # -1 = outlier

    lof = LocalOutlierFactor(contamination=contamination, n_jobs=-1)
    lof_preds = lof.fit_predict(X_scaled)           # -1 = outlier

    # Consensus: both agree it's an outlier
    consensus = (iso_preds == -1) & (lof_preds == -1)
    count = int(consensus.sum())
    if count == 0:
        return []

    rate = count / len(df)
    sev = Severity.CRITICAL if rate > 0.1 else Severity.HIGH if rate > 0.05 else Severity.MEDIUM

    return [Anomaly(
        id=_make_id(),
        column=None,
        anomaly_type=AnomalyType.MULTIVARIATE_OUTLIER,
        severity=sev,
        affected_rows=count,
        affected_rate=round(rate, 4),
        description=(
            f"{count} rows flagged as multivariate outliers by IsolationForest + LOF "
            f"on {len(num_df.columns)} numeric features."
        ),
    )]
