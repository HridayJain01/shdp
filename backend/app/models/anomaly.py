from __future__ import annotations
from enum import Enum
from pydantic import BaseModel, Field
from uuid import UUID


class Severity(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class AnomalyType(str, Enum):
    MISSING_VALUES = "missing_values"
    OUTLIER = "outlier"
    DUPLICATE_ROWS = "duplicate_rows"
    TYPE_MISMATCH = "type_mismatch"
    CONSTANT_COLUMN = "constant_column"
    MULTIVARIATE_OUTLIER = "multivariate_outlier"
    FORMAT_VIOLATION = "format_violation"
    RANGE_VIOLATION = "range_violation"


class Anomaly(BaseModel):
    id: str
    column: str | None = None              # None for row-level anomalies
    anomaly_type: AnomalyType
    severity: Severity
    affected_rows: int
    affected_rate: float                   # fraction of total rows
    description: str
    sample_values: list = Field(default_factory=list)


class AnomalyReport(BaseModel):
    dataset_id: UUID
    total_anomalies: int
    critical_count: int
    high_count: int
    medium_count: int
    low_count: int
    anomalies: list[Anomaly]
