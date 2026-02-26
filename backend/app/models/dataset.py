from __future__ import annotations
from datetime import datetime
from enum import Enum
from uuid import UUID, uuid4
from pydantic import BaseModel, Field


class FileFormat(str, Enum):
    CSV = "csv"
    JSON = "json"
    EXCEL = "excel"


class DatasetStatus(str, Enum):
    PENDING = "pending"
    PROFILING = "profiling"
    ANOMALY_DETECTION = "anomaly_detection"
    PLANNING = "planning"
    HEALING = "healing"
    COMPLETE = "complete"
    FAILED = "failed"


class DatasetMeta(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    filename: str
    format: FileFormat
    rows: int
    columns: int
    size_bytes: int
    column_names: list[str]
    uploaded_at: datetime = Field(default_factory=datetime.utcnow)
    status: DatasetStatus = DatasetStatus.PENDING
    job_id: str | None = None


class DatasetUploadResponse(BaseModel):
    dataset_id: UUID
    status: DatasetStatus
    message: str
