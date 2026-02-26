"""Shared Pydantic response envelopes used across all API routes.

Every endpoint returns either a typed data model directly (for Swagger
clarity) or a wrapped :class:`APIResponse` for uniform error surfacing.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Generic, Optional, TypeVar
from uuid import UUID

from pydantic import BaseModel, Field

T = TypeVar("T")


# ── Error detail ──────────────────────────────────────────────────────────────

class ErrorDetail(BaseModel):
    code: str
    message: str
    detail: Any = None


# ── Generic envelope ──────────────────────────────────────────────────────────

class APIResponse(BaseModel, Generic[T]):
    """Uniform API response wrapper."""

    success: bool
    data: Optional[T] = None
    error: Optional[ErrorDetail] = None
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @classmethod
    def ok(cls, data: T) -> "APIResponse[T]":
        return cls(success=True, data=data)

    @classmethod
    def fail(cls, code: str, message: str, detail: Any = None) -> "APIResponse[None]":
        return cls(
            success=False,
            error=ErrorDetail(code=code, message=message, detail=detail),
        )


# ── Upload ────────────────────────────────────────────────────────────────────

class UploadJobResponse(BaseModel):
    """Returned by POST /upload."""

    dataset_id: UUID
    job_id: str
    filename: str
    rows: int
    columns: int
    size_bytes: int
    status: str
    message: str


# ── Job status ────────────────────────────────────────────────────────────────

class JobStatusResponse(BaseModel):
    job_id: str
    state: str                               # PENDING / PROGRESS / SUCCESS / FAILURE
    step: Optional[str] = None               # current pipeline step when PROGRESS
    error: Optional[str] = None


# ── Profile ───────────────────────────────────────────────────────────────────

class ProfileResponse(BaseModel):
    """Returned by GET /profile."""

    dataset_id: UUID
    job_id: str
    rows: int
    columns: int
    profile: dict                            # serialised DatasetProfile


# ── Anomalies ─────────────────────────────────────────────────────────────────

class AnomalyResponse(BaseModel):
    """Returned by GET /anomalies."""

    dataset_id: UUID
    job_id: str
    total_anomalies: int
    critical_count: int
    high_count: int
    anomaly_report: dict                     # serialised AnomalyReport


# ── AI plan ───────────────────────────────────────────────────────────────────

class GenerateAIPlanRequest(BaseModel):
    """Body for POST /generate-ai-plan."""

    job_id: str
    model: str = "anthropic/claude-3.5-sonnet"
    temperature: float = Field(default=0.2, ge=0.0, le=2.0)
    max_tokens: int = Field(default=2048, ge=256, le=8192)


class AIPlanResponse(BaseModel):
    """Returned by POST /generate-ai-plan."""

    dataset_id: UUID
    job_id: str
    model_used: str
    confidence_score: float
    step_count: int
    transformation_order: list[str]
    agent_result: dict                       # serialised AgentResult


# ── Healing ───────────────────────────────────────────────────────────────────

class ApplyHealingRequest(BaseModel):
    """Body for POST /apply-healing."""

    dataset_id: UUID
    # Optionally pass back the AgentResult from /generate-ai-plan
    agent_result: Optional[dict] = None
    # If True and agent_result is provided, uses AI-directed plan
    use_ai_plan: bool = False


class HealingApplyResponse(BaseModel):
    """Returned by POST /apply-healing."""

    dataset_id: UUID
    actions_applied: int
    actions_skipped: int
    rows_before: int
    rows_after: int
    total_corrections: int
    ai_corrections: int
    strategies_applied: list[str]
    validation_warnings: list[str]
    healing_result: dict                    # serialised HealingResult


# ── Quality score ─────────────────────────────────────────────────────────────

class QualityScoreResponse(BaseModel):
    """Returned by GET /quality-score."""

    dataset_id: UUID
    job_id: str
    score_before: float
    score_after: float
    delta: float
    grade_before: str
    grade_after: str
    quality_delta: dict                     # serialised QualityDelta


# ── Health metrics ────────────────────────────────────────────────────────────

class ServiceStatus(BaseModel):
    name: str
    status: str                             # "ok" | "degraded" | "down"
    latency_ms: Optional[float] = None
    detail: Optional[str] = None


class HealthMetricsResponse(BaseModel):
    """Returned by GET /health-metrics."""

    status: str                             # "healthy" | "degraded" | "unhealthy"
    version: str
    uptime_seconds: float
    services: list[ServiceStatus]
    active_workers: int
    cpu_count: int
    checked_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


# ── Download ──────────────────────────────────────────────────────────────────

class DownloadMeta(BaseModel):
    """Returned in headers / pre-flight for GET /download-cleaned."""

    dataset_id: UUID
    filename: str
    size_bytes: int
    rows: int
    columns: int
    content_type: str = "text/csv"
