"""
AI agent that takes a DatasetProfile + AnomalyReport and returns a fully
validated, structured healing strategy.

Public API
----------
run_agent(profile, anomaly_report, dataset_id, *, model=None, config=None)
    → AgentResult

The agent:
  1. Serialises the profile and anomaly report to compact JSON.
  2. Builds a structured SYSTEM + USER message pair.
  3. Calls OpenRouter with ``response_format: json_object`` (via json_completion).
  4. Validates the response against the ``AgentResponse`` Pydantic schema —
     raises ``OpenRouterValidationError`` if the model returned a non-conforming
     structure.
  5. Returns an ``AgentResult`` with the validated response and run metadata
     (model used, token counts, latency).
"""
from __future__ import annotations

import json
import time
import uuid
from typing import Any

from app.core.config import settings
from app.core.logging import get_logger
from app.models.agent import AgentResponse, AgentResult
from app.models.anomaly import AnomalyReport
from app.models.profile import DatasetProfile
from app.modules.ai.openrouter_client import (
    ClientConfig,
    OpenRouterClient,
    OpenRouterError,
    OpenRouterValidationError,
)
from app.modules.ai.prompts import AGENT_SYSTEM, AGENT_USER

logger = get_logger(__name__)

# Maximum number of columns / anomalies to include in the prompt to avoid
# exceeding token limits for very wide datasets.
_MAX_PROFILE_COLUMNS: int = 40
_MAX_ANOMALIES: int = 25
_MAX_VALUE_SAMPLES: int = 5


# ─── Serialisation helpers ────────────────────────────────────────────────────

def _serialise_profile(profile: DatasetProfile) -> str:
    """
    Convert a DatasetProfile to a compact JSON string safe for prompt injection.
    Caps at ``_MAX_PROFILE_COLUMNS`` columns; truncates sample values.
    """
    cols = []
    for cp in profile.columns[:_MAX_PROFILE_COLUMNS]:
        entry: dict[str, Any] = {
            "name":        cp.name,
            "dtype":       cp.dtype,
            "semantic":    cp.semantic_type,
            "null_pct":    round(cp.null_pct, 2),
            "unique_pct":  round(cp.unique_pct, 2),
        }
        if cp.numeric_stats is not None:
            ns = cp.numeric_stats
            entry["numeric"] = {
                "mean":   round(ns.mean, 4) if ns.mean is not None else None,
                "std":    round(ns.std, 4)  if ns.std  is not None else None,
                "min":    ns.min,
                "max":    ns.max,
            }
        if cp.top_values:
            entry["top_values"] = [
                {"value": tv.value, "count": tv.count}
                for tv in cp.top_values[:_MAX_VALUE_SAMPLES]
            ]
        cols.append(entry)

    summary: dict[str, Any] = {
        "row_count":      profile.row_count,
        "column_count":   profile.column_count,
        "duplicate_rows": profile.duplicate_rows,
        "memory_mb":      round(profile.memory_mb, 3),
        "columns":        cols,
    }
    if len(profile.columns) > _MAX_PROFILE_COLUMNS:
        summary["columns_truncated"] = len(profile.columns) - _MAX_PROFILE_COLUMNS

    return json.dumps(summary, default=str, indent=2)


def _serialise_anomalies(report: AnomalyReport) -> str:
    """
    Convert an AnomalyReport to compact JSON safe for prompt injection.
    Caps at ``_MAX_ANOMALIES`` anomalies.
    """
    anomalies = []
    for a in report.anomalies[:_MAX_ANOMALIES]:
        anomalies.append({
            "id":            a.id,
            "column":        a.column,
            "type":          a.anomaly_type.value,
            "severity":      a.severity.value,
            "affected_rows": a.affected_rows,
            "affected_rate": round(a.affected_rate, 4),
            "description":   a.description,
        })

    summary: dict[str, Any] = {
        "total_anomalies": report.total_anomalies,
        "critical":        report.critical_count,
        "high":            report.high_count,
        "medium":          report.medium_count,
        "low":             report.low_count,
        "anomalies":       anomalies,
    }
    if len(report.anomalies) > _MAX_ANOMALIES:
        summary["anomalies_truncated"] = len(report.anomalies) - _MAX_ANOMALIES

    return json.dumps(summary, default=str, indent=2)


# ─── Agent entry point ────────────────────────────────────────────────────────

async def run_agent(
    profile: DatasetProfile,
    anomaly_report: AnomalyReport,
    dataset_id: uuid.UUID,
    *,
    model: str | None = None,
    temperature: float = 0.1,
    max_tokens: int = 6000,
    config: ClientConfig | None = None,
) -> AgentResult:
    """
    Run the data-quality AI agent and return a validated ``AgentResult``.

    Parameters
    ----------
    profile:       Profiling module output for the dataset.
    anomaly_report: Anomaly detector output for the dataset.
    dataset_id:    UUID for provenance tracking.
    model:         OpenRouter model identifier; falls back to settings.
    temperature:   Sampling temperature (keep low for structured output).
    max_tokens:    Maximum tokens in the response.
    config:        Optional :class:`ClientConfig` override.

    Returns
    -------
    AgentResult with ``response: AgentResponse`` validated against the schema.

    Raises
    ------
    OpenRouterAuthError        – missing / invalid API key
    OpenRouterRateLimitError   – 429 after all retries
    OpenRouterHTTPError        – non-retryable HTTP error
    OpenRouterTimeoutError     – timeout after all retries
    OpenRouterJSONError        – model returned non-JSON content
    OpenRouterValidationError  – JSON did not match AgentResponse schema
    """
    request_id = str(dataset_id)
    resolved_model = model or settings.OPENROUTER_MODEL

    logger.info(
        "agent_run_start",
        dataset_id=request_id,
        model=resolved_model,
        rows=profile.row_count,
        columns=profile.column_count,
        anomalies=anomaly_report.total_anomalies,
    )

    profile_json = _serialise_profile(profile)
    anomaly_json = _serialise_anomalies(anomaly_report)

    messages = [
        {"role": "system", "content": AGENT_SYSTEM},
        {
            "role": "user",
            "content": AGENT_USER.format(
                profile_json=profile_json,
                anomaly_json=anomaly_json,
            ),
        },
    ]

    t_start = time.monotonic()

    async with OpenRouterClient(config) as client:
        # json_completion enforces response_format=json_object AND validates
        # the response against AgentResponse via Pydantic model_validate.
        raw_dict = await client.json_completion(
            messages,
            model=resolved_model,
            temperature=temperature,
            max_tokens=max_tokens,
            schema_type=AgentResponse,
            request_id=request_id,
        )

    latency_ms = round((time.monotonic() - t_start) * 1000, 1)

    # Re-validate to get a typed AgentResponse instance (raw_dict is already
    # a model_dump() of the validated model, so this never raises).
    response = AgentResponse.model_validate(raw_dict)

    logger.info(
        "agent_run_complete",
        dataset_id=request_id,
        model=resolved_model,
        steps=len(response.healing_plan),
        column_fixes=len(response.column_fixes),
        confidence=response.confidence_score,
        latency_ms=latency_ms,
    )

    return AgentResult(
        dataset_id=dataset_id,
        model_used=resolved_model,
        response=response,
        latency_ms=latency_ms,
    )
