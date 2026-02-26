"""
Pydantic models for the AI agent's structured healing-strategy response.

The exact shape enforced on every LLM response:

    {
        "healing_plan":         [ HealingStep, … ],
        "column_fixes":         [ ColumnFix, … ],
        "value_corrections":    [ ValueCorrection, … ],
        "transformation_order": [ str, … ],
        "confidence_score":     float  (0.0 – 1.0)
    }

All sub-models carry enough information to drive the downstream healing engine
without any additional LLM calls.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator


# ─── Sub-models ───────────────────────────────────────────────────────────────

class HealingStep(BaseModel):
    """One high-level step in the overall healing plan."""

    step_id: str = Field(description="Short identifier, e.g. 'step_01'")
    title: str = Field(description="Human-readable step name")
    strategy: str = Field(
        description="Healing strategy key, e.g. 'median_imputation'"
    )
    target_columns: list[str] = Field(
        default_factory=list,
        description="Columns this step applies to; empty means row-level",
    )
    rationale: str = Field(description="Why this step is needed")
    priority: int = Field(
        ge=1,
        description="Execution priority (1 = apply first)",
    )
    estimated_impact: float = Field(
        ge=0.0, le=1.0,
        description="Expected quality-score improvement fraction (0–1)",
    )
    parameters: dict[str, Any] = Field(
        default_factory=dict,
        description="Strategy-specific parameters passed to the healing engine",
    )

    @field_validator("estimated_impact", mode="before")
    @classmethod
    def clamp_impact(cls, v: Any) -> float:
        return max(0.0, min(1.0, float(v)))


class ColumnFix(BaseModel):
    """Targeted fix instructions for a specific column."""

    column: str = Field(description="Column name")
    detected_issue: str = Field(description="What problem was detected")
    fix_type: str = Field(
        description="Fix category, e.g. 'impute', 'cast', 'normalize'"
    )
    target_dtype: Optional[str] = Field(
        default=None,
        description="Expected dtype after the fix, e.g. 'float64', 'datetime64'",
    )
    parameters: dict[str, Any] = Field(default_factory=dict)
    severity: str = Field(
        default="medium",
        description="critical | high | medium | low",
    )
    expected_null_reduction: Optional[float] = Field(
        default=None,
        ge=0.0, le=1.0,
        description="Fraction of nulls expected to be resolved (0–1)",
    )


class ValueCorrection(BaseModel):
    """A concrete before→after correction at the individual-value level."""

    column: str = Field(description="Column containing the erroneous value")
    original_value: Any = Field(description="Value as it currently exists in the data")
    corrected_value: Any = Field(description="Value after the correction")
    correction_type: str = Field(
        description="Type of correction, e.g. 'format_fix', 'outlier_cap', 'imputation'"
    )
    reason: str = Field(description="Why this specific correction is appropriate")
    row_index: Optional[int] = Field(
        default=None,
        description="Row index if the correction targets a specific row",
    )


# ─── Top-level response schema ────────────────────────────────────────────────

class AgentResponse(BaseModel):
    """
    Complete structured healing strategy returned by the AI agent.

    This model is used as the ``schema_type`` argument to
    ``json_completion()`` — every LLM response is validated against it
    before being passed downstream.
    """

    healing_plan: list[HealingStep] = Field(
        description="Ordered list of healing strategy steps"
    )
    column_fixes: list[ColumnFix] = Field(
        description="Per-column fix instructions"
    )
    value_corrections: list[ValueCorrection] = Field(
        description="Sample individual value corrections (up to 20)"
    )
    transformation_order: list[str] = Field(
        description="Ordered strategy names to apply, e.g. ['deduplication', 'median_imputation']"
    )
    confidence_score: float = Field(
        ge=0.0, le=1.0,
        description="Agent's confidence in this healing strategy (0–1)",
    )

    @field_validator("confidence_score", mode="before")
    @classmethod
    def clamp_confidence(cls, v: Any) -> float:
        return max(0.0, min(1.0, float(v)))

    @model_validator(mode="after")
    def healing_plan_not_empty(self) -> "AgentResponse":
        if not self.healing_plan:
            raise ValueError("healing_plan must contain at least one step")
        return self

    @model_validator(mode="after")
    def transformation_order_not_empty(self) -> "AgentResponse":
        if not self.transformation_order:
            raise ValueError("transformation_order must contain at least one entry")
        return self

    def summary(self) -> dict[str, Any]:
        """Compact summary dict safe to embed in API responses or logs."""
        return {
            "steps":               len(self.healing_plan),
            "columns_targeted":    len(self.column_fixes),
            "value_corrections":   len(self.value_corrections),
            "transformation_order": self.transformation_order,
            "confidence_score":    self.confidence_score,
            "top_strategy":        self.healing_plan[0].strategy if self.healing_plan else None,
        }


# ─── Agent run metadata (wraps the response for provenance) ──────────────────

class AgentResult(BaseModel):
    """
    Full result returned by :func:`app.modules.ai.agent.run_agent`.
    Carries the validated response alongside run metadata.
    """

    dataset_id: UUID
    model_used: str
    response: AgentResponse
    prompt_tokens: Optional[int] = None
    completion_tokens: Optional[int] = None
    latency_ms: Optional[float] = None
    ran_at: datetime = Field(default_factory=datetime.utcnow)
