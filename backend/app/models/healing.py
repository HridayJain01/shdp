"""Data models for the healing layer.

Hierarchy
─────────
HealingPlan
  └─ HealingAction × N

TransformationLog
  └─ TransformationEntry × N   (one per healer × column/operation)

HealingResult   (returned by the engine / executor)
  └─ transformation_log: TransformationLog
"""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field
from uuid import UUID


# ── Strategy enum ─────────────────────────────────────────────────────────────

class HealingStrategy(str, Enum):
    # Missing values
    MEAN_IMPUTATION      = "mean_imputation"
    MEDIAN_IMPUTATION    = "median_imputation"
    MODE_IMPUTATION      = "mode_imputation"
    CONSTANT_IMPUTATION  = "constant_imputation"
    INTERPOLATION        = "interpolation"
    FORWARD_FILL         = "forward_fill"
    BACKWARD_FILL        = "backward_fill"
    # Row / column removal
    DROP_ROWS            = "drop_rows"
    DROP_COLUMN          = "drop_column"
    # Outliers
    ZSCORE_CLAMP         = "zscore_clamp"
    IQR_CLAMP            = "iqr_clamp"
    PERCENTILE_CLAMP     = "percentile_clamp"
    # Duplicates
    DEDUPLICATION        = "deduplication"
    # Type coercion
    TYPE_CAST            = "type_cast"
    # Format fixing
    FORMAT_STANDARDIZE   = "format_standardize"
    # Category normalisation
    CATEGORY_NORMALIZE   = "category_normalize"
    # Numeric scaling
    NORMALIZE            = "normalize"


# ── Plan models ───────────────────────────────────────────────────────────────

class HealingAction(BaseModel):
    action_id: str
    column: str | None = None
    strategy: HealingStrategy
    parameters: dict = Field(default_factory=dict)
    rationale: str
    priority: int           # lower = apply first
    estimated_impact: float # expected quality-score delta


class HealingPlan(BaseModel):
    dataset_id: UUID
    llm_model: str
    actions: list[HealingAction]
    overall_rationale: str


# ── Transformation log models ─────────────────────────────────────────────────

class TransformationEntry(BaseModel):
    """A single healer's record of what it changed."""
    strategy_name: str                          # e.g. "MissingValueHealer"
    operation: str                              # e.g. "median_imputation"
    column: str | None = None                   # None for row-level ops
    corrections: int                            # cells / rows changed
    before_sample: list[Any] = Field(default_factory=list)  # ≤5 values before
    after_sample: list[Any] = Field(default_factory=list)   # ≤5 values after
    detail: str = ""                            # free-text note
    # Provenance: "rule" = local engine default; "ai" = directed by AgentResponse
    source: str = "rule"
    # Validation notes attached after transformation validation runs
    validation_warnings: list[str] = Field(default_factory=list)
    applied_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class TransformationLog(BaseModel):
    """Aggregated log produced by the HealingEngine for a single run."""
    entries: list[TransformationEntry] = Field(default_factory=list)
    total_corrections: int = 0
    strategies_applied: list[str] = Field(default_factory=list)

    # AI-sourced entries tracked separately for reporting / auditing
    ai_entries: list[TransformationEntry] = Field(default_factory=list)
    ai_total_corrections: int = 0

    def append(self, entry: TransformationEntry) -> None:
        self.entries.append(entry)
        self.total_corrections += entry.corrections
        name = entry.strategy_name
        if name not in self.strategies_applied:
            self.strategies_applied.append(name)
        if entry.source == "ai":
            self.ai_entries.append(entry)
            self.ai_total_corrections += entry.corrections

    def ai_summary(self) -> dict:
        """Summary scoped to AI-directed transformations only."""
        return {
            "ai_total_corrections": self.ai_total_corrections,
            "ai_entry_count": len(self.ai_entries),
            "ai_strategies": list({
                e.strategy_name for e in self.ai_entries
                if e.operation != "ERROR"
            }),
            "ai_errors": sum(
                1 for e in self.ai_entries if e.operation == "ERROR"
            ),
        }

    def summary(self) -> dict:
        return {
            "total_corrections": self.total_corrections,
            "strategies_applied": self.strategies_applied,
            "entry_count": len(self.entries),
            "ai": self.ai_summary(),
        }


# ── Result model ──────────────────────────────────────────────────────────────

class HealingResult(BaseModel):
    dataset_id: UUID
    actions_applied: int
    actions_skipped: int
    rows_modified: int
    healed_dataset_path: str                    # S3 key (filled post-upload)
    transformation_log: TransformationLog = Field(default_factory=TransformationLog)
    # Kept for backward compatibility with existing pipeline serialisation
    execution_log: list[dict] = Field(default_factory=list)
    # AI-directed modifications separated for auditing
    ai_execution_log: list[dict] = Field(default_factory=list)
    # Validation warnings produced by post-transformation checks
    validation_warnings: list[str] = Field(default_factory=list)
