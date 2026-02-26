from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field
from uuid import UUID


# ─── Per-dimension detail ──────────────────────────────────────────────────────

class DimensionBreakdown(BaseModel):
    """Scoring detail for a single quality dimension."""

    score: float                          # 0–100  (higher = better)
    ratio: float                          # problem ratio 0–1 (lower = fewer issues)
    issue_count: int                      # absolute number of affected values/rows
    affected_columns: list[str] = Field(default_factory=list)
    weight: float                         # this dimension's weight in final score
    label: str                            # human-readable label


class ScoringBreakdown(BaseModel):
    """Full breakdown across the five quality dimensions."""

    completeness: DimensionBreakdown
    uniqueness: DimensionBreakdown
    outlier_health: DimensionBreakdown
    format_validity: DimensionBreakdown
    schema_consistency: DimensionBreakdown

    def as_dict(self) -> dict:
        return {
            "completeness":      self.completeness.model_dump(),
            "uniqueness":        self.uniqueness.model_dump(),
            "outlier_health":    self.outlier_health.model_dump(),
            "format_validity":   self.format_validity.model_dump(),
            "schema_consistency": self.schema_consistency.model_dump(),
        }


# ─── Improvement suggestions ───────────────────────────────────────────────────

class ImprovementSuggestion(BaseModel):
    """Actionable recommendation for a specific quality dimension."""

    dimension: str
    action: str
    estimated_gain: float                 # final-score points recoverable
    affected_columns: list[str] = Field(default_factory=list)


# ─── Legacy pillar (backward compat) ──────────────────────────────────────────

class PillarScore(BaseModel):
    name: str
    weight: float
    raw_score: float        # 0.0 – 1.0
    weighted_score: float   # raw × weight × 100


# ─── Top-level score object ───────────────────────────────────────────────────

class QualityScore(BaseModel):
    dataset_id: UUID
    total_score: float                    # 0–100
    grade: str                            # A / B / C / D / F
    pillars: list[PillarScore]
    breakdown: Optional[ScoringBreakdown] = None
    improvement_potential: list[ImprovementSuggestion] = Field(default_factory=list)
    scored_at: datetime = Field(default_factory=datetime.utcnow)


class QualityDelta(BaseModel):
    dataset_id: UUID
    before: QualityScore
    after: QualityScore
    delta: float             # positive = improvement
    improvement_pct: float   # (delta / before) * 100


# ─── Grade helper ─────────────────────────────────────────────────────────────

def score_to_grade(score: float) -> str:
    if score >= 90:
        return "A"
    if score >= 80:
        return "B"
    if score >= 65:
        return "C"
    if score >= 50:
        return "D"
    return "F"
