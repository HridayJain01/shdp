"""
Composite quality scorer — produces a 0–100 score across five dimensions:

  1. Completeness    (missing ratio)
  2. Uniqueness      (duplicate ratio)
  3. Outlier Health  (IQR-based outlier ratio)
  4. Format Validity (semantic/dtype format checks)
  5. Schema Consistency (type-consistency checks)

Public API
----------
compute_score(df, dataset_id, profile=None) → QualityScore
    Returns {final_score, breakdown, improvement_potential} plus legacy pillars.

compute_delta(df_before, df_after, dataset_id, ...) → QualityDelta
    Before/after comparison after healing.
"""
from __future__ import annotations

import uuid
from typing import Any

import pandas as pd

from app.core.config import settings
from app.models.quality import (
    DimensionBreakdown,
    ImprovementSuggestion,
    PillarScore,
    QualityDelta,
    QualityScore,
    ScoringBreakdown,
    score_to_grade,
)
from app.modules.scoring.metrics import (
    duplicate_ratio,
    format_validity_score,
    missing_ratio,
    outlier_ratio_iqr,
    schema_consistency_score,
)

# ─── Human-readable labels ────────────────────────────────────────────────────

_LABELS: dict[str, str] = {
    "completeness":      "Missing values",
    "uniqueness":        "Duplicate rows",
    "outlier_health":    "Outlier values",
    "format_validity":   "Format errors",
    "schema_consistency": "Type inconsistencies",
}

# ─── Actionable improvement text ─────────────────────────────────────────────

def _action_text(dim: str, d: DimensionBreakdown) -> str:
    n_cols = len(d.affected_columns)
    if dim == "completeness":
        return (
            f"Impute or remove {d.issue_count} missing value(s) "
            f"across {n_cols} column(s): {', '.join(d.affected_columns[:3])}"
            + (" …" if n_cols > 3 else "")
        )
    if dim == "uniqueness":
        return f"Deduplicate {d.issue_count} duplicate row(s)"
    if dim == "outlier_health":
        return (
            f"Cap or remove {d.issue_count} outlier value(s) "
            f"in {n_cols} column(s): {', '.join(d.affected_columns[:3])}"
            + (" …" if n_cols > 3 else "")
        )
    if dim == "format_validity":
        return (
            f"Fix {d.issue_count} format error(s) "
            f"in {n_cols} column(s): {', '.join(d.affected_columns[:3])}"
            + (" …" if n_cols > 3 else "")
        )
    if dim == "schema_consistency":
        return (
            f"Resolve type inconsistencies in "
            f"{n_cols} column(s): {', '.join(d.affected_columns[:3])}"
            + (" …" if n_cols > 3 else "")
        )
    return f"Improve {dim}"


# ─── Improvement suggestions ─────────────────────────────────────────────────

def _build_suggestions(
    breakdown: ScoringBreakdown,
    weight_map: dict[str, float],
) -> list[ImprovementSuggestion]:
    """
    For each dimension below 100, compute the points recoverable if it reached
    100. Return top-5 by estimated_gain descending.
    """
    dims: dict[str, DimensionBreakdown] = {
        "completeness":      breakdown.completeness,
        "uniqueness":        breakdown.uniqueness,
        "outlier_health":    breakdown.outlier_health,
        "format_validity":   breakdown.format_validity,
        "schema_consistency": breakdown.schema_consistency,
    }

    suggestions: list[ImprovementSuggestion] = []
    for dim_name, dim in dims.items():
        if dim.score >= 100.0:
            continue
        # Points we'd gain if this dimension were perfect
        gain = round((100.0 - dim.score) * weight_map[dim_name], 2)
        if gain < 0.01:
            continue
        suggestions.append(
            ImprovementSuggestion(
                dimension=dim_name,
                action=_action_text(dim_name, dim),
                estimated_gain=gain,
                affected_columns=dim.affected_columns,
            )
        )

    suggestions.sort(key=lambda s: s.estimated_gain, reverse=True)
    return suggestions[:5]


# ─── Main scorer ─────────────────────────────────────────────────────────────

def compute_score(
    df: pd.DataFrame,
    dataset_id: uuid.UUID,
    profile: Any = None,
) -> QualityScore:
    """
    Score a DataFrame on five quality dimensions and return a structured result:

        {
            "final_score":          float (0–100),
            "breakdown":            ScoringBreakdown,
            "improvement_potential": list[ImprovementSuggestion],
            ...
        }

    Parameters
    ----------
    df:         DataFrame to evaluate.
    dataset_id: UUID for provenance tracking.
    profile:    Optional ``DatasetProfile`` (from the profiling module).
                When supplied, format-validity checks use semantic_type metadata.
    """
    # ── Raw metric collection ─────────────────────────────────────────────────
    miss_ratio,  miss_count,  miss_cols  = missing_ratio(df)
    dup_ratio_v, dup_count,   _          = duplicate_ratio(df)
    out_ratio,   out_count,   out_cols   = outlier_ratio_iqr(df)
    fmt_valid,   fmt_invalid, fmt_cols   = format_validity_score(df, profile)
    sch_valid,   sch_invalid, sch_cols   = schema_consistency_score(df, profile)

    # ── Convert to 0–100 scores ───────────────────────────────────────────────
    s_completeness  = round((1.0 - miss_ratio)  * 100.0, 2)
    s_uniqueness    = round((1.0 - dup_ratio_v) * 100.0, 2)
    s_outlier       = round((1.0 - out_ratio)   * 100.0, 2)
    s_format        = round(fmt_valid            * 100.0, 2)
    s_schema        = round(sch_valid            * 100.0, 2)

    # ── Dimension weights from config ─────────────────────────────────────────
    # Config names are reused but semantics are remapped to the 5 new dimensions:
    #   WEIGHT_COMPLETENESS → missing_ratio (completeness)
    #   WEIGHT_VALIDITY     → format_validity
    #   WEIGHT_UNIQUENESS   → duplicate_ratio (uniqueness)
    #   WEIGHT_CONSISTENCY  → schema_consistency
    #   WEIGHT_TIMELINESS   → outlier_health
    w = settings
    weight_map: dict[str, float] = {
        "completeness":      w.WEIGHT_COMPLETENESS,   # 0.30
        "uniqueness":        w.WEIGHT_UNIQUENESS,      # 0.20
        "outlier_health":    w.WEIGHT_TIMELINESS,      # 0.10
        "format_validity":   w.WEIGHT_VALIDITY,        # 0.25
        "schema_consistency": w.WEIGHT_CONSISTENCY,    # 0.15
    }

    # ── Assemble ScoringBreakdown ─────────────────────────────────────────────
    breakdown = ScoringBreakdown(
        completeness=DimensionBreakdown(
            score=s_completeness,
            ratio=round(miss_ratio, 6),
            issue_count=miss_count,
            affected_columns=miss_cols,
            weight=weight_map["completeness"],
            label=_LABELS["completeness"],
        ),
        uniqueness=DimensionBreakdown(
            score=s_uniqueness,
            ratio=round(dup_ratio_v, 6),
            issue_count=dup_count,
            affected_columns=[],
            weight=weight_map["uniqueness"],
            label=_LABELS["uniqueness"],
        ),
        outlier_health=DimensionBreakdown(
            score=s_outlier,
            ratio=round(out_ratio, 6),
            issue_count=out_count,
            affected_columns=out_cols,
            weight=weight_map["outlier_health"],
            label=_LABELS["outlier_health"],
        ),
        format_validity=DimensionBreakdown(
            score=s_format,
            ratio=round(1.0 - fmt_valid, 6),
            issue_count=fmt_invalid,
            affected_columns=fmt_cols,
            weight=weight_map["format_validity"],
            label=_LABELS["format_validity"],
        ),
        schema_consistency=DimensionBreakdown(
            score=s_schema,
            ratio=round(1.0 - sch_valid, 6),
            issue_count=sch_invalid,
            affected_columns=sch_cols,
            weight=weight_map["schema_consistency"],
            label=_LABELS["schema_consistency"],
        ),
    )

    # ── Weighted final score ──────────────────────────────────────────────────
    final_score = round(
        s_completeness * weight_map["completeness"]
        + s_uniqueness  * weight_map["uniqueness"]
        + s_outlier     * weight_map["outlier_health"]
        + s_format      * weight_map["format_validity"]
        + s_schema      * weight_map["schema_consistency"],
        2,
    )

    # ── Improvement potential ─────────────────────────────────────────────────
    improvement = _build_suggestions(breakdown, weight_map)

    # ── Legacy pillar list (backward compat for existing API consumers) ───────
    pillars = [
        PillarScore(
            name="Completeness",
            weight=weight_map["completeness"],
            raw_score=round(1.0 - miss_ratio, 6),
            weighted_score=round(s_completeness * weight_map["completeness"], 2),
        ),
        PillarScore(
            name="Format Validity",
            weight=weight_map["format_validity"],
            raw_score=round(fmt_valid, 6),
            weighted_score=round(s_format * weight_map["format_validity"], 2),
        ),
        PillarScore(
            name="Uniqueness",
            weight=weight_map["uniqueness"],
            raw_score=round(1.0 - dup_ratio_v, 6),
            weighted_score=round(s_uniqueness * weight_map["uniqueness"], 2),
        ),
        PillarScore(
            name="Schema Consistency",
            weight=weight_map["schema_consistency"],
            raw_score=round(sch_valid, 6),
            weighted_score=round(s_schema * weight_map["schema_consistency"], 2),
        ),
        PillarScore(
            name="Outlier Health",
            weight=weight_map["outlier_health"],
            raw_score=round(1.0 - out_ratio, 6),
            weighted_score=round(s_outlier * weight_map["outlier_health"], 2),
        ),
    ]

    return QualityScore(
        dataset_id=dataset_id,
        total_score=final_score,
        grade=score_to_grade(final_score),
        pillars=pillars,
        breakdown=breakdown,
        improvement_potential=improvement,
    )


# ─── Delta (before / after healing) ──────────────────────────────────────────

def compute_delta(
    df_before: pd.DataFrame,
    df_after: pd.DataFrame,
    dataset_id: uuid.UUID,
    profile_before: Any = None,
    profile_after: Any = None,
) -> QualityDelta:
    before = compute_score(df_before, dataset_id, profile_before)
    after  = compute_score(df_after,  dataset_id, profile_after)
    delta  = round(after.total_score - before.total_score, 2)
    improvement_pct = round((delta / before.total_score) * 100, 2) if before.total_score else 0.0
    return QualityDelta(
        dataset_id=dataset_id,
        before=before,
        after=after,
        delta=delta,
        improvement_pct=improvement_pct,
    )
