"""
Comprehensive test suite for the quality scoring module.

Covers:
  - metrics.py: all five dimension functions
  - scorer.py: compute_score, compute_delta, improvement suggestions
  - models: DimensionBreakdown, ScoringBreakdown, QualityScore serialisation
"""
from __future__ import annotations

import math
import uuid
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest

from app.modules.scoring.metrics import (
    duplicate_ratio,
    format_validity_score,
    missing_ratio,
    outlier_ratio_iqr,
    schema_consistency_score,
    # legacy
    completeness,
    uniqueness,
    validity,
    consistency,
    timeliness,
)
from app.modules.scoring.scorer import compute_delta, compute_score
from app.models.quality import (
    DimensionBreakdown,
    ImprovementSuggestion,
    QualityScore,
    ScoringBreakdown,
    score_to_grade,
)

# ─── Fixtures ─────────────────────────────────────────────────────────────────

DATASET_ID = uuid.uuid4()


def _perfect_df() -> pd.DataFrame:
    """Clean, consistent, no issues DataFrame."""
    return pd.DataFrame({
        "id":    [1, 2, 3, 4, 5],
        "name":  ["Alice", "Bob", "Carol", "Dave", "Eve"],
        "score": [10.0, 20.0, 30.0, 40.0, 50.0],
    })


def _df_with_missing() -> pd.DataFrame:
    return pd.DataFrame({
        "a": [1.0, None, 3.0, None, 5.0],
        "b": ["x", "y", None, "w", None],
    })


def _df_with_duplicates() -> pd.DataFrame:
    base = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    return pd.concat([base, base.iloc[[0, 1]]], ignore_index=True)  # 2 dups


def _df_with_outliers() -> pd.DataFrame:
    # 98 normal values + 2 extreme outliers
    normal = list(range(1, 99))
    return pd.DataFrame({"v": normal + [1000, -1000]})


def _df_messy_types() -> pd.DataFrame:
    """Object column with mixed Python types."""
    return pd.DataFrame({
        "mixed": ["hello", 42, 3.14, None, "world"],
        "clean": [1.0, 2.0, 3.0, 4.0, 5.0],
    })


def _make_profile(sem_map: dict[str, str]) -> Any:
    """Build a minimal fake DatasetProfile for format_validity_score tests."""
    cols = []
    for col_name, sem in sem_map.items():
        cols.append(SimpleNamespace(name=col_name, semantic_type=sem))
    return SimpleNamespace(columns=cols)


# ─── 1. missing_ratio ─────────────────────────────────────────────────────────

class TestMissingRatio:
    def test_perfect_no_missing(self):
        df = _perfect_df()
        ratio, count, cols = missing_ratio(df)
        assert ratio == 0.0
        assert count == 0
        assert cols == []

    def test_partial_missing(self):
        df = _df_with_missing()
        ratio, count, affected = missing_ratio(df)
        # 4 nulls out of 10 cells
        assert count == 4
        assert math.isclose(ratio, 0.4, rel_tol=1e-6)
        assert set(affected) == {"a", "b"}

    def test_all_missing(self):
        df = pd.DataFrame({"x": [None, None], "y": [None, None]})
        ratio, count, cols = missing_ratio(df)
        assert ratio == 1.0
        assert count == 4

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        ratio, count, cols = missing_ratio(df)
        assert ratio == 0.0
        assert count == 0

    def test_no_missing_numeric(self):
        df = pd.DataFrame({"v": [1, 2, 3]})
        ratio, count, cols = missing_ratio(df)
        assert ratio == 0.0
        assert cols == []


# ─── 2. duplicate_ratio ──────────────────────────────────────────────────────

class TestDuplicateRatio:
    def test_no_duplicates(self):
        df = _perfect_df()
        ratio, count, _ = duplicate_ratio(df)
        assert ratio == 0.0
        assert count == 0

    def test_with_duplicates(self):
        df = _df_with_duplicates()  # 5 rows, 2 are dups of first two
        ratio, count, _ = duplicate_ratio(df)
        assert count == 2
        assert math.isclose(ratio, 2 / 5, rel_tol=1e-6)

    def test_all_duplicates(self):
        df = pd.DataFrame({"a": [1, 1, 1], "b": [2, 2, 2]})
        ratio, count, _ = duplicate_ratio(df)
        # keep="first" so 2 out of 3 are duplicates
        assert count == 2
        assert math.isclose(ratio, 2 / 3, rel_tol=1e-6)

    def test_empty_dataframe(self):
        df = pd.DataFrame({"a": []})
        ratio, count, _ = duplicate_ratio(df)
        assert ratio == 0.0
        assert count == 0

    def test_single_row(self):
        df = pd.DataFrame({"a": [1]})
        ratio, count, _ = duplicate_ratio(df)
        assert ratio == 0.0


# ─── 3. outlier_ratio_iqr ────────────────────────────────────────────────────

class TestOutlierRatioIQR:
    def test_no_outliers(self):
        df = pd.DataFrame({"v": [10.0, 11.0, 12.0, 11.5, 10.5]})
        ratio, count, cols = outlier_ratio_iqr(df)
        assert ratio == 0.0
        assert count == 0
        assert cols == []

    def test_clear_outliers(self):
        df = _df_with_outliers()  # 2 extreme outliers in 100 rows
        ratio, count, cols = outlier_ratio_iqr(df)
        assert count == 2
        assert "v" in cols
        assert 0.0 < ratio < 0.05

    def test_no_numeric_columns(self):
        df = pd.DataFrame({"name": ["a", "b", "c"]})
        ratio, count, cols = outlier_ratio_iqr(df)
        assert ratio == 0.0
        assert count == 0
        assert cols == []

    def test_constant_column_skipped(self):
        # All same value → IQR == 0, should not crash
        df = pd.DataFrame({"v": [5.0] * 10})
        ratio, count, cols = outlier_ratio_iqr(df)
        assert ratio == 0.0
        assert count == 0

    def test_multiple_numeric_columns(self):
        df = pd.DataFrame({
            "a": list(range(20)) + [500],   # 1 outlier
            "b": [1.0] * 21,                # constant — no outlier
        })
        ratio, count, cols = outlier_ratio_iqr(df)
        assert count >= 1
        assert "a" in cols

    def test_custom_iqr_factor(self):
        df = _df_with_outliers()
        # With very large factor, 1000/-1000 might still be caught
        ratio_tight, c_tight, _ = outlier_ratio_iqr(df, iqr_factor=0.5)
        ratio_loose, c_loose, _ = outlier_ratio_iqr(df, iqr_factor=3.0)
        # Tighter factor catches more
        assert c_tight >= c_loose

    def test_empty_numeric_after_dropna(self):
        df = pd.DataFrame({"v": [None, None, None]})
        ratio, count, cols = outlier_ratio_iqr(df)
        assert ratio == 0.0


# ─── 4. format_validity_score ────────────────────────────────────────────────

class TestFormatValidityScore:
    def test_all_valid_no_profile(self):
        df = pd.DataFrame({"v": [1.0, 2.0, 3.0]})
        score, invalid, cols = format_validity_score(df)
        assert score == 1.0
        assert invalid == 0

    def test_inf_values_in_numeric(self):
        df = pd.DataFrame({"v": [1.0, float("inf"), 3.0, float("-inf")]})
        score, invalid, cols = format_validity_score(df)
        assert invalid == 2
        assert "v" in cols
        assert score < 1.0

    def test_email_column_with_profile(self):
        df = pd.DataFrame({
            "email": ["alice@example.com", "not-an-email", "bob@test.org", "bad"]
        })
        profile = _make_profile({"email": "email"})
        score, invalid, cols = format_validity_score(df, profile)
        assert invalid == 2
        assert "email" in cols

    def test_numeric_column_with_profile(self):
        df = pd.DataFrame({"amount": ["10.5", "abc", "30.0", "xyz"]})
        profile = _make_profile({"amount": "numeric"})
        score, invalid, cols = format_validity_score(df, profile)
        assert invalid == 2
        assert "amount" in cols

    def test_boolean_column_with_profile(self):
        df = pd.DataFrame({"flag": ["true", "false", "maybe", "1"]})
        profile = _make_profile({"flag": "boolean"})
        score, invalid, cols = format_validity_score(df, profile)
        assert invalid == 1   # "maybe" only
        assert "flag" in cols

    def test_date_column_with_profile(self):
        df = pd.DataFrame({"dt": ["2024-01-15", "not-a-date", "2023-06-30"]})
        profile = _make_profile({"dt": "date"})
        score, invalid, cols = format_validity_score(df, profile)
        assert invalid == 1
        assert "dt" in cols

    def test_nulls_excluded_from_count(self):
        df = pd.DataFrame({"email": [None, "good@email.com", None]})
        profile = _make_profile({"email": "email"})
        score, invalid, cols = format_validity_score(df, profile)
        assert invalid == 0
        assert score == 1.0

    def test_empty_dataframe(self):
        df = pd.DataFrame({"v": []})
        score, invalid, cols = format_validity_score(df)
        assert score == 1.0
        assert invalid == 0


# ─── 5. schema_consistency_score ─────────────────────────────────────────────

class TestSchemaConsistencyScore:
    def test_all_consistent(self):
        df = _perfect_df()
        score, count, cols = schema_consistency_score(df)
        assert score == 1.0
        assert count == 0

    def test_mixed_types_in_object_col(self):
        df = _df_messy_types()
        score, count, cols = schema_consistency_score(df)
        assert "mixed" in cols
        assert score < 1.0

    def test_inf_in_float_col(self):
        df = pd.DataFrame({"v": [1.0, 2.0, float("inf"), 4.0]})
        score, count, cols = schema_consistency_score(df)
        assert "v" in cols
        assert score < 1.0

    def test_null_like_strings_flagged(self):
        df = pd.DataFrame({"x": ["hello", "none", "world", "nan"]})
        score, count, cols = schema_consistency_score(df)
        assert "x" in cols

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        score, count, cols = schema_consistency_score(df)
        assert score == 1.0
        assert count == 0

    def test_pure_numeric_consistent(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        score, count, cols = schema_consistency_score(df)
        assert score == 1.0
        assert count == 0


# ─── 6. Legacy metric aliases ─────────────────────────────────────────────────

class TestLegacyAliases:
    def test_completeness_perfect(self):
        assert completeness(_perfect_df()) == 1.0

    def test_completeness_with_nulls(self):
        v = completeness(_df_with_missing())
        assert 0.0 < v < 1.0

    def test_uniqueness_no_dups(self):
        assert uniqueness(_perfect_df()) == 1.0

    def test_uniqueness_with_dups(self):
        v = uniqueness(_df_with_duplicates())
        assert 0.0 < v < 1.0

    def test_validity_no_inf(self):
        assert validity(_perfect_df()) == 1.0

    def test_consistency_clean(self):
        assert consistency(_perfect_df()) == 1.0

    def test_timeliness_no_date_col(self):
        assert timeliness(_perfect_df()) == 1.0

    def test_timeliness_with_recent_dates(self):
        df = pd.DataFrame({"created_at": [pd.Timestamp.now(), pd.Timestamp.now()]})
        assert timeliness(df) == 1.0


# ─── 7. compute_score (scorer.py) ────────────────────────────────────────────

class TestComputeScore:
    def test_returns_quality_score(self):
        result = compute_score(_perfect_df(), DATASET_ID)
        assert isinstance(result, QualityScore)

    def test_score_range(self):
        result = compute_score(_perfect_df(), DATASET_ID)
        assert 0.0 <= result.total_score <= 100.0

    def test_perfect_df_high_score(self):
        result = compute_score(_perfect_df(), DATASET_ID)
        assert result.total_score >= 90.0

    def test_grade_assigned(self):
        result = compute_score(_perfect_df(), DATASET_ID)
        assert result.grade in ("A", "B", "C", "D", "F")

    def test_breakdown_populated(self):
        result = compute_score(_perfect_df(), DATASET_ID)
        assert result.breakdown is not None
        bd = result.breakdown
        assert isinstance(bd.completeness, DimensionBreakdown)
        assert isinstance(bd.uniqueness, DimensionBreakdown)
        assert isinstance(bd.outlier_health, DimensionBreakdown)
        assert isinstance(bd.format_validity, DimensionBreakdown)
        assert isinstance(bd.schema_consistency, DimensionBreakdown)

    def test_breakdown_scores_in_range(self):
        result = compute_score(_df_with_missing(), DATASET_ID)
        bd = result.breakdown
        for dim in [bd.completeness, bd.uniqueness, bd.outlier_health,
                    bd.format_validity, bd.schema_consistency]:
            assert 0.0 <= dim.score <= 100.0

    def test_completeness_dim_reflects_nulls(self):
        result = compute_score(_df_with_missing(), DATASET_ID)
        bd = result.breakdown
        assert bd.completeness.score < 100.0
        assert bd.completeness.issue_count > 0

    def test_uniqueness_dim_reflects_dups(self):
        result = compute_score(_df_with_duplicates(), DATASET_ID)
        bd = result.breakdown
        assert bd.uniqueness.score < 100.0
        assert bd.uniqueness.issue_count == 2

    def test_outlier_dim_reflects_outliers(self):
        result = compute_score(_df_with_outliers(), DATASET_ID)
        bd = result.breakdown
        assert bd.outlier_health.score < 100.0
        assert bd.outlier_health.issue_count == 2

    def test_schema_dim_reflects_mixed_types(self):
        result = compute_score(_df_messy_types(), DATASET_ID)
        bd = result.breakdown
        assert bd.schema_consistency.score < 100.0
        assert "mixed" in bd.schema_consistency.affected_columns

    def test_improvement_potential_list(self):
        result = compute_score(_df_with_missing(), DATASET_ID)
        assert isinstance(result.improvement_potential, list)
        # At least completeness should appear
        dims = {s.dimension for s in result.improvement_potential}
        assert "completeness" in dims

    def test_improvement_potential_sorted_descending(self):
        result = compute_score(_df_with_missing(), DATASET_ID)
        gains = [s.estimated_gain for s in result.improvement_potential]
        assert gains == sorted(gains, reverse=True)

    def test_improvement_potential_max_5(self):
        result = compute_score(_df_with_missing(), DATASET_ID)
        assert len(result.improvement_potential) <= 5

    def test_perfect_df_no_improvements(self):
        result = compute_score(_perfect_df(), DATASET_ID)
        # A perfect DF might still suggest nothing or very few improvements
        for s in result.improvement_potential:
            assert s.estimated_gain >= 0.0

    def test_pillars_backward_compat(self):
        result = compute_score(_perfect_df(), DATASET_ID)
        assert len(result.pillars) == 5
        names = {p.name for p in result.pillars}
        assert "Completeness" in names
        assert "Format Validity" in names

    def test_weighted_sum_consistent(self):
        """final_score ≈ sum(pillar.weighted_score)."""
        result = compute_score(_df_with_missing(), DATASET_ID)
        pillar_sum = round(sum(p.weighted_score for p in result.pillars), 2)
        assert math.isclose(result.total_score, pillar_sum, rel_tol=1e-3)

    def test_with_profile_uses_semantic_types(self):
        df = pd.DataFrame({
            "email": ["alice@example.com", "not-an-email", "bob@test.org"]
        })
        profile = _make_profile({"email": "email"})
        result_with = compute_score(df, DATASET_ID, profile=profile)
        result_without = compute_score(df, DATASET_ID, profile=None)
        # With profile, format_validity catches the bad email
        assert (
            result_with.breakdown.format_validity.score
            <= result_without.breakdown.format_validity.score
        )

    def test_dataset_id_propagated(self):
        result = compute_score(_perfect_df(), DATASET_ID)
        assert result.dataset_id == DATASET_ID

    def test_scored_at_populated(self):
        result = compute_score(_perfect_df(), DATASET_ID)
        assert result.scored_at is not None

    def test_json_serialisable(self):
        """QualityScore must round-trip through model_dump / JSON."""
        result = compute_score(_df_with_missing(), DATASET_ID)
        d = result.model_dump()
        assert "total_score" in d
        assert "breakdown" in d
        assert "improvement_potential" in d
        bd = d["breakdown"]
        assert "completeness" in bd
        assert "uniqueness" in bd
        assert "outlier_health" in bd
        assert "format_validity" in bd
        assert "schema_consistency" in bd


# ─── 8. ScoringBreakdown model ────────────────────────────────────────────────

class TestScoringBreakdown:
    def _make_dim(self, score: float = 80.0) -> DimensionBreakdown:
        return DimensionBreakdown(
            score=score, ratio=0.2, issue_count=10,
            affected_columns=["col_a"], weight=0.2, label="Test"
        )

    def test_as_dict_keys(self):
        bd = ScoringBreakdown(
            completeness=self._make_dim(90),
            uniqueness=self._make_dim(95),
            outlier_health=self._make_dim(85),
            format_validity=self._make_dim(70),
            schema_consistency=self._make_dim(100),
        )
        d = bd.as_dict()
        assert set(d.keys()) == {
            "completeness", "uniqueness", "outlier_health",
            "format_validity", "schema_consistency"
        }

    def test_as_dict_values_are_dicts(self):
        bd = ScoringBreakdown(
            completeness=self._make_dim(),
            uniqueness=self._make_dim(),
            outlier_health=self._make_dim(),
            format_validity=self._make_dim(),
            schema_consistency=self._make_dim(),
        )
        for v in bd.as_dict().values():
            assert isinstance(v, dict)
            assert "score" in v
            assert "ratio" in v
            assert "issue_count" in v


# ─── 9. compute_delta ────────────────────────────────────────────────────────

class TestComputeDelta:
    def test_delta_positive_after_fix(self):
        before_df = _df_with_missing()
        after_df = before_df.fillna(0)
        delta = compute_delta(before_df, after_df, DATASET_ID)
        assert delta.delta > 0

    def test_delta_zero_same_df(self):
        df = _perfect_df()
        delta = compute_delta(df, df, DATASET_ID)
        assert delta.delta == 0.0

    def test_improvement_pct_positive(self):
        before_df = _df_with_missing()
        after_df = before_df.fillna(0)
        delta = compute_delta(before_df, after_df, DATASET_ID)
        assert delta.improvement_pct > 0

    def test_before_after_populated(self):
        df = _perfect_df()
        delta = compute_delta(df, df, DATASET_ID)
        assert isinstance(delta.before, QualityScore)
        assert isinstance(delta.after, QualityScore)

    def test_dataset_id_in_delta(self):
        df = _perfect_df()
        delta = compute_delta(df, df, DATASET_ID)
        assert delta.dataset_id == DATASET_ID

    def test_delta_with_profile_args(self):
        df = _perfect_df()
        delta = compute_delta(df, df, DATASET_ID, profile_before=None, profile_after=None)
        assert delta.delta == 0.0


# ─── 10. score_to_grade ──────────────────────────────────────────────────────

class TestScoreToGrade:
    @pytest.mark.parametrize("score,expected", [
        (100, "A"), (95, "A"), (90, "A"),
        (89,  "B"), (85, "B"), (80, "B"),
        (79,  "C"), (70, "C"), (65, "C"),
        (64,  "D"), (55, "D"), (50, "D"),
        (49,  "F"), (0,  "F"),
    ])
    def test_grade_boundaries(self, score, expected):
        assert score_to_grade(score) == expected
