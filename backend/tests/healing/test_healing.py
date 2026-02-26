"""Tests for the modular healing engine.

Covers:
  • MissingValueHealer   — all imputation strategies + auto-mode
  • TypeMismatchHealer   — numeric / datetime / bool coercion + auto-mode
  • DuplicateResolver    — exact dedup, subset, keep options
  • OutlierCapper        — IQR, Z-score, percentile fences
  • CategoryNormalizer   — strip, case, alias_map, unknown_token
  • FormatCorrector      — email, phone, date, currency, percentage, zipcode
  • HealingEngine        — sequential pipeline, TransformationLog
  • TransformationLog    — append, total_corrections, summary
  • execute_plan()       — public API / backward compat
"""
from __future__ import annotations

import uuid
from typing import Any

import numpy as np
import pandas as pd
import pytest

from app.models.healing import (
    HealingAction,
    HealingPlan,
    HealingResult,
    HealingStrategy,
    TransformationEntry,
    TransformationLog,
)
from app.modules.healing.engine import HealingEngine
from app.modules.healing.executor import execute_plan
from app.modules.healing.strategies import (
    CategoryNormalizer,
    DuplicateResolver,
    FormatCorrector,
    HealingContext,
    HealerResult,
    MissingValueHealer,
    OutlierCapper,
    TypeMismatchHealer,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

def _plan(*actions: HealingAction) -> HealingPlan:
    return HealingPlan(
        dataset_id=uuid.uuid4(),
        llm_model="test",
        actions=list(actions),
        overall_rationale="test plan",
    )


def _action(
    strategy: HealingStrategy,
    column: str | None = None,
    params: dict | None = None,
    priority: int = 1,
) -> HealingAction:
    return HealingAction(
        action_id=str(uuid.uuid4()),
        column=column,
        strategy=strategy,
        parameters=params or {},
        rationale="test",
        priority=priority,
        estimated_impact=0.1,
    )


def _ctx(plan: HealingPlan, **kwargs) -> HealingContext:
    return HealingContext(plan=plan, **kwargs)


def _df(**kwargs) -> pd.DataFrame:
    return pd.DataFrame(kwargs)


# ─────────────────────────────────────────────────────────────────────────────
# 1. TransformationLog
# ─────────────────────────────────────────────────────────────────────────────

class TestTransformationLog:
    def test_append_accumulates(self):
        log = TransformationLog()
        log.append(TransformationEntry(strategy_name="A", operation="op1", corrections=3))
        log.append(TransformationEntry(strategy_name="B", operation="op2", corrections=7))
        assert log.total_corrections == 10
        assert len(log.entries) == 2

    def test_strategies_applied_unique(self):
        log = TransformationLog()
        log.append(TransformationEntry(strategy_name="A", operation="x", corrections=1))
        log.append(TransformationEntry(strategy_name="A", operation="y", corrections=2))
        assert log.strategies_applied == ["A"]

    def test_summary_keys(self):
        log = TransformationLog()
        log.append(TransformationEntry(strategy_name="A", operation="x", corrections=5))
        s = log.summary()
        assert s["total_corrections"] == 5
        assert "strategies_applied" in s
        assert "entry_count" in s


# ─────────────────────────────────────────────────────────────────────────────
# 2. MissingValueHealer
# ─────────────────────────────────────────────────────────────────────────────

class TestMissingValueHealer:
    def test_mean_imputation(self):
        df = _df(x=[1.0, None, 3.0])
        plan = _plan(_action(HealingStrategy.MEAN_IMPUTATION, "x"))
        ctx = _ctx(plan, config={"auto_impute": False})
        result = MissingValueHealer().apply(df, ctx)
        assert result.dataframe["x"].isna().sum() == 0
        assert result.dataframe["x"].iloc[1] == pytest.approx(2.0)

    def test_median_imputation(self):
        df = _df(x=[1.0, None, 3.0, 5.0])
        plan = _plan(_action(HealingStrategy.MEDIAN_IMPUTATION, "x"))
        ctx = _ctx(plan, config={"auto_impute": False})
        result = MissingValueHealer().apply(df, ctx)
        assert result.dataframe["x"].iloc[1] == pytest.approx(3.0)

    def test_mode_imputation_string(self):
        df = _df(cat=["a", "a", None, "b"])
        plan = _plan(_action(HealingStrategy.MODE_IMPUTATION, "cat"))
        ctx = _ctx(plan, config={"auto_impute": False})
        result = MissingValueHealer().apply(df, ctx)
        assert result.dataframe["cat"].iloc[2] == "a"

    def test_constant_imputation(self):
        df = _df(x=[1.0, None, None])
        plan = _plan(_action(HealingStrategy.CONSTANT_IMPUTATION, "x", params={"value": -1}))
        ctx = _ctx(plan, config={"auto_impute": False})
        result = MissingValueHealer().apply(df, ctx)
        assert (result.dataframe["x"] == -1).sum() == 2

    def test_forward_fill(self):
        df = _df(x=[1.0, None, None, 4.0])
        plan = _plan(_action(HealingStrategy.FORWARD_FILL, "x"))
        ctx = _ctx(plan, config={"auto_impute": False})
        result = MissingValueHealer().apply(df, ctx)
        assert result.dataframe["x"].tolist() == [1.0, 1.0, 1.0, 4.0]

    def test_backward_fill(self):
        df = _df(x=[None, None, 3.0, 4.0])
        plan = _plan(_action(HealingStrategy.BACKWARD_FILL, "x"))
        ctx = _ctx(plan, config={"auto_impute": False})
        result = MissingValueHealer().apply(df, ctx)
        assert result.dataframe["x"].iloc[0] == 3.0

    def test_drop_rows(self):
        df = _df(x=[1.0, None, 3.0], y=["a", "b", "c"])
        plan = _plan(_action(HealingStrategy.DROP_ROWS, params={"subset": ["x"]}))
        ctx = _ctx(plan, config={"auto_impute": False})
        result = MissingValueHealer().apply(df, ctx)
        assert len(result.dataframe) == 2

    def test_auto_impute_numeric(self):
        df = _df(score=[10.0, None, 30.0])
        plan = _plan()   # no actions
        ctx = _ctx(plan, config={"auto_impute": True})
        result = MissingValueHealer().apply(df, ctx)
        assert result.dataframe["score"].isna().sum() == 0

    def test_auto_impute_string(self):
        df = _df(cat=["x", "x", None])
        plan = _plan()
        ctx = _ctx(plan, config={"auto_impute": True})
        result = MissingValueHealer().apply(df, ctx)
        assert result.dataframe["cat"].iloc[2] == "x"

    def test_log_entry_created(self):
        df = _df(x=[1.0, None, 3.0])
        plan = _plan(_action(HealingStrategy.MEAN_IMPUTATION, "x"))
        ctx = _ctx(plan, config={"auto_impute": False})
        result = MissingValueHealer().apply(df, ctx)
        assert result.total_corrections > 0
        assert len(result.entries) == 1
        assert result.entries[0].column == "x"

    def test_no_log_when_no_nulls(self):
        df = _df(x=[1.0, 2.0, 3.0])
        plan = _plan(_action(HealingStrategy.MEAN_IMPUTATION, "x"))
        ctx = _ctx(plan, config={"auto_impute": False})
        result = MissingValueHealer().apply(df, ctx)
        assert result.total_corrections == 0


# ─────────────────────────────────────────────────────────────────────────────
# 3. TypeMismatchHealer
# ─────────────────────────────────────────────────────────────────────────────

class TestTypeMismatchHealer:
    def test_cast_to_float(self):
        df = _df(price=["1.5", "2.0", "bad", "3.0"])
        plan = _plan(_action(HealingStrategy.TYPE_CAST, "price", {"target_type": "float"}))
        ctx = _ctx(plan, config={"auto_type_cast": False})
        result = TypeMismatchHealer().apply(df, ctx)
        assert pd.api.types.is_float_dtype(result.dataframe["price"])

    def test_cast_to_datetime(self):
        df = _df(created=["2024-01-01", "2024-06-15", "2023-12-31"])
        plan = _plan(_action(HealingStrategy.TYPE_CAST, "created", {"target_type": "datetime"}))
        ctx = _ctx(plan, config={"auto_type_cast": False})
        result = TypeMismatchHealer().apply(df, ctx)
        assert pd.api.types.is_datetime64_any_dtype(result.dataframe["created"])

    def test_cast_to_bool(self):
        df = _df(flag=["yes", "no", "true", "false"])
        plan = _plan(_action(HealingStrategy.TYPE_CAST, "flag", {"target_type": "bool"}))
        ctx = _ctx(plan, config={"auto_type_cast": False})
        result = TypeMismatchHealer().apply(df, ctx)
        assert result.dataframe["flag"].tolist()[:2] == [True, False]

    def test_auto_cast_via_semantic(self):
        df = _df(age=["25", "30", "45"])
        plan = _plan()
        ctx = _ctx(
            plan,
            column_semantic_types={"age": "numeric"},
            config={"auto_type_cast": True},
        )
        result = TypeMismatchHealer().apply(df, ctx)
        assert pd.api.types.is_float_dtype(result.dataframe["age"])

    def test_log_entry_on_cast(self):
        df = _df(x=["1", "2", "3"])
        plan = _plan(_action(HealingStrategy.TYPE_CAST, "x", {"target_type": "float"}))
        ctx = _ctx(plan, config={"auto_type_cast": False})
        result = TypeMismatchHealer().apply(df, ctx)
        assert len(result.entries) > 0
        assert "type_cast" in result.entries[0].operation

    def test_no_log_already_correct_dtype(self):
        df = _df(x=[1.0, 2.0, 3.0])
        plan = _plan()
        ctx = _ctx(
            plan,
            column_semantic_types={"x": "numeric"},
            config={"auto_type_cast": True},
        )
        result = TypeMismatchHealer().apply(df, ctx)
        assert result.total_corrections == 0


# ─────────────────────────────────────────────────────────────────────────────
# 4. DuplicateResolver
# ─────────────────────────────────────────────────────────────────────────────

class TestDuplicateResolver:
    def test_removes_exact_duplicates(self):
        df = _df(a=[1, 1, 2, 3], b=["x", "x", "y", "z"])
        plan = _plan(_action(HealingStrategy.DEDUPLICATION))
        ctx = _ctx(plan, config={"auto_deduplicate": False})
        result = DuplicateResolver().apply(df, ctx)
        assert len(result.dataframe) == 3

    def test_keeps_first_by_default(self):
        df = _df(a=[1, 1, 2], b=["original", "dup", "other"])
        plan = _plan(_action(HealingStrategy.DEDUPLICATION, params={"keep": "first"}))
        ctx = _ctx(plan, config={"auto_deduplicate": False})
        result = DuplicateResolver().apply(df, ctx)
        assert result.dataframe.iloc[0]["b"] == "original"

    def test_subset_dedup(self):
        df = _df(id=[1, 1, 2], name=["Alice", "Alice", "Bob"], score=[10, 20, 30])
        plan = _plan(_action(HealingStrategy.DEDUPLICATION, params={"subset": ["id", "name"]}))
        ctx = _ctx(plan, config={"auto_deduplicate": False})
        result = DuplicateResolver().apply(df, ctx)
        assert len(result.dataframe) == 2

    def test_auto_deduplicate(self):
        df = _df(x=[1, 1, 2])
        plan = _plan()  # no actions
        ctx = _ctx(plan, config={"auto_deduplicate": True})
        result = DuplicateResolver().apply(df, ctx)
        assert len(result.dataframe) == 2

    def test_log_entry_records_count(self):
        df = _df(a=[1, 1, 2, 2, 3])
        plan = _plan(_action(HealingStrategy.DEDUPLICATION))
        ctx = _ctx(plan, config={"auto_deduplicate": False})
        result = DuplicateResolver().apply(df, ctx)
        assert result.total_corrections == 2

    def test_no_log_when_no_duplicates(self):
        df = _df(a=[1, 2, 3])
        plan = _plan(_action(HealingStrategy.DEDUPLICATION))
        ctx = _ctx(plan, config={"auto_deduplicate": False})
        result = DuplicateResolver().apply(df, ctx)
        assert result.total_corrections == 0
        assert len(result.entries) == 0


# ─────────────────────────────────────────────────────────────────────────────
# 5. OutlierCapper
# ─────────────────────────────────────────────────────────────────────────────

class TestOutlierCapper:
    def _numeric_df(self):
        return _df(x=[1.0, 2.0, 3.0, 4.0, 5.0, 100.0, -100.0])

    def test_iqr_caps_outliers(self):
        df = self._numeric_df()
        plan = _plan(_action(HealingStrategy.IQR_CLAMP, "x", {"factor": 1.5}))
        ctx = _ctx(plan, config={"auto_cap_outliers": False})
        result = OutlierCapper().apply(df, ctx)
        assert result.dataframe["x"].max() < 100.0
        assert result.dataframe["x"].min() > -100.0

    def test_zscore_caps_outliers(self):
        df = self._numeric_df()
        plan = _plan(_action(HealingStrategy.ZSCORE_CLAMP, "x", {"threshold": 2.0}))
        ctx = _ctx(plan, config={"auto_cap_outliers": False})
        result = OutlierCapper().apply(df, ctx)
        assert result.dataframe["x"].max() < 100.0

    def test_percentile_caps_outliers(self):
        df = _df(x=list(range(100)) + [9999])
        plan = _plan(_action(
            HealingStrategy.PERCENTILE_CLAMP, "x",
            {"lower_pct": 1.0, "upper_pct": 99.0},
        ))
        ctx = _ctx(plan, config={"auto_cap_outliers": False})
        result = OutlierCapper().apply(df, ctx)
        assert result.dataframe["x"].max() < 9999

    def test_log_entry_corrections(self):
        df = self._numeric_df()
        plan = _plan(_action(HealingStrategy.IQR_CLAMP, "x", {"factor": 1.5}))
        ctx = _ctx(plan, config={"auto_cap_outliers": False})
        result = OutlierCapper().apply(df, ctx)
        assert result.total_corrections >= 2   # 100 and -100 capped

    def test_non_numeric_column_skipped(self):
        df = _df(cat=["a", "b", "c"])
        plan = _plan(_action(HealingStrategy.IQR_CLAMP, "cat"))
        ctx = _ctx(plan, config={"auto_cap_outliers": False})
        result = OutlierCapper().apply(df, ctx)
        assert result.total_corrections == 0

    def test_auto_cap_disabled_by_default(self):
        df = self._numeric_df()
        plan = _plan()
        ctx = _ctx(plan, config={"auto_cap_outliers": False})
        result = OutlierCapper().apply(df, ctx)
        assert result.total_corrections == 0

    def test_auto_cap_when_enabled(self):
        df = self._numeric_df()
        plan = _plan()
        ctx = _ctx(plan, config={"auto_cap_outliers": True})
        result = OutlierCapper().apply(df, ctx)
        assert result.total_corrections >= 2


# ─────────────────────────────────────────────────────────────────────────────
# 6. CategoryNormalizer
# ─────────────────────────────────────────────────────────────────────────────

class TestCategoryNormalizer:
    def test_strip_whitespace(self):
        df = _df(status=["  active", "inactive  ", " pending "])
        plan = _plan(_action(HealingStrategy.CATEGORY_NORMALIZE, "status", {"strip": True, "case": None}))
        ctx = _ctx(plan, config={"auto_normalize_categories": False})
        result = CategoryNormalizer().apply(df, ctx)
        assert result.dataframe["status"].tolist() == ["active", "inactive", "pending"]

    def test_lowercase(self):
        df = _df(cat=["Red", "GREEN", "Blue"])
        plan = _plan(_action(HealingStrategy.CATEGORY_NORMALIZE, "cat", {"case": "lower"}))
        ctx = _ctx(plan, config={"auto_normalize_categories": False})
        result = CategoryNormalizer().apply(df, ctx)
        assert result.dataframe["cat"].tolist() == ["red", "green", "blue"]

    def test_uppercase(self):
        df = _df(cat=["red", "green"])
        plan = _plan(_action(HealingStrategy.CATEGORY_NORMALIZE, "cat", {"case": "upper"}))
        ctx = _ctx(plan, config={"auto_normalize_categories": False})
        result = CategoryNormalizer().apply(df, ctx)
        assert result.dataframe["cat"].tolist() == ["RED", "GREEN"]

    def test_alias_map(self):
        df = _df(country=["US", "USA", "United States", "UK"])
        alias_map = {"US": "United States", "USA": "United States"}
        plan = _plan(_action(
            HealingStrategy.CATEGORY_NORMALIZE, "country",
            {"alias_map": alias_map, "case": None},
        ))
        ctx = _ctx(plan, config={"auto_normalize_categories": False})
        result = CategoryNormalizer().apply(df, ctx)
        assert result.dataframe["country"].tolist()[:3] == ["United States"] * 3

    def test_unknown_token(self):
        df = _df(size=["S", "M", "L", "XZ"])
        plan = _plan(_action(
            HealingStrategy.CATEGORY_NORMALIZE, "size",
            {"allowed_values": ["S", "M", "L"], "unknown_token": "UNKNOWN", "case": None},
        ))
        ctx = _ctx(plan, config={"auto_normalize_categories": False})
        result = CategoryNormalizer().apply(df, ctx)
        assert result.dataframe["size"].iloc[3] == "UNKNOWN"

    def test_auto_normalise_low_cardinality(self):
        df = _df(status=["Active", "INACTIVE", "Pending"] * 5)
        plan = _plan()
        ctx = _ctx(plan, config={"auto_normalize_categories": True})
        result = CategoryNormalizer().apply(df, ctx)
        assert result.dataframe["status"].str.islower().all()

    def test_null_values_preserved(self):
        df = _df(cat=["a", None, "b"])
        plan = _plan(_action(HealingStrategy.CATEGORY_NORMALIZE, "cat", {"case": "lower"}))
        ctx = _ctx(plan, config={"auto_normalize_categories": False})
        result = CategoryNormalizer().apply(df, ctx)
        assert pd.isna(result.dataframe["cat"].iloc[1])


# ─────────────────────────────────────────────────────────────────────────────
# 7. FormatCorrector
# ─────────────────────────────────────────────────────────────────────────────

class TestFormatCorrector:
    def _action_fmt(self, col: str, sem: str) -> HealingAction:
        return _action(
            HealingStrategy.FORMAT_STANDARDIZE, col,
            params={"semantic_type": sem},
        )

    def test_email_lowercased_and_stripped(self):
        df = _df(email=["  User@EXAMPLE.COM ", "admin@Test.ORG"])
        plan = _plan(self._action_fmt("email", "email"))
        ctx = _ctx(plan, config={"auto_format_correct": False})
        result = FormatCorrector().apply(df, ctx)
        assert result.dataframe["email"].iloc[0] == "user@example.com"

    def test_phone_normalised_to_e164(self):
        df = _df(phone=["(555) 123-4567", "555.987.6543"])
        plan = _plan(self._action_fmt("phone", "phone"))
        ctx = _ctx(plan, config={"auto_format_correct": False})
        result = FormatCorrector().apply(df, ctx)
        assert result.dataframe["phone"].iloc[0].startswith("+1")

    def test_date_to_iso(self):
        df = _df(date=["01/15/2024", "15-06-2023"])
        plan = _plan(self._action_fmt("date", "date"))
        ctx = _ctx(plan, config={"auto_format_correct": False})
        result = FormatCorrector().apply(df, ctx)
        assert result.dataframe["date"].iloc[0] == "2024-01-15"

    def test_currency_stripped(self):
        df = _df(price=["$1,200.00", "€350.50"])
        plan = _plan(self._action_fmt("price", "currency"))
        ctx = _ctx(plan, config={"auto_format_correct": False})
        result = FormatCorrector().apply(df, ctx)
        assert float(result.dataframe["price"].iloc[0]) == 1200.0

    def test_percentage_stripped(self):
        df = _df(rate=["12%", "45.5%", "100%"])
        plan = _plan(self._action_fmt("rate", "percentage"))
        ctx = _ctx(plan, config={"auto_format_correct": False})
        result = FormatCorrector().apply(df, ctx)
        assert result.dataframe["rate"].iloc[0] == "12.0"

    def test_us_zipcode_padded(self):
        df = _df(zip=["90210", "1001", "10001"])
        plan = _plan(self._action_fmt("zip", "zipcode"))
        ctx = _ctx(plan, config={"auto_format_correct": False})
        result = FormatCorrector().apply(df, ctx)
        assert result.dataframe["zip"].iloc[1] == "01001"

    def test_auto_correct_via_semantic_metadata(self):
        df = _df(email=["  BAD@EXAMPLE.COM "])
        plan = _plan()
        ctx = _ctx(
            plan,
            column_semantic_types={"email": "email"},
            config={"auto_format_correct": True},
        )
        result = FormatCorrector().apply(df, ctx)
        assert result.dataframe["email"].iloc[0] == "bad@example.com"

    def test_null_values_not_changed(self):
        df = _df(email=["user@test.com", None])
        plan = _plan(self._action_fmt("email", "email"))
        ctx = _ctx(plan, config={"auto_format_correct": False})
        result = FormatCorrector().apply(df, ctx)
        assert pd.isna(result.dataframe["email"].iloc[1])


# ─────────────────────────────────────────────────────────────────────────────
# 8. HealingEngine — full pipeline
# ─────────────────────────────────────────────────────────────────────────────

class TestHealingEngine:
    @pytest.fixture
    def df(self):
        return pd.DataFrame({
            "id":       [1, 2, 2, 3, 4],           # row 1 and 2 duplicate
            "score":    [10.0, None, 20.0, 30.0, 200.0],  # null + outlier
            "category": ["Red", "GREEN", "Blue", None, "Red"],  # case inconsistency + null
            "email":    ["A@TEST.COM", "b@test.com", None, "c@test.com", None],
        })

    @pytest.fixture
    def plan(self, df):
        return _plan(
            _action(HealingStrategy.DEDUPLICATION, priority=1),
            _action(HealingStrategy.MEAN_IMPUTATION, "score", priority=2),
            _action(HealingStrategy.IQR_CLAMP, "score", {"factor": 1.5}, priority=3),
            _action(HealingStrategy.CATEGORY_NORMALIZE, "category",
                    {"case": "lower", "strip": True}, priority=4),
            _action(HealingStrategy.FORMAT_STANDARDIZE, "email",
                    {"semantic_type": "email"}, priority=5),
        )

    def test_returns_healed_dataframe(self, df, plan):
        engine = HealingEngine(config={"auto_impute": False, "auto_normalize_categories": False,
                                       "auto_format_correct": False, "auto_type_cast": False,
                                       "auto_deduplicate": False})
        healed, log = engine.run(df, plan)
        assert isinstance(healed, pd.DataFrame)

    def test_deduplication_applied_first(self, df, plan):
        engine = HealingEngine(config={"auto_impute": False, "auto_normalize_categories": False,
                                       "auto_format_correct": False, "auto_type_cast": False,
                                       "auto_deduplicate": False})
        healed, _ = engine.run(df, plan)
        assert len(healed) < len(df)

    def test_transformation_log_populated(self, df, plan):
        engine = HealingEngine(config={"auto_impute": False, "auto_normalize_categories": False,
                                       "auto_format_correct": False, "auto_type_cast": False,
                                       "auto_deduplicate": False})
        _, log = engine.run(df, plan)
        assert isinstance(log, TransformationLog)
        assert log.total_corrections > 0

    def test_strategies_applied_in_log(self, df, plan):
        engine = HealingEngine(config={"auto_impute": False, "auto_normalize_categories": False,
                                       "auto_format_correct": False, "auto_type_cast": False,
                                       "auto_deduplicate": False})
        _, log = engine.run(df, plan)
        applied = log.strategies_applied
        # At least dedup and imputation should appear
        assert any("Duplicate" in s for s in applied)
        assert any("Missing" in s or "Outlier" in s for s in applied)

    def test_each_entry_has_required_fields(self, df, plan):
        engine = HealingEngine(config={"auto_impute": False, "auto_normalize_categories": False,
                                       "auto_format_correct": False, "auto_type_cast": False,
                                       "auto_deduplicate": False})
        _, log = engine.run(df, plan)
        for e in log.entries:
            assert isinstance(e.strategy_name, str)
            assert isinstance(e.operation, str)
            assert isinstance(e.corrections, int)
            assert isinstance(e.applied_at, str)

    def test_exceptions_in_one_strategy_dont_abort_pipeline(self, df):
        """A bad strategy should log an error entry, not raise."""
        from app.modules.healing.strategies.base import HealerBase, HealerResult

        class BrokenHealer(HealerBase):
            name = "BrokenHealer"
            def apply(self, df, ctx):
                raise RuntimeError("intentional failure")

        plan = _plan(_action(HealingStrategy.MEAN_IMPUTATION, "score"))
        engine = HealingEngine(
            strategies=[BrokenHealer, MissingValueHealer],
            config={"auto_impute": False},
        )
        healed, log = engine.run(df, plan)
        assert isinstance(healed, pd.DataFrame)
        error_entries = [e for e in log.entries if e.operation == "ERROR"]
        assert len(error_entries) == 1

    def test_custom_strategy_order(self, df):
        """Engine respects caller-supplied strategy list."""
        plan = _plan()
        engine = HealingEngine(
            strategies=[MissingValueHealer],
            config={"auto_impute": True},
        )
        healed, log = engine.run(df, plan)
        assert healed["score"].isna().sum() == 0

    def test_log_summary_dict(self, df, plan):
        engine = HealingEngine(config={"auto_impute": False, "auto_normalize_categories": False,
                                       "auto_format_correct": False, "auto_type_cast": False,
                                       "auto_deduplicate": False})
        _, log = engine.run(df, plan)
        s = log.summary()
        assert "total_corrections" in s
        assert s["total_corrections"] == log.total_corrections


# ─────────────────────────────────────────────────────────────────────────────
# 9. execute_plan() — public API
# ─────────────────────────────────────────────────────────────────────────────

class TestExecutePlan:
    def test_returns_tuple(self):
        df = _df(x=[1.0, None, 3.0])
        plan = _plan(_action(HealingStrategy.MEAN_IMPUTATION, "x"))
        result_tuple = execute_plan(df, plan, engine_config={"auto_impute": False})
        assert len(result_tuple) == 2

    def test_healed_df_is_dataframe(self):
        df = _df(x=[1.0, None, 3.0])
        plan = _plan(_action(HealingStrategy.MEAN_IMPUTATION, "x"))
        healed, _ = execute_plan(df, plan, engine_config={"auto_impute": False})
        assert isinstance(healed, pd.DataFrame)

    def test_result_is_healing_result(self):
        df = _df(x=[1.0, None, 3.0])
        plan = _plan(_action(HealingStrategy.MEAN_IMPUTATION, "x"))
        _, result = execute_plan(df, plan, engine_config={"auto_impute": False})
        assert isinstance(result, HealingResult)

    def test_result_has_transformation_log(self):
        df = _df(x=[1.0, None, 3.0])
        plan = _plan(_action(HealingStrategy.MEAN_IMPUTATION, "x"))
        _, result = execute_plan(df, plan, engine_config={"auto_impute": False})
        assert isinstance(result.transformation_log, TransformationLog)

    def test_result_has_backward_compat_execution_log(self):
        df = _df(x=[1.0, None, 3.0])
        plan = _plan(_action(HealingStrategy.MEAN_IMPUTATION, "x"))
        _, result = execute_plan(df, plan, engine_config={"auto_impute": False})
        assert isinstance(result.execution_log, list)

    def test_rows_modified_nonzero_on_changes(self):
        df = _df(x=[1.0, None, None])
        plan = _plan(_action(HealingStrategy.MEAN_IMPUTATION, "x"))
        _, result = execute_plan(df, plan, engine_config={"auto_impute": False})
        assert result.rows_modified >= 2

    def test_semantic_types_forwarded(self):
        df = _df(age=["25", "30", "45"])
        plan = _plan()
        _, result = execute_plan(
            df, plan,
            column_semantic_types={"age": "numeric"},
            engine_config={"auto_type_cast": True},
        )
        assert result.transformation_log.total_corrections > 0

    def test_model_is_json_serialisable(self):
        df = _df(x=[1.0, None, 3.0])
        plan = _plan(_action(HealingStrategy.MEAN_IMPUTATION, "x"))
        _, result = execute_plan(df, plan, engine_config={"auto_impute": False})
        data = result.model_dump()
        assert "transformation_log" in data
        assert "total_corrections" in data["transformation_log"]
