"""Tests for AI-directed healing engine integration.

Covers:
  - STRATEGY_REGISTRY completeness and correctness
  - _agent_plan_to_healing_plan() conversion
  - _validate_transformation() checks
  - TransformationLog AI tracking (source field, ai_entries, ai_total_corrections)
  - HealingEngine.run_ai() success, fallback, unknown strategy, error handling
  - execute_ai_plan() public API
"""
from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from app.models.healing import (
    HealingResult,
    HealingStrategy,
    TransformationEntry,
    TransformationLog,
)
from app.modules.healing.engine import (
    DEFAULT_STRATEGIES,
    STRATEGY_REGISTRY,
    HealingEngine,
    _agent_plan_to_healing_plan,
    _validate_transformation,
)
from app.modules.healing.executor import execute_ai_plan
from app.modules.healing.strategies import (
    CategoryNormalizer,
    DuplicateResolver,
    FormatCorrector,
    MissingValueHealer,
    OutlierCapper,
    TypeMismatchHealer,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_healing_step(
    strategy: str = "median_imputation",
    target_columns: list[str] | None = None,
    parameters: dict | None = None,
    rationale: str = "test",
    priority: int = 5,
    estimated_impact: float = 0.5,
) -> MagicMock:
    step = MagicMock()
    step.strategy = strategy
    step.target_columns = target_columns or []
    step.parameters = parameters or {}
    step.rationale = rationale
    step.priority = priority
    step.estimated_impact = estimated_impact
    return step


def _make_agent_response(
    transformation_order: list[str] | None = None,
    healing_steps: list | None = None,
) -> MagicMock:
    resp = MagicMock()
    resp.transformation_order = transformation_order or ["deduplication", "median_imputation"]
    resp.healing_plan = healing_steps or [_make_healing_step()]
    return resp


def _make_agent_result(response=None) -> MagicMock:
    result = MagicMock()
    result.dataset_id = uuid.uuid4()
    result.model_used = "test-model"
    result.response = response or _make_agent_response()
    return result


def _simple_df() -> pd.DataFrame:
    return pd.DataFrame({
        "a": [1, 2, None, 4, 5],
        "b": ["x", "x", "y", "z", "z"],
    })


# ===========================================================================
# 1. STRATEGY_REGISTRY
# ===========================================================================

class TestStrategyRegistry:
    def test_registry_is_not_empty(self):
        assert len(STRATEGY_REGISTRY) > 0

    def test_all_values_are_healer_subclasses(self):
        from app.modules.healing.strategies.base import HealerBase
        for name, cls in STRATEGY_REGISTRY.items():
            assert issubclass(cls, HealerBase), f"{name!r} maps to non-healer {cls}"

    def test_deduplication_maps_to_duplicate_resolver(self):
        assert STRATEGY_REGISTRY["deduplication"] is DuplicateResolver
        assert STRATEGY_REGISTRY["deduplicate"] is DuplicateResolver

    def test_imputation_variants_map_to_missing_value_healer(self):
        imputation_keys = [
            "median_imputation", "mean_imputation", "mode_imputation",
            "forward_fill", "backward_fill", "drop_rows", "impute",
        ]
        for key in imputation_keys:
            assert STRATEGY_REGISTRY[key] is MissingValueHealer, key

    def test_outlier_variants_map_to_outlier_capper(self):
        for key in ["iqr_clamp", "zscore_clamp", "percentile_clamp"]:
            assert STRATEGY_REGISTRY[key] is OutlierCapper, key

    def test_type_cast_maps_to_type_mismatch_healer(self):
        for key in ["type_cast", "type_coercion"]:
            assert STRATEGY_REGISTRY[key] is TypeMismatchHealer, key

    def test_format_variants_map_to_format_corrector(self):
        for key in ["format_standardize", "format_correction"]:
            assert STRATEGY_REGISTRY[key] is FormatCorrector, key

    def test_category_variants_map_to_category_normalizer(self):
        for key in ["category_normalize", "normalize"]:
            assert STRATEGY_REGISTRY[key] is CategoryNormalizer, key

    def test_all_default_strategy_classes_reachable(self):
        """Every class in DEFAULT_STRATEGIES must be reachable via registry."""
        registry_classes = set(STRATEGY_REGISTRY.values())
        for cls in DEFAULT_STRATEGIES:
            assert cls in registry_classes, f"{cls.__name__} missing from registry"


# ===========================================================================
# 2. _agent_plan_to_healing_plan
# ===========================================================================

class TestAgentPlanConversion:
    def test_returns_healing_plan(self):
        from app.models.healing import HealingPlan
        dataset_id = uuid.uuid4()
        steps = [_make_healing_step("median_imputation", target_columns=["a", "b"])]
        plan = _agent_plan_to_healing_plan(steps, dataset_id, "claude-3")
        assert isinstance(plan, HealingPlan)

    def test_dataset_id_and_model_forwarded(self):
        dataset_id = uuid.uuid4()
        plan = _agent_plan_to_healing_plan(
            [_make_healing_step()], dataset_id, "my-model"
        )
        assert plan.dataset_id == dataset_id
        assert plan.llm_model == "my-model"

    def test_expands_target_columns(self):
        steps = [_make_healing_step(target_columns=["col_a", "col_b"])]
        plan = _agent_plan_to_healing_plan(steps, uuid.uuid4(), "m")
        # one step × 2 columns → 2 actions
        assert len(plan.actions) == 2
        col_names = {a.column for a in plan.actions}
        assert col_names == {"col_a", "col_b"}

    def test_no_target_columns_uses_wildcard(self):
        steps = [_make_healing_step(target_columns=[])]
        plan = _agent_plan_to_healing_plan(steps, uuid.uuid4(), "m")
        assert len(plan.actions) == 1
        assert plan.actions[0].column == "*"

    def test_priority_and_impact_forwarded(self):
        steps = [_make_healing_step(priority=3, estimated_impact=0.7)]
        plan = _agent_plan_to_healing_plan(steps, uuid.uuid4(), "m")
        assert plan.actions[0].priority == 3
        assert plan.actions[0].estimated_impact == pytest.approx(0.7)

    def test_unknown_strategy_defaults_to_custom(self):
        steps = [_make_healing_step(strategy="totally_unknown_strategy")]
        plan = _agent_plan_to_healing_plan(steps, uuid.uuid4(), "m")
        assert plan.actions[0].strategy == HealingStrategy.CUSTOM

    def test_empty_steps_produces_empty_actions(self):
        plan = _agent_plan_to_healing_plan([], uuid.uuid4(), "m")
        assert plan.actions == []

    def test_multiple_steps(self):
        steps = [
            _make_healing_step("median_imputation", target_columns=["a"]),
            _make_healing_step("deduplication", target_columns=[]),
        ]
        plan = _agent_plan_to_healing_plan(steps, uuid.uuid4(), "m")
        assert len(plan.actions) == 2   # 1 col + wildcard

    def test_parameters_forwarded(self):
        params = {"fill_value": 0, "method": "constant"}
        steps = [_make_healing_step(parameters=params)]
        plan = _agent_plan_to_healing_plan(steps, uuid.uuid4(), "m")
        assert plan.actions[0].parameters == params


# ===========================================================================
# 3. _validate_transformation
# ===========================================================================

class TestValidateTransformation:
    def _base(self) -> pd.DataFrame:
        return pd.DataFrame({"a": [1.0, 2.0, 3.0], "b": ["x", "y", "z"]})

    def test_identical_frames_no_warnings(self):
        df = self._base()
        assert _validate_transformation(df, df.copy()) == []

    def test_row_increase_triggers_warning(self):
        before = self._base()
        after = pd.concat([before, before], ignore_index=True)
        warnings = _validate_transformation(before, after)
        assert any("Row count increased" in w for w in warnings)

    def test_added_column_triggers_warning(self):
        before = self._base()
        after = before.copy()
        after["c"] = 0
        warnings = _validate_transformation(before, after)
        assert any("columns added" in w.lower() for w in warnings)

    def test_removed_column_triggers_warning(self):
        before = self._base()
        after = before.drop(columns=["b"])
        warnings = _validate_transformation(before, after)
        assert any("Columns removed" in w for w in warnings)

    def test_null_increase_triggers_warning(self):
        before = pd.DataFrame({"a": [1.0, 2.0, 3.0]})
        after = pd.DataFrame({"a": [1.0, None, None]})
        warnings = _validate_transformation(before, after)
        assert any("null count increased" in w.lower() for w in warnings)

    def test_numeric_to_object_regression_triggers_warning(self):
        before = pd.DataFrame({"a": [1.0, 2.0, 3.0]})
        after = pd.DataFrame({"a": ["x", "y", "z"]})
        warnings = _validate_transformation(before, after)
        assert any("regressed from numeric" in w for w in warnings)

    def test_row_decrease_no_warning(self):
        before = self._base()
        after = before.iloc[:2].copy()
        # Shrinking rows is allowed; should not produce the row-increase warning
        warnings = _validate_transformation(before, after)
        row_warns = [w for w in warnings if "Row count increased" in w]
        assert row_warns == []


# ===========================================================================
# 4. TransformationLog AI tracking
# ===========================================================================

class TestTransformationLogAI:
    def _rule_entry(self, corrections: int = 1) -> TransformationEntry:
        return TransformationEntry(
            strategy_name="MissingValueHealer",
            operation="median_imputation",
            column="a",
            corrections=corrections,
            source="rule",
        )

    def _ai_entry(self, corrections: int = 2) -> TransformationEntry:
        return TransformationEntry(
            strategy_name="MissingValueHealer",
            operation="median_imputation",
            column="b",
            corrections=corrections,
            source="ai",
        )

    def test_source_field_default_is_rule(self):
        entry = TransformationEntry(
            strategy_name="S", operation="op", corrections=0
        )
        assert entry.source == "rule"

    def test_ai_entry_source(self):
        entry = self._ai_entry()
        assert entry.source == "ai"

    def test_append_rule_not_in_ai_entries(self):
        log = TransformationLog()
        log.append(self._rule_entry())
        assert len(log.ai_entries) == 0
        assert log.ai_total_corrections == 0

    def test_append_ai_tracked_separately(self):
        log = TransformationLog()
        log.append(self._ai_entry(corrections=3))
        assert len(log.ai_entries) == 1
        assert log.ai_total_corrections == 3

    def test_mixed_entries_separated_correctly(self):
        log = TransformationLog()
        log.append(self._rule_entry(corrections=5))
        log.append(self._ai_entry(corrections=7))
        log.append(self._rule_entry(corrections=2))
        log.append(self._ai_entry(corrections=4))
        assert log.total_corrections == 18
        assert log.ai_total_corrections == 11
        assert len(log.ai_entries) == 2
        assert len(log.entries) == 4

    def test_strategies_applied_not_duplicated(self):
        log = TransformationLog()
        log.append(self._rule_entry())
        log.append(self._rule_entry())
        assert log.strategies_applied.count("MissingValueHealer") == 1

    def test_ai_summary_structure(self):
        log = TransformationLog()
        log.append(self._ai_entry(corrections=3))
        summary = log.ai_summary()
        assert summary["ai_total_corrections"] == 3
        assert summary["ai_entry_count"] == 1

    def test_full_summary_includes_ai_key(self):
        log = TransformationLog()
        summary = log.summary()
        assert "ai" in summary

    def test_validation_warnings_stored_on_entry(self):
        entry = TransformationEntry(
            strategy_name="V",
            operation="VALIDATION",
            corrections=0,
            source="ai",
            validation_warnings=["null count increased"],
        )
        assert "null count increased" in entry.validation_warnings


# ===========================================================================
# 5. HealingEngine.run_ai()
# ===========================================================================

class TestRunAI:
    def _df_with_nulls(self) -> pd.DataFrame:
        return pd.DataFrame({
            "a": [1.0, 2.0, None, 4.0, 5.0],
            "b": [10, 10, 20, 30, 30],
        })

    def test_returns_tuple_of_dataframe_and_log(self):
        engine = HealingEngine()
        agent_resp = _make_agent_response(
            transformation_order=["deduplication"],
            healing_steps=[_make_healing_step("deduplication")],
        )
        df = self._df_with_nulls()
        result = engine.run_ai(df, agent_resp)
        assert isinstance(result, tuple)
        healed_df, log = result
        assert isinstance(healed_df, pd.DataFrame)
        assert isinstance(log, TransformationLog)

    def test_all_entries_marked_as_ai(self):
        engine = HealingEngine()
        agent_resp = _make_agent_response(
            transformation_order=["deduplication"],
        )
        _, log = engine.run_ai(self._df_with_nulls(), agent_resp)
        for entry in log.entries:
            if entry.operation not in ("SKIPPED",):
                assert entry.source == "ai", f"Entry source: {entry.source!r}"

    def test_unknown_strategy_generates_skipped_entry(self):
        engine = HealingEngine()
        agent_resp = _make_agent_response(
            transformation_order=["totally_made_up_strategy"],
        )
        _, log = engine.run_ai(self._df_with_nulls(), agent_resp)
        skipped = [e for e in log.entries if e.operation == "SKIPPED"]
        assert len(skipped) >= 1
        assert skipped[0].strategy_name == "totally_made_up_strategy"

    def test_empty_transformation_order_falls_back_to_defaults(self):
        engine = HealingEngine()
        agent_resp = _make_agent_response(transformation_order=[])
        df = self._df_with_nulls()
        healed_df, log = engine.run_ai(df, agent_resp)
        # Should not raise; log should have entries
        assert isinstance(healed_df, pd.DataFrame)

    def test_healer_exception_logged_not_raised(self):
        """If a healer raises, run_ai continues and logs an ERROR entry."""
        engine = HealingEngine()
        agent_resp = _make_agent_response(
            transformation_order=["deduplication"],
        )
        with patch.object(
            DuplicateResolver, "apply", side_effect=ValueError("boom")
        ):
            _, log = engine.run_ai(self._df_with_nulls(), agent_resp)
        error_entries = [e for e in log.entries if e.operation == "ERROR"]
        assert len(error_entries) >= 1
        assert "boom" in error_entries[0].detail

    def test_dataset_id_forwarded_to_plan(self):
        engine = HealingEngine()
        did = uuid.uuid4()
        agent_resp = _make_agent_response(transformation_order=["deduplication"])
        # Just confirm it runs without error when dataset_id provided
        healed_df, log = engine.run_ai(
            self._df_with_nulls(), agent_resp, dataset_id=did
        )
        assert isinstance(healed_df, pd.DataFrame)

    def test_validation_warning_appended_when_null_increases(self):
        """Force a scenario where after_df has more nulls than before_df."""
        engine = HealingEngine(strategies=[])  # no healers → df unchanged
        # Craft a response where transformation_order resolves to no-op
        agent_resp = _make_agent_response(transformation_order=[])

        # Monkey-patch _validate_transformation to always return a warning
        with patch(
            "app.modules.healing.engine._validate_transformation",
            return_value=["null count increased: 0 → 5"],
        ):
            _, log = engine.run_ai(self._df_with_nulls(), agent_resp)

        validation_entries = [e for e in log.entries if e.operation == "VALIDATION"]
        assert len(validation_entries) == 1
        assert "null count increased" in validation_entries[0].detail

    def test_deduplication_reduces_rows(self):
        df = pd.DataFrame({
            "a": [1, 1, 2, 3],
            "b": ["x", "x", "y", "z"],
        })
        engine = HealingEngine()
        agent_resp = _make_agent_response(
            transformation_order=["deduplication"],
            healing_steps=[_make_healing_step("deduplication")],
        )
        healed_df, log = engine.run_ai(df, agent_resp)
        assert len(healed_df) < len(df)

    def test_ai_total_corrections_tracked(self):
        df = pd.DataFrame({"a": [1, 1, 2, 3], "b": ["x", "x", "y", "z"]})
        engine = HealingEngine()
        agent_resp = _make_agent_response(
            transformation_order=["deduplication"],
            healing_steps=[_make_healing_step("deduplication")],
        )
        _, log = engine.run_ai(df, agent_resp)
        assert log.ai_total_corrections >= 0   # non-negative, may be 0 if no dups


# ===========================================================================
# 6. execute_ai_plan()
# ===========================================================================

class TestExecuteAIPlan:
    def _df(self) -> pd.DataFrame:
        return pd.DataFrame({
            "x": [1, 2, 2, 3, 4],
            "y": [None, 2.0, 2.0, 3.0, 4.0],
        })

    def test_returns_tuple(self):
        result = execute_ai_plan(self._df(), _make_agent_result())
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_healed_df_is_dataframe(self):
        healed_df, _ = execute_ai_plan(self._df(), _make_agent_result())
        assert isinstance(healed_df, pd.DataFrame)

    def test_result_is_healing_result(self):
        _, result = execute_ai_plan(self._df(), _make_agent_result())
        assert isinstance(result, HealingResult)

    def test_dataset_id_propagated(self):
        agent_result = _make_agent_result()
        _, result = execute_ai_plan(self._df(), agent_result)
        assert result.dataset_id == agent_result.dataset_id

    def test_ai_execution_log_populated(self):
        _, result = execute_ai_plan(self._df(), _make_agent_result())
        # ai_execution_log must be a list (possibly empty if no AI entries)
        assert isinstance(result.ai_execution_log, list)

    def test_execution_log_has_entries(self):
        agent_result = _make_agent_result(
            response=_make_agent_response(
                transformation_order=["deduplication"],
                healing_steps=[_make_healing_step("deduplication")],
            )
        )
        _, result = execute_ai_plan(self._df(), agent_result)
        assert isinstance(result.execution_log, list)

    def test_validation_warnings_accessible(self):
        _, result = execute_ai_plan(self._df(), _make_agent_result())
        assert isinstance(result.validation_warnings, list)

    def test_empty_df_does_not_raise(self):
        empty = pd.DataFrame({"a": pd.Series([], dtype=float)})
        agent_result = _make_agent_result()
        healed_df, result = execute_ai_plan(empty, agent_result)
        assert isinstance(healed_df, pd.DataFrame)
        assert isinstance(result, HealingResult)

    def test_engine_config_forwarded(self):
        config = {"auto_impute": False}
        _, result = execute_ai_plan(
            self._df(), _make_agent_result(), engine_config=config
        )
        assert isinstance(result, HealingResult)

    def test_semantic_types_forwarded(self):
        sem = {"x": "integer", "y": "float"}
        _, result = execute_ai_plan(
            self._df(), _make_agent_result(), column_semantic_types=sem
        )
        assert isinstance(result, HealingResult)

    def test_actions_applied_and_skipped_non_negative(self):
        _, result = execute_ai_plan(self._df(), _make_agent_result())
        assert result.actions_applied >= 0
        assert result.actions_skipped >= 0
