"""HealingEngine — orchestrates the sequential application of healing strategies.

Usage
─────
    from app.modules.healing.engine import HealingEngine
    from app.models.healing import HealingPlan

    engine = HealingEngine()
    healed_df, log = engine.run(df, plan)

The default strategy order is intentional:
  1. DuplicateResolver    — remove exact duplicates first so later stats are clean
  2. TypeMismatchHealer   — coerce types before imputing / capping
  3. MissingValueHealer   — fill nulls after types are correct
  4. OutlierCapper        — cap outliers on clean, correctly-typed numerics
  5. CategoryNormalizer   — normalise strings after type coercion
  6. FormatCorrector      — fix formats last (depends on clean values)

Custom strategy lists can be passed to the constructor.

Every healer that modifies the DataFrame appends :class:`TransformationEntry`
records to the shared :class:`TransformationLog`. The engine also catches and
logs healer-level exceptions so that a single broken strategy never aborts the
entire pipeline.
"""
from __future__ import annotations

import uuid
from typing import Any

import pandas as pd

from app.core.logging import get_logger
from app.models.healing import (
    HealingAction,
    HealingPlan,
    HealingStrategy,
    TransformationEntry,
    TransformationLog,
)
from app.modules.healing.strategies import (
    CategoryNormalizer,
    DuplicateResolver,
    FormatCorrector,
    HealerBase,
    HealingContext,
    MissingValueHealer,
    OutlierCapper,
    TypeMismatchHealer,
)

logger = get_logger(__name__)

# ── Default strategy pipeline ─────────────────────────────────────────────────

DEFAULT_STRATEGIES: list[type[HealerBase]] = [
    DuplicateResolver,
    TypeMismatchHealer,
    MissingValueHealer,
    OutlierCapper,
    CategoryNormalizer,
    FormatCorrector,
]

# ── Strategy registry — maps AI-produced strategy name strings to healer classes
# The AI agent emits free-form strategy names; this registry resolves them.

STRATEGY_REGISTRY: dict[str, type[HealerBase]] = {
    # Deduplication
    "deduplication":         DuplicateResolver,
    "deduplicate":           DuplicateResolver,
    "remove_duplicates":     DuplicateResolver,
    # Missing-value imputation
    "median_imputation":     MissingValueHealer,
    "mean_imputation":       MissingValueHealer,
    "mode_imputation":       MissingValueHealer,
    "constant_imputation":   MissingValueHealer,
    "interpolation":         MissingValueHealer,
    "forward_fill":          MissingValueHealer,
    "backward_fill":         MissingValueHealer,
    "drop_rows":             MissingValueHealer,
    "drop_missing":          MissingValueHealer,
    "imputation":            MissingValueHealer,
    "impute":                MissingValueHealer,
    # Outlier handling
    "iqr_clamp":             OutlierCapper,
    "zscore_clamp":          OutlierCapper,
    "percentile_clamp":      OutlierCapper,
    "outlier_capping":       OutlierCapper,
    "cap_outliers":          OutlierCapper,
    "outlier_removal":       OutlierCapper,
    # Type coercion
    "type_cast":             TypeMismatchHealer,
    "type_coercion":         TypeMismatchHealer,
    "type_mismatch":         TypeMismatchHealer,
    # Format correction
    "format_standardize":    FormatCorrector,
    "format_correction":     FormatCorrector,
    "format_fix":            FormatCorrector,
    # Category normalisation
    "category_normalize":    CategoryNormalizer,
    "normalize":             CategoryNormalizer,
    "category_normalisation":CategoryNormalizer,
    "string_normalise":      CategoryNormalizer,
    "value_mapping":         CategoryNormalizer,
}


# ── Module-level helpers ──────────────────────────────────────────────────────

def _agent_plan_to_healing_plan(
    healing_steps: list,   # list[HealingStep] — avoid hard import
    dataset_id: Any,
    model_used: str,
) -> HealingPlan:
    """Convert AI agent HealingStep objects to a :class:`HealingPlan`.

    Each ``HealingStep`` maps to one ``HealingAction`` per target column.
    Unrecognised strategy strings default to :data:`HealingStrategy.CUSTOM`.
    """
    actions: list[HealingAction] = []
    strategy_map: dict[str, HealingStrategy] = {
        s.value: s for s in HealingStrategy
    }

    for step in healing_steps:
        raw_strategy = str(step.strategy).upper()
        # Try enum name first, then value, then CUSTOM
        healing_strategy = (
            HealingStrategy.__members__.get(raw_strategy)
            or strategy_map.get(str(step.strategy).lower())
            or HealingStrategy.CUSTOM
        )

        target_cols: list[str | None] = (
            list(step.target_columns) if step.target_columns else [None]
        )

        for col in target_cols:
            actions.append(
                HealingAction(
                    action_id=uuid.uuid4(),
                    column=col or "*",
                    strategy=healing_strategy,
                    parameters=dict(step.parameters) if step.parameters else {},
                    rationale=step.rationale,
                    priority=int(step.priority) if step.priority is not None else 5,
                    estimated_impact=(
                        float(step.estimated_impact)
                        if step.estimated_impact is not None else 0.0
                    ),
                )
            )

    return HealingPlan(
        dataset_id=dataset_id,
        llm_model=model_used,
        actions=actions,
        overall_rationale="Converted from AI agent response.",
    )


def _validate_transformation(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
) -> list[str]:
    """Structural sanity checks between *before_df* and *after_df*.

    Returns a (possibly empty) list of human-readable warning strings.
    These are logged and stored in :class:`TransformationLog` entries but
    never abort the pipeline.
    """
    warnings: list[str] = []

    # Row count — a healer should rarely add rows
    if len(after_df) > len(before_df):
        warnings.append(
            f"Row count increased: {len(before_df)} → {len(after_df)}"
        )

    # Column cardinality
    added_cols = set(after_df.columns) - set(before_df.columns)
    removed_cols = set(before_df.columns) - set(after_df.columns)
    if added_cols:
        warnings.append(f"Unexpected columns added: {sorted(added_cols)}")
    if removed_cols:
        warnings.append(f"Columns removed: {sorted(removed_cols)}")

    # Null delta — warn if overall nulls increased after healing
    null_before = int(before_df.isnull().sum().sum())
    null_after = int(after_df.isnull().sum().sum())
    if null_after > null_before:
        warnings.append(
            f"Total null count increased after healing: {null_before} → {null_after}"
        )

    # dtype regressions — numeric column became object
    for col in before_df.columns:
        if col not in after_df.columns:
            continue
        b_kind = before_df[col].dtype.kind
        a_kind = after_df[col].dtype.kind
        if b_kind in ("i", "u", "f") and a_kind == "O":
            warnings.append(
                f"Column '{col}' regressed from numeric to object dtype."
            )

    return warnings


# ── Engine ────────────────────────────────────────────────────────────────────

class HealingEngine:
    """Runs healing strategies sequentially and accumulates a TransformationLog.

    Parameters
    ----------
    strategies:
        Ordered list of healer *classes* (not instances) to run.
        Defaults to :data:`DEFAULT_STRATEGIES`.
    config:
        Dict of engine-wide configuration flags forwarded to every
        :class:`~app.modules.healing.strategies.base.HealingContext`.
        Recognised keys (all bool):
          auto_impute, auto_type_cast, auto_deduplicate,
          auto_cap_outliers, auto_normalize_categories, auto_format_correct
    """

    def __init__(
        self,
        strategies: list[type[HealerBase]] | None = None,
        config: dict[str, Any] | None = None,
    ) -> None:
        self._strategy_classes = strategies or DEFAULT_STRATEGIES
        self._config = config or {}

    # ── Public API ────────────────────────────────────────────────────────

    def run(
        self,
        df: pd.DataFrame,
        plan: HealingPlan,
        column_semantic_types: dict[str, str] | None = None,
        column_dtype_categories: dict[str, str] | None = None,
    ) -> tuple[pd.DataFrame, TransformationLog]:
        """Apply all strategies to *df* using *plan*.

        Returns
        -------
        healed_df : pd.DataFrame
            The DataFrame after all strategies have been applied.
        log : TransformationLog
            Full record of every transformation, total corrections, and list
            of strategies that made at least one change.
        """
        context = HealingContext(
            plan=plan,
            column_semantic_types=column_semantic_types or {},
            column_dtype_categories=column_dtype_categories or {},
            config=self._config,
        )

        current_df = df.copy()
        transformation_log = TransformationLog()

        for strategy_cls in self._strategy_classes:
            healer: HealerBase = strategy_cls()
            try:
                if not healer.can_apply(current_df, context):
                    logger.debug(
                        "healing_strategy_skipped",
                        strategy=healer.name,
                        reason="can_apply returned False",
                    )
                    continue

                logger.info("healing_strategy_start", strategy=healer.name)
                result = healer.apply(current_df, context)
                current_df = result.dataframe

                for entry in result.entries:
                    transformation_log.append(entry)

                logger.info(
                    "healing_strategy_done",
                    strategy=healer.name,
                    corrections=result.total_corrections,
                    entries=len(result.entries),
                )

            except Exception as exc:
                logger.error(
                    "healing_strategy_error",
                    strategy=healer.name,
                    error=str(exc),
                    exc_info=True,
                )
                # Log the failure as a zero-correction entry so it is visible
                transformation_log.append(
                    TransformationEntry(
                        strategy_name=healer.name,
                        operation="ERROR",
                        corrections=0,
                        detail=f"Strategy raised exception: {exc}",
                    )
                )

        logger.info(
            "healing_engine_complete",
            total_corrections=transformation_log.total_corrections,
            strategies_applied=transformation_log.strategies_applied,
        )
        return current_df, transformation_log

    # ── AI-directed plan execution ────────────────────────────────────────

    def run_ai(
        self,
        df: pd.DataFrame,
        agent_response,    # app.models.agent.AgentResponse
        column_semantic_types: dict[str, str] | None = None,
        column_dtype_categories: dict[str, str] | None = None,
        dataset_id: Any = None,
        model_used: str = "unknown",
    ) -> tuple[pd.DataFrame, TransformationLog]:
        """Execute an AI-agent healing plan on *df*.

        This mirrors :meth:`run` but sources strategy ordering from
        ``agent_response.transformation_order`` and marks every resulting
        :class:`TransformationEntry` with ``source="ai"``.

        Unrecognised strategy names in ``transformation_order`` are skipped
        with a warning rather than raising an exception.  After all strategies
        have been applied, :func:`_validate_transformation` runs a structural
        sanity check and any warnings are appended as a final ERROR-free entry.

        Parameters
        ----------
        df:
            DataFrame to heal.
        agent_response:
            An :class:`~app.models.agent.AgentResponse` from the AI agent.
        column_semantic_types / column_dtype_categories:
            Optional column metadata forwarded to each healer's context.
        dataset_id:
            UUID of the dataset — used when building the intermediate
            :class:`HealingPlan` for the healer context.
        model_used:
            Model identifier string stored in the generated HealingPlan.

        Returns
        -------
        healed_df : pd.DataFrame
        log       : TransformationLog
        """
        _dataset_id = dataset_id or uuid.uuid4()

        # Build HealingPlan from the AI response so HealingContext is satisfied
        plan = _agent_plan_to_healing_plan(
            agent_response.healing_plan,
            _dataset_id,
            model_used,
        )

        context = HealingContext(
            plan=plan,
            column_semantic_types=column_semantic_types or {},
            column_dtype_categories=column_dtype_categories or {},
            config=self._config,
        )

        before_df = df.copy()
        current_df = df.copy()
        transformation_log = TransformationLog()

        # Resolve ordered strategy classes; fall back to default if list is empty
        ordered_classes: list[type[HealerBase]] = []
        for name in (agent_response.transformation_order or []):
            cls = STRATEGY_REGISTRY.get(name.lower().strip())
            if cls is None:
                logger.warning(
                    "ai_strategy_unknown",
                    strategy_name=name,
                    message="Strategy name not found in registry; skipping.",
                )
                transformation_log.append(
                    TransformationEntry(
                        strategy_name=name,
                        operation="SKIPPED",
                        corrections=0,
                        detail=f"Unknown strategy '{name}' — not in STRATEGY_REGISTRY.",
                        source="ai",
                    )
                )
                continue
            # De-duplicate while preserving order
            if cls not in ordered_classes:
                ordered_classes.append(cls)

        if not ordered_classes:
            logger.warning(
                "ai_run_fallback_to_default",
                message="transformation_order yielded no known strategies; "
                        "falling back to DEFAULT_STRATEGIES.",
            )
            ordered_classes = list(DEFAULT_STRATEGIES)

        for strategy_cls in ordered_classes:
            healer: HealerBase = strategy_cls()
            try:
                if not healer.can_apply(current_df, context):
                    logger.debug(
                        "ai_healing_strategy_skipped",
                        strategy=healer.name,
                        reason="can_apply returned False",
                    )
                    continue

                logger.info("ai_healing_strategy_start", strategy=healer.name)
                result = healer.apply(current_df, context)
                current_df = result.dataframe

                for entry in result.entries:
                    # Mark every entry as AI-sourced
                    entry.source = "ai"
                    transformation_log.append(entry)

                logger.info(
                    "ai_healing_strategy_done",
                    strategy=healer.name,
                    corrections=result.total_corrections,
                )

            except Exception as exc:
                logger.error(
                    "ai_healing_strategy_error",
                    strategy=healer.name,
                    error=str(exc),
                    exc_info=True,
                )
                transformation_log.append(
                    TransformationEntry(
                        strategy_name=healer.name,
                        operation="ERROR",
                        corrections=0,
                        detail=f"Strategy raised exception: {exc}",
                        source="ai",
                    )
                )

        # Post-run structural validation
        validation_warnings = _validate_transformation(before_df, current_df)
        if validation_warnings:
            logger.warning(
                "ai_transformation_validation_warnings",
                warnings=validation_warnings,
            )
            transformation_log.append(
                TransformationEntry(
                    strategy_name="TransformationValidator",
                    operation="VALIDATION",
                    corrections=0,
                    detail="; ".join(validation_warnings),
                    source="ai",
                    validation_warnings=validation_warnings,
                )
            )

        logger.info(
            "ai_healing_engine_complete",
            total_corrections=transformation_log.total_corrections,
            ai_corrections=transformation_log.ai_total_corrections,
            strategies_applied=transformation_log.strategies_applied,
            validation_warnings=len(validation_warnings),
        )
        return current_df, transformation_log

    # ── Convenience factory ───────────────────────────────────────────────

    @classmethod
    def from_profile(
        cls,
        profile,          # DatasetProfile — avoid hard import for circular-dep safety
        config: dict[str, Any] | None = None,
    ) -> tuple[HealingEngine, dict[str, str], dict[str, str]]:
        """Build an engine pre-loaded with column metadata from a DatasetProfile.

        Returns (engine, semantic_types_dict, dtype_categories_dict) so the
        caller can pass those directly to :meth:`run`.
        """
        semantic_types: dict[str, str] = {}
        dtype_categories: dict[str, str] = {}
        for col in profile.columns:
            if col.semantic_type:
                semantic_types[col.name] = col.semantic_type
            dtype_categories[col.name] = col.dtype_category
        return cls(config=config), semantic_types, dtype_categories
