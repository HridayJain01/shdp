"""Healing executor — public API consumed by the Celery pipeline.

This module is the stable integration point; the real logic lives in
:class:`~app.modules.healing.engine.HealingEngine` and the individual
strategy classes under ``strategies/``.

``execute_plan`` keeps its original signature so ``pipeline.py`` requires no
changes.
"""
from __future__ import annotations

import pandas as pd

from app.core.logging import get_logger
from app.models.healing import HealingPlan, HealingResult
from app.modules.healing.engine import HealingEngine

logger = get_logger(__name__)


def execute_plan(
    df: pd.DataFrame,
    plan: HealingPlan,
    *,
    column_semantic_types: dict[str, str] | None = None,
    column_dtype_categories: dict[str, str] | None = None,
    engine_config: dict | None = None,
) -> tuple[pd.DataFrame, HealingResult]:
    """Apply *plan* to *df* using the :class:`HealingEngine`.

    Parameters
    ----------
    df:
        Raw / profiled DataFrame to heal.
    plan:
        LLM-generated or rule-based healing plan.
    column_semantic_types:
        Mapping ``{column_name: semantic_type}`` from the profiler, used by
        type-aware strategies. Pass ``None`` to skip semantic-driven auto-fixes.
    column_dtype_categories:
        Mapping ``{column_name: dtype_category}`` from the profiler.
    engine_config:
        Dict of boolean flags forwarded to the engine (e.g.
        ``{"auto_impute": True, "auto_cap_outliers": False}``).

    Returns
    -------
    healed_df : pd.DataFrame
    result    : HealingResult
    """
    engine = HealingEngine(config=engine_config)
    healed_df, transformation_log = engine.run(
        df,
        plan,
        column_semantic_types=column_semantic_types,
        column_dtype_categories=column_dtype_categories,
    )

    applied = sum(
        1 for e in transformation_log.entries if e.operation != "ERROR"
    )
    skipped = sum(
        1 for e in transformation_log.entries if e.operation == "ERROR"
    )

    result = HealingResult(
        dataset_id=plan.dataset_id,
        actions_applied=applied,
        actions_skipped=skipped,
        rows_modified=transformation_log.total_corrections,
        healed_dataset_path="",   # filled by pipeline after S3 upload
        transformation_log=transformation_log,
        # Backward-compat flat log for existing pipeline serialisation
        execution_log=[e.model_dump() for e in transformation_log.entries],
    )

    logger.info(
        "execute_plan_complete",
        dataset_id=str(plan.dataset_id),
        corrections=transformation_log.total_corrections,
        strategies=transformation_log.strategies_applied,
    )
    return healed_df, result


def execute_ai_plan(
    df: pd.DataFrame,
    agent_result,    # app.models.agent.AgentResult
    *,
    column_semantic_types: dict[str, str] | None = None,
    column_dtype_categories: dict[str, str] | None = None,
    engine_config: dict | None = None,
) -> tuple[pd.DataFrame, HealingResult]:
    """Apply an AI-agent healing plan to *df*.

    Delegates to :meth:`~app.modules.healing.engine.HealingEngine.run_ai`.
    Returns the same *(healed_df, HealingResult)* shape as :func:`execute_plan`
    so callers can swap functions without further changes.

    Parameters
    ----------
    df:
        DataFrame to heal.
    agent_result:
        An :class:`~app.models.agent.AgentResult` previously returned by
        :func:`~app.modules.ai.agent.run_agent`.
    column_semantic_types:
        Optional column→semantic-type mapping from the profiler.
    column_dtype_categories:
        Optional column→dtype-category mapping from the profiler.
    engine_config:
        Bool-flag dict forwarded to the engine
        (e.g. ``{"auto_impute": True}``).

    Returns
    -------
    healed_df : pd.DataFrame
    result    : HealingResult
    """
    engine = HealingEngine(config=engine_config)
    healed_df, transformation_log = engine.run_ai(
        df,
        agent_result.response,
        column_semantic_types=column_semantic_types,
        column_dtype_categories=column_dtype_categories,
        dataset_id=agent_result.dataset_id,
        model_used=agent_result.model_used,
    )

    applied = sum(
        1 for e in transformation_log.entries
        if e.operation not in ("ERROR", "SKIPPED")
    )
    skipped = sum(
        1 for e in transformation_log.entries
        if e.operation in ("ERROR", "SKIPPED")
    )
    ai_log = [e.model_dump() for e in transformation_log.ai_entries]

    result = HealingResult(
        dataset_id=agent_result.dataset_id,
        actions_applied=applied,
        actions_skipped=skipped,
        rows_modified=transformation_log.total_corrections,
        healed_dataset_path="",   # filled after S3 upload
        transformation_log=transformation_log,
        execution_log=[e.model_dump() for e in transformation_log.entries],
        ai_execution_log=ai_log,
        validation_warnings=[
            w
            for e in transformation_log.entries
            if e.operation == "VALIDATION"
            for w in e.validation_warnings
        ],
    )

    logger.info(
        "execute_ai_plan_complete",
        dataset_id=str(agent_result.dataset_id),
        total_corrections=transformation_log.total_corrections,
        ai_corrections=transformation_log.ai_total_corrections,
        strategies=transformation_log.strategies_applied,
    )
    return healed_df, result
