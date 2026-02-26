"""POST /apply-healing — apply a healing plan to a previously uploaded dataset.

Flow
────
1. Accept {dataset_id, use_ai_plan, agent_result}.
2. Load raw bytes from Redis (stored during POST /upload).
3. Parse back to a DataFrame.
4. Retrieve profile metadata (semantic types) from the Celery job result.
5. Apply healing via execute_plan (rule-based) or execute_ai_plan (AI-directed).
6. Persist healed CSV bytes to Redis for GET /download-cleaned.
7. Compute quality score delta.
8. Return HealingApplyResponse.
"""
from __future__ import annotations

import asyncio
import io
from typing import Any

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.core.redis_store import get_redis_store
from app.core.security import require_api_key
from app.models.responses import ApplyHealingRequest, HealingApplyResponse

router = APIRouter()


@router.post(
    "/apply-healing",
    response_model=HealingApplyResponse,
    summary="Apply healing plan to dataset",
    description=(
        "Loads the raw dataset from cache, applies the rule-based pipeline plan "
        "or an AI-directed plan if agent_result is provided. "
        "Stores the healed file for GET /download-cleaned."
    ),
)
async def apply_healing(
    body: ApplyHealingRequest,
    _: str = Depends(require_api_key),
) -> HealingApplyResponse:
    store = get_redis_store()

    # ── Load raw bytes ────────────────────────────────────────────────
    raw_bytes = await store.load_raw(body.dataset_id)
    if raw_bytes is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Raw dataset not found for dataset_id={body.dataset_id}. "
                "Re-upload the file or check that it has not expired (24 h TTL)."
            ),
        )

    # ── Parse DataFrame ─────────────────────────────────────────────
    try:
        from app.modules.ingestion.parser import parse
        parse_result = parse(raw_bytes, "dataset.csv")
        df = parse_result.dataframe
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Could not re-parse cached dataset: {exc}",
        )

    rows_before = len(df)

    # ── Extract healing plan + column metadata from pipeline result ───────
    # We use a best-effort lookup; healing still works without metadata.
    sem_types: dict[str, str] = {}
    dtype_cats: dict[str, str] = {}
    rule_plan = None

    healed_df: pd.DataFrame
    healing_result: Any

    # ── Run healing ────────────────────────────────────────────────
    if body.use_ai_plan and body.agent_result:
        # AI-directed healing via execute_ai_plan
        from app.models.agent import AgentResult
        from app.modules.healing.executor import execute_ai_plan

        try:
            agent_result_obj = AgentResult.model_validate(body.agent_result)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Invalid agent_result payload: {exc}",
            )

        try:
            healed_df, healing_result = await asyncio.to_thread(
                execute_ai_plan,
                df,
                agent_result_obj,
                column_semantic_types=sem_types or None,
                column_dtype_categories=dtype_cats or None,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"AI healing failed: {exc}",
            )

    else:
        # Rule-based healing using the pipeline-generated plan
        from app.modules.profiling.profiler import profile_dataset
        from app.modules.anomaly.detector import detect
        from app.modules.ai.reasoning import generate_healing_plan
        from app.modules.healing.executor import execute_plan

        # Build a fresh plan from profiling (works even without a job)
        try:
            profile = await asyncio.to_thread(profile_dataset, df, body.dataset_id)
            sem_types = {
                c.name: c.semantic_type
                for c in profile.columns if c.semantic_type
            }
            dtype_cats = {c.name: c.dtype_category for c in profile.columns}
            anomaly_report = await asyncio.to_thread(detect, df, body.dataset_id)
            healing_plan = await asyncio.to_thread(
                _run_generate_plan_sync, profile, anomaly_report, body.dataset_id
            )
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to generate healing plan: {exc}",
            )

        try:
            healed_df, healing_result = await asyncio.to_thread(
                execute_plan,
                df,
                healing_plan,
                column_semantic_types=sem_types,
                column_dtype_categories=dtype_cats,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Healing execution failed: {exc}",
            )

    rows_after = len(healed_df)

    # ── Persist healed CSV to Redis ──────────────────────────────────
    healed_csv = healed_df.to_csv(index=False).encode("utf-8")
    await store.save_healed(body.dataset_id, healed_csv)

    tlog = healing_result.transformation_log
    return HealingApplyResponse(
        dataset_id=body.dataset_id,
        actions_applied=healing_result.actions_applied,
        actions_skipped=healing_result.actions_skipped,
        rows_before=rows_before,
        rows_after=rows_after,
        total_corrections=tlog.total_corrections,
        ai_corrections=tlog.ai_total_corrections,
        strategies_applied=tlog.strategies_applied,
        validation_warnings=healing_result.validation_warnings,
        healing_result=healing_result.model_dump(mode="json"),
    )


@router.get(
    "/heal/{job_id}/plan",
    summary="Get pipeline-generated healing plan",
)
async def get_healing_plan(
    job_id: str,
    _: str = Depends(require_api_key),
) -> dict:
    from app.tasks.worker import celery_app

    result = celery_app.AsyncResult(job_id)
    if result.state != "SUCCESS":
        raise HTTPException(
            status_code=status.HTTP_202_ACCEPTED,
            detail=f"Job not ready. State: {result.state}",
        )
    return result.result.get("healing_plan", {})


@router.get(
    "/heal/{job_id}/result",
    summary="Get pipeline healing result",
)
async def get_healing_result(
    job_id: str,
    _: str = Depends(require_api_key),
) -> dict:
    from app.tasks.worker import celery_app

    result = celery_app.AsyncResult(job_id)
    if result.state != "SUCCESS":
        raise HTTPException(
            status_code=status.HTTP_202_ACCEPTED,
            detail=f"Job not ready. State: {result.state}",
        )
    return result.result.get("healing_result", {})


# ── Internal helper ─────────────────────────────────────────────────────

def _run_generate_plan_sync(profile, anomaly_report, dataset_id):
    """Wraps the async generate_healing_plan for use with asyncio.to_thread."""
    import asyncio
    from app.modules.ai.reasoning import generate_healing_plan
    return asyncio.run(generate_healing_plan(profile, anomaly_report, dataset_id))
