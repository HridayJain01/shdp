"""POST /generate-ai-plan — call the AI agent to produce a healing strategy.

Takes the profiling + anomaly data from a completed pipeline job and calls
:func:`~app.modules.ai.agent.run_agent` synchronously (wrapped in a thread
so the async event loop is not blocked).
"""
from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status

from app.core.security import require_api_key
from app.models.responses import AIPlanResponse, GenerateAIPlanRequest

router = APIRouter()


@router.post(
    "/generate-ai-plan",
    response_model=AIPlanResponse,
    summary="Generate AI healing plan",
    description=(
        "Calls the OpenRouter language model with the dataset profile and "
        "anomaly report to produce a structured healing strategy. "
        "Requires a job_id from a successfully completed POST /upload."
    ),
)
async def generate_ai_plan(
    body: GenerateAIPlanRequest,
    _: str = Depends(require_api_key),
) -> AIPlanResponse:
    from app.tasks.worker import celery_app

    # ── Retrieve pipeline results ─────────────────────────────────────────
    result = celery_app.AsyncResult(body.job_id)
    if result.state == "FAILURE":
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Pipeline job failed: {result.info}",
        )
    if result.state != "SUCCESS":
        raise HTTPException(
            status_code=status.HTTP_202_ACCEPTED,
            detail={
                "message": "Pipeline not ready. Wait for SUCCESS state.",
                "state": result.state,
                "job_id": body.job_id,
            },
        )

    data: dict[str, Any] = result.result

    # ── Reconstruct Pydantic models ───────────────────────────────────────
    from app.models.profile import DatasetProfile
    from app.models.anomaly import AnomalyReport

    try:
        profile = DatasetProfile.model_validate(data["profile"])
        anomaly_report = AnomalyReport.model_validate(data["anomaly_report"])
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to deserialise pipeline result: {exc}",
        )

    import uuid
    dataset_id = uuid.UUID(data["dataset_id"])

    # ── Call agent in a thread (avoids blocking the event loop) ──────────
    from app.modules.ai.agent import run_agent
    from app.core.config import settings

    try:
        agent_result = await asyncio.to_thread(
            _run_agent_sync,
            profile=profile,
            anomaly_report=anomaly_report,
            dataset_id=dataset_id,
            model=body.model,
            temperature=body.temperature,
            max_tokens=body.max_tokens,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"AI agent call failed: {exc}",
        )

    return AIPlanResponse(
        dataset_id=dataset_id,
        job_id=body.job_id,
        model_used=agent_result.model_used,
        confidence_score=agent_result.response.confidence_score,
        step_count=len(agent_result.response.healing_plan),
        transformation_order=agent_result.response.transformation_order,
        agent_result=agent_result.model_dump(mode="json"),
    )


def _run_agent_sync(
    *,
    profile,
    anomaly_report,
    dataset_id,
    model: str,
    temperature: float,
    max_tokens: int,
):
    """Synchronous wrapper around the async run_agent for use with to_thread."""
    import asyncio
    from app.modules.ai.agent import run_agent

    return asyncio.run(
        run_agent(
            profile=profile,
            anomaly_report=anomaly_report,
            dataset_id=dataset_id,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
        )
    )
