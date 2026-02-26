"""Before/after comparison and chart data endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from app.core.security import require_api_key

router = APIRouter()


@router.get(
    "/reports/{job_id}/comparison",
    summary="Get before/after column comparison",
)
async def get_comparison(
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
    return result.result.get("comparison", {})


@router.get(
    "/reports/{job_id}/charts",
    summary="Get chart data for visualisation",
)
async def get_charts(
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
    return result.result.get("charts", {})
