"""GET /profile — retrieve the dataset profile computed by the pipeline.

The profile is stored inside the Celery job result; this endpoint surfaces it
in a typed, documented response.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.core.security import require_api_key
from app.models.responses import ProfileResponse

router = APIRouter()


@router.get(
    "/profile",
    response_model=ProfileResponse,
    summary="Get dataset profile",
    description=(
        "Returns column-level statistics and inferred constraints. "
        "The job must be in SUCCESS state (use GET /upload/{job_id}/status to check)."
    ),
)
async def get_profile(
    job_id: str = Query(..., description="Celery job ID returned by POST /upload"),
    _: str = Depends(require_api_key),
) -> ProfileResponse:
    from app.tasks.worker import celery_app

    result = celery_app.AsyncResult(job_id)

    if result.state == "FAILURE":
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Pipeline job failed: {result.info}",
        )
    if result.state != "SUCCESS":
        raise HTTPException(
            status_code=status.HTTP_202_ACCEPTED,
            detail={
                "message": "Profile not ready yet.",
                "state": result.state,
                "job_id": job_id,
            },
        )

    data = result.result
    profile = data.get("profile", {})

    return ProfileResponse(
        dataset_id=data["dataset_id"],
        job_id=job_id,
        rows=profile.get("row_count", 0),
        columns=len(profile.get("columns", [])),
        profile=profile,
    )
