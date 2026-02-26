"""GET /quality-score — retrieve the quality delta computed by the pipeline."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.core.security import require_api_key
from app.models.responses import QualityScoreResponse

router = APIRouter()


@router.get(
    "/quality-score",
    response_model=QualityScoreResponse,
    summary="Get quality score delta",
    description=(
        "Returns the before/after quality scores and their delta. "
        "Pipeline must be in SUCCESS state."
    ),
)
async def get_quality_score(
    job_id: str = Query(..., description="Celery job ID returned by POST /upload"),
    _: str = Depends(require_api_key),
) -> QualityScoreResponse:
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
                "message": "Quality score not ready yet.",
                "state": result.state,
                "job_id": job_id,
            },
        )

    data = result.result
    delta = data.get("quality_delta", {})
    before = delta.get("before", {})
    after = delta.get("after", {})

    return QualityScoreResponse(
        dataset_id=data["dataset_id"],
        job_id=job_id,
        score_before=before.get("total_score", 0.0),
        score_after=after.get("total_score", 0.0),
        delta=delta.get("delta", 0.0),
        grade_before=before.get("grade", "?"),
        grade_after=after.get("grade", "?"),
        quality_delta=delta,
    )
