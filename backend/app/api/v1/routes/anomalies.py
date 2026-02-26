"""GET /anomalies — retrieve the anomaly report for an uploaded dataset.

Anomaly detection runs as part of the standard pipeline (step 3).  This
endpoint surfaces the stored report from the Celery result.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.core.security import require_api_key
from app.models.responses import AnomalyResponse

router = APIRouter()


@router.get(
    "/anomalies",
    response_model=AnomalyResponse,
    summary="Get anomaly detection report",
    description=(
        "Returns detected anomalies (missing values, outliers, type mismatches, "
        "duplicates, format violations) with severity ratings. "
        "Pipeline must be in SUCCESS state."
    ),
)
async def get_anomalies(
    job_id: str = Query(..., description="Celery job ID returned by POST /upload"),
    _: str = Depends(require_api_key),
) -> AnomalyResponse:
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
                "message": "Anomaly report not ready yet.",
                "state": result.state,
                "job_id": job_id,
            },
        )

    data = result.result
    report = data.get("anomaly_report", {})

    return AnomalyResponse(
        dataset_id=data["dataset_id"],
        job_id=job_id,
        total_anomalies=report.get("total_anomalies", 0),
        critical_count=report.get("critical_count", 0),
        high_count=report.get("high_count", 0),
        anomaly_report=report,
    )
