"""POST /upload — ingest a dataset file and start the processing pipeline.

Flow
────
1. Read file bytes from the multipart upload.
2. Run parse + validate synchronously (fast, gives instant error feedback).
3. Persist raw bytes to Redis (consumed later by POST /apply-healing).
4. Dispatch the full Celery pipeline task.
5. Return dataset_id + job_id so the client can poll status.
"""
from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status

from app.core.redis_store import get_redis_store
from app.core.security import require_api_key
from app.models.responses import JobStatusResponse, UploadJobResponse
from app.modules.ingestion.exceptions import (
    CorruptRowsError,
    EmptyDatasetError,
    IngestionError,
    ParseError,
    SchemaError,
    UnsupportedFormatError,
    ValidationError,
)
from app.tasks.pipeline import run_full_pipeline

router = APIRouter()


@router.post(
    "/upload",
    response_model=UploadJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Upload a dataset file",
    description=(
        "Accepts CSV, JSON, or Excel uploads (≤ MAX_UPLOAD_MB). "
        "Returns a job_id for polling. Use GET /upload/{job_id}/status "
        "to track progress."
    ),
)
async def upload_dataset(
    file: UploadFile = File(..., description="CSV / JSON / XLSX dataset file"),
    _: str = Depends(require_api_key),
) -> UploadJobResponse:
    content = await file.read()
    size = len(content)
    filename = file.filename or "upload"

    # ── Pre-flight: parse + validate synchronously ───────────────────────
    try:
        from app.modules.ingestion.parser import parse
        from app.modules.ingestion.validator import validate

        parse_result = parse(content, filename)
        validate(parse_result.dataframe, size)
        df = parse_result.dataframe

    except UnsupportedFormatError as exc:
        raise HTTPException(status_code=415, detail=exc.to_dict())
    except (ParseError, EmptyDatasetError, SchemaError, CorruptRowsError, ValidationError) as exc:
        raise HTTPException(status_code=422, detail=exc.to_dict())
    except IngestionError as exc:
        raise HTTPException(status_code=400, detail=exc.to_dict())

    dataset_id = uuid.uuid4()

    # ── Persist raw bytes (for later /apply-healing calls) ────────────
    store = get_redis_store()
    await store.save_raw(dataset_id, content)

    # ── Dispatch Celery task ─────────────────────────────────────
    task = run_full_pipeline.delay(
        dataset_id=str(dataset_id),
        file_bytes_hex=content.hex(),
        filename=filename,
        file_size=size,
    )

    return UploadJobResponse(
        dataset_id=dataset_id,
        job_id=task.id,
        filename=filename,
        rows=len(df),
        columns=len(df.columns),
        size_bytes=size,
        status="pending",
        message="Pipeline started. Poll GET /upload/{job_id}/status for progress.",
    )


@router.get(
    "/upload/{job_id}/status",
    response_model=JobStatusResponse,
    summary="Poll pipeline job status",
)
async def get_job_status(
    job_id: str,
    _: str = Depends(require_api_key),
) -> JobStatusResponse:
    from app.tasks.worker import celery_app

    result = celery_app.AsyncResult(job_id)
    step: str | None = None
    if result.state == "PROGRESS" and isinstance(result.info, dict):
        step = result.info.get("step")

    return JobStatusResponse(
        job_id=job_id,
        state=result.state,
        step=step,
        error=str(result.info) if result.state == "FAILURE" else None,
    )


@router.get(
    "/upload/{job_id}/result",
    summary="Retrieve raw pipeline result",
    description="Returns the full Celery task result dict once the job succeeds.",
)
async def get_job_result(
    job_id: str,
    _: str = Depends(require_api_key),
) -> dict:
    from app.tasks.worker import celery_app

    result = celery_app.AsyncResult(job_id)
    if result.state != "SUCCESS":
        raise HTTPException(
            status_code=status.HTTP_202_ACCEPTED,
            detail=f"Job not complete. State: {result.state}",
        )
    return result.result
