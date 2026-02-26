"""GET /health-metrics and GET /download-cleaned endpoints.

health-metrics — reports Redis, database, and Celery worker availability plus
                  basic process/platform stats.
download-cleaned — streams the healed CSV file stored by POST /apply-healing.
"""
from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse

from app.core.redis_store import get_redis_store
from app.core.security import require_api_key
from app.models.responses import HealthMetricsResponse, ServiceStatus

router = APIRouter()

# Track process start time for uptime calculation
_START_TIME = time.monotonic()


@router.get(
    "/health-metrics",
    response_model=HealthMetricsResponse,
    summary="System health metrics",
    description=(
        "Returns status of Redis, database, and Celery workers, plus "
        "basic process metrics (uptime, CPU count)."
    ),
)
async def get_health_metrics(
    _: str = Depends(require_api_key),
) -> HealthMetricsResponse:
    from app.core.config import settings

    services: list[ServiceStatus] = []
    active_workers = 0

    # ── Redis ─────────────────────────────────────────────────────────────
    store = get_redis_store()
    redis_latency = await store.ping()
    services.append(
        ServiceStatus(
            name="redis",
            status="ok" if redis_latency is not None else "down",
            latency_ms=redis_latency,
            detail=None if redis_latency is not None else "Ping failed",
        )
    )

    # ── Database ──────────────────────────────────────────────────────────
    db_status = await _check_database()
    services.append(db_status)

    # ── Celery workers ────────────────────────────────────────────────────
    try:
        from app.tasks.worker import celery_app
        inspector = celery_app.control.inspect(timeout=2.0)
        active = inspector.active()
        if active:
            active_workers = sum(len(v) for v in active.values())
            services.append(ServiceStatus(name="celery", status="ok", detail=f"{len(active)} worker(s) online"))
        else:
            services.append(ServiceStatus(name="celery", status="degraded", detail="No active workers detected"))
    except Exception as exc:
        services.append(ServiceStatus(name="celery", status="down", detail=str(exc)))

    # ── Derive overall health ─────────────────────────────────────────────
    statuses = {s.status for s in services}
    if "down" in statuses:
        overall = "unhealthy"
    elif "degraded" in statuses:
        overall = "degraded"
    else:
        overall = "healthy"

    return HealthMetricsResponse(
        status=overall,
        version=settings.APP_VERSION,
        uptime_seconds=round(time.monotonic() - _START_TIME, 1),
        services=services,
        active_workers=active_workers,
        cpu_count=os.cpu_count() or 1,
    )


@router.get(
    "/download-cleaned",
    summary="Download healed dataset as CSV",
    description=(
        "Streams the healed CSV produced by POST /apply-healing. "
        "The file is cached in Redis for 24 hours after healing."
    ),
    responses={
        200: {
            "content": {"text/csv": {}},
            "description": "Healed dataset as a CSV file.",
        }
    },
)
async def download_cleaned(
    dataset_id: str = Query(..., description="dataset_id returned by POST /upload"),
    _: str = Depends(require_api_key),
) -> StreamingResponse:
    import uuid as _uuid
    from uuid import UUID

    try:
        did = UUID(dataset_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid dataset_id: {dataset_id!r}",
        )

    store = get_redis_store()
    healed_bytes = await store.load_healed(did)

    if healed_bytes is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"No healed dataset found for dataset_id={dataset_id}. "
                "Run POST /apply-healing first, or check that it has not expired (24 h TTL)."
            ),
        )

    filename = f"healed_{dataset_id}.csv"

    return StreamingResponse(
        content=iter([healed_bytes]),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(len(healed_bytes)),
            "X-Dataset-Id": dataset_id,
        },
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _check_database() -> ServiceStatus:
    """Attempt an async DB ping; returns ServiceStatus regardless of outcome."""
    try:
        import time
        from sqlalchemy import text
        from app.db.session import AsyncSessionLocal

        t0 = time.perf_counter()
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
        latency_ms = round((time.perf_counter() - t0) * 1000, 2)
        return ServiceStatus(name="database", status="ok", latency_ms=latency_ms)
    except Exception as exc:
        return ServiceStatus(name="database", status="down", detail=str(exc))
