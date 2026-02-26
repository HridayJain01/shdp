from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from app.core.config import settings
from app.core.logging import setup_logging
from app.core.middleware import (
    ExceptionHandlerMiddleware,
    http_exception_handler,
    validation_exception_handler,
)
from app.db.session import init_db
from app.api.v1.routes import (
    anomalies,
    ai,
    heal,
    health,
    profile,
    quality,
    reports,
    upload,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    await init_db()
    yield
    # Cleanly close the Redis store connection pool on shutdown
    try:
        from app.core.redis_store import get_redis_store
        await get_redis_store().close()
    except Exception:
        pass


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.APP_NAME,
        version=settings.APP_VERSION,
        docs_url="/api/docs" if settings.DEBUG else None,
        redoc_url="/api/redoc" if settings.DEBUG else None,
        openapi_url="/api/openapi.json",
        description=(
            "Smart Healing Data Pipeline — upload, profile, detect anomalies, "
            "generate AI healing plans, apply healing, score quality, and download "
            "cleaned datasets."
        ),
        lifespan=lifespan,
    )

    # ── Exception handlers ──────────────────────────────────────────────────
    app.add_exception_handler(HTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)

    # ── Middleware (applied last-in, first-out) ─────────────────────────────
    app.add_middleware(ExceptionHandlerMiddleware)
    app.add_middleware(GZipMiddleware, minimum_size=1024)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[str(o) for o in settings.CORS_ORIGINS] or ["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── Routers ─────────────────────────────────────────────────────────────
    prefix = "/api/v1"

    # Core pipeline endpoints
    app.include_router(upload.router,    prefix=prefix, tags=["Upload"])
    app.include_router(profile.router,   prefix=prefix, tags=["Profile"])
    app.include_router(anomalies.router, prefix=prefix, tags=["Anomalies"])
    app.include_router(ai.router,        prefix=prefix, tags=["AI"])
    app.include_router(heal.router,      prefix=prefix, tags=["Healing"])
    app.include_router(quality.router,   prefix=prefix, tags=["Quality"])
    app.include_router(health.router,    prefix=prefix, tags=["Health"])
    app.include_router(reports.router,   prefix=prefix, tags=["Reports"])

    # ── Internal health probe (no auth, used by Docker / k8s) ──────────────
    @app.get("/healthz", tags=["Health"], include_in_schema=False)
    async def healthz() -> dict:
        return {"status": "ok", "version": settings.APP_VERSION}

    return app


app = create_app()

