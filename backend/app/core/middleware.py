"""Exception handling & request/response logging middleware.

Catches every unhandled exception and formats it as a JSON error envelope
matching :class:`~app.models.responses.APIResponse`.  HTTP exceptions raised
by route handlers preserve their original status code; all others produce 500.
"""
from __future__ import annotations

import time
import traceback
from datetime import datetime, timezone
from typing import Callable

from fastapi import Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response as StarletteResponse

from app.core.logging import get_logger

logger = get_logger(__name__)


# ── Exception handler middleware ──────────────────────────────────────────────

class ExceptionHandlerMiddleware(BaseHTTPMiddleware):
    """Catch-all middleware that formats unhandled exceptions as JSON."""

    async def dispatch(
        self, request: Request, call_next: Callable
    ) -> StarletteResponse:
        start = time.perf_counter()
        try:
            response = await call_next(request)
            elapsed_ms = (time.perf_counter() - start) * 1000
            logger.info(
                "http_request",
                method=request.method,
                path=request.url.path,
                status=response.status_code,
                elapsed_ms=round(elapsed_ms, 2),
            )
            return response

        except Exception as exc:
            elapsed_ms = (time.perf_counter() - start) * 1000
            logger.error(
                "unhandled_exception",
                method=request.method,
                path=request.url.path,
                error=str(exc),
                elapsed_ms=round(elapsed_ms, 2),
                exc_info=True,
            )
            return _error_response(
                status_code=500,
                code="INTERNAL_SERVER_ERROR",
                message="An unexpected error occurred.",
                detail=str(exc) if _is_debug(request) else None,
            )


# ── FastAPI exception handlers (registered on the app) ───────────────────────

async def http_exception_handler(request: Request, exc) -> JSONResponse:
    """Handler for ``fastapi.HTTPException``."""
    logger.warning(
        "http_exception",
        method=request.method,
        path=request.url.path,
        status=exc.status_code,
        detail=exc.detail,
    )
    return _error_response(
        status_code=exc.status_code,
        code=_status_to_code(exc.status_code),
        message=str(exc.detail) if isinstance(exc.detail, str) else "Request error.",
        detail=exc.detail if not isinstance(exc.detail, str) else None,
    )


async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """Handler for Pydantic request validation failures."""
    errors = exc.errors()
    logger.warning(
        "validation_error",
        method=request.method,
        path=request.url.path,
        errors=errors,
    )
    return _error_response(
        status_code=422,
        code="VALIDATION_ERROR",
        message="Request payload validation failed.",
        detail=errors,
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _error_response(
    status_code: int,
    code: str,
    message: str,
    detail=None,
) -> JSONResponse:
    body = {
        "success": False,
        "data": None,
        "error": {
            "code": code,
            "message": message,
            "detail": detail,
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return JSONResponse(status_code=status_code, content=body)


def _status_to_code(status: int) -> str:
    mapping = {
        400: "BAD_REQUEST",
        401: "UNAUTHORIZED",
        403: "FORBIDDEN",
        404: "NOT_FOUND",
        405: "METHOD_NOT_ALLOWED",
        409: "CONFLICT",
        415: "UNSUPPORTED_MEDIA_TYPE",
        422: "UNPROCESSABLE_ENTITY",
        429: "TOO_MANY_REQUESTS",
        500: "INTERNAL_SERVER_ERROR",
        502: "BAD_GATEWAY",
        503: "SERVICE_UNAVAILABLE",
    }
    return mapping.get(status, f"HTTP_{status}")


def _is_debug(request: Request) -> bool:
    try:
        from app.core.config import settings
        return settings.DEBUG
    except Exception:
        return False
