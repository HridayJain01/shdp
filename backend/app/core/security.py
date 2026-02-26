from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader
from app.core.config import settings

api_key_scheme = APIKeyHeader(name=settings.API_KEY_HEADER, auto_error=False)


async def require_api_key(api_key: str = Security(api_key_scheme)) -> str:
    """Validate API key. If no keys are configured, auth is disabled (dev mode)."""
    if not settings.ALLOWED_API_KEYS:
        return "dev"
    if api_key not in settings.ALLOWED_API_KEYS:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or missing API key.",
        )
    return api_key
