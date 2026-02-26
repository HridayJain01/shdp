from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import AnyHttpUrl, field_validator
from typing import List


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # General
    APP_NAME: str = "SHDP"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    ENVIRONMENT: str = "production"

    # Security
    SECRET_KEY: str = "change-me-in-production"
    API_KEY_HEADER: str = "X-API-Key"
    ALLOWED_API_KEYS: List[str] = []

    # Database
    DATABASE_URL: str = "postgresql+asyncpg://shdp:shdp@localhost:5432/shdp"

    # Redis / Celery
    REDIS_URL: str = "redis://localhost:6379/0"
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/1"

    # Storage (S3-compatible)
    S3_ENDPOINT_URL: str = "http://localhost:9000"
    S3_ACCESS_KEY: str = "minioadmin"
    S3_SECRET_KEY: str = "minioadmin"
    S3_BUCKET_RAW: str = "shdp-raw"
    S3_BUCKET_HEALED: str = "shdp-healed"

    # OpenRouter
    OPENROUTER_API_KEY: str = ""
    OPENROUTER_BASE_URL: str = "https://openrouter.ai/api/v1"
    OPENROUTER_MODEL: str = "anthropic/claude-3.5-sonnet"
    OPENROUTER_TIMEOUT: int = 60           # total read timeout (seconds)
    OPENROUTER_CONNECT_TIMEOUT: float = 10.0
    OPENROUTER_MAX_RETRIES: int = 3
    OPENROUTER_RETRY_MIN_WAIT: float = 1.0
    OPENROUTER_RETRY_MAX_WAIT: float = 30.0

    # Processing limits
    MAX_UPLOAD_MB: int = 100
    MAX_ROWS: int = 2_000_000
    MAX_COLUMNS: int = 500

    # Scoring weights (must sum to 1.0)
    WEIGHT_COMPLETENESS: float = 0.30
    WEIGHT_VALIDITY: float = 0.25
    WEIGHT_UNIQUENESS: float = 0.20
    WEIGHT_CONSISTENCY: float = 0.15
    WEIGHT_TIMELINESS: float = 0.10

    # CORS
    CORS_ORIGINS: List[AnyHttpUrl] = []

    @field_validator("ALLOWED_API_KEYS", "CORS_ORIGINS", mode="before")
    @classmethod
    def parse_list(cls, v):
        if isinstance(v, str):
            return [i.strip() for i in v.split(",") if i.strip()]
        return v


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
