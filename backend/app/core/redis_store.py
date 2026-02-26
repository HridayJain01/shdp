"""Async Redis byte-store for raw + healed dataset blobs.

Keys
────
    raw:{dataset_id}    — original uploaded file bytes (CSV/JSON/XLSX)
    healed:{dataset_id} — healed DataFrame serialised as UTF-8 CSV

Both keys expire after :data:`TTL_SECONDS` (24 hours by default).

Usage
─────
    store = get_redis_store()
    await store.save_raw(dataset_id, file_bytes)
    raw_bytes = await store.load_raw(dataset_id)

    await store.save_healed(dataset_id, healed_csv_bytes)
    healed_bytes = await store.load_healed(dataset_id)
"""
from __future__ import annotations

import functools
from uuid import UUID

import redis.asyncio as aioredis

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

# 24 hours
TTL_SECONDS: int = 86_400


class RedisStore:
    """Thin async wrapper around a Redis connection for blob storage."""

    def __init__(self, url: str) -> None:
        self._url = url
        self._client: aioredis.Redis | None = None

    async def _get_client(self) -> aioredis.Redis:
        if self._client is None:
            self._client = aioredis.from_url(
                self._url,
                encoding="utf-8",
                decode_responses=False,  # keep bytes
            )
        return self._client

    # ── Raw dataset ───────────────────────────────────────────────────────

    async def save_raw(self, dataset_id: UUID, data: bytes) -> None:
        client = await self._get_client()
        key = f"raw:{dataset_id}"
        await client.setex(key, TTL_SECONDS, data)
        logger.debug("redis_store_raw_saved", key=key, size=len(data))

    async def load_raw(self, dataset_id: UUID) -> bytes | None:
        client = await self._get_client()
        key = f"raw:{dataset_id}"
        data = await client.get(key)
        if data is None:
            logger.warning("redis_store_raw_miss", key=key)
        return data

    # ── Healed dataset ────────────────────────────────────────────────────

    async def save_healed(self, dataset_id: UUID, data: bytes) -> None:
        client = await self._get_client()
        key = f"healed:{dataset_id}"
        await client.setex(key, TTL_SECONDS, data)
        logger.debug("redis_store_healed_saved", key=key, size=len(data))

    async def load_healed(self, dataset_id: UUID) -> bytes | None:
        client = await self._get_client()
        key = f"healed:{dataset_id}"
        data = await client.get(key)
        if data is None:
            logger.warning("redis_store_healed_miss", key=key)
        return data

    # ── Utility ───────────────────────────────────────────────────────────

    async def ping(self) -> float | None:
        """Return round-trip latency in ms, or None on failure."""
        import time
        try:
            client = await self._get_client()
            t0 = time.perf_counter()
            await client.ping()
            return round((time.perf_counter() - t0) * 1000, 2)
        except Exception as exc:
            logger.error("redis_ping_failed", error=str(exc))
            return None

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None


@functools.lru_cache(maxsize=1)
def get_redis_store() -> RedisStore:
    """Return the singleton :class:`RedisStore` instance."""
    return RedisStore(settings.REDIS_URL)
