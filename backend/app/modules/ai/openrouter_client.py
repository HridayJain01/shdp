"""
Production-ready async OpenRouter Chat Completions client.

Features
--------
- Secure API key: never logged or exposed in tracebacks (masked to first-8 chars)
- Retry logic: exponential back-off with jitter for transient errors (429, 5xx,
  network/timeout faults); respects ``Retry-After`` header on 429 responses
- Split connect / read timeouts, fully configurable
- Model selection: per-call override with settings fallback
- JSON mode: passes ``response_format: {type: json_object}`` and validates the
  response is well-formed JSON before returning
- Schema validation: optional Pydantic ``BaseModel`` subclass; response dict is
  passed through ``model_cls.model_validate()`` — exceptions are surfaced as
  ``OpenRouterValidationError``
- Persistent ``httpx.AsyncClient`` connection pool via async context manager
- Module-level singleton + backward-compat free functions so existing call sites
  require zero changes

Exception hierarchy
-------------------
OpenRouterError          – base for all client errors
  OpenRouterAuthError    – 401 / 403 (bad or missing API key)
  OpenRouterRateLimitError – 429 that was not resolved after all retries
  OpenRouterHTTPError    – any other non-2xx (non-retryable: 400, 404, 422 …)
  OpenRouterTimeoutError – connect or read timeout after retries exhausted
  OpenRouterJSONError    – response content is not valid JSON
  OpenRouterValidationError – JSON did not match the requested Pydantic schema
"""
from __future__ import annotations

import asyncio
import json
import random
import re
import time
from dataclasses import dataclass, field
from typing import Any, Type

import httpx
from pydantic import BaseModel

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)


# ─── Exceptions ───────────────────────────────────────────────────────────────

class OpenRouterError(Exception):
    """Base exception for all OpenRouter client errors."""


class OpenRouterAuthError(OpenRouterError):
    """Raised on 401 / 403 — bad or revoked API key."""


class OpenRouterRateLimitError(OpenRouterError):
    """Raised when 429 rate-limit was not resolved after all retries."""


class OpenRouterHTTPError(OpenRouterError):
    """Raised for non-retryable HTTP errors (400, 404, 422, etc.)."""
    def __init__(self, status_code: int, body: str) -> None:
        super().__init__(f"HTTP {status_code}")
        self.status_code = status_code
        self.body = body


class OpenRouterTimeoutError(OpenRouterError):
    """Raised when connect or read timeout persists after all retries."""


class OpenRouterJSONError(OpenRouterError):
    """Raised when the model response is not valid JSON (json_mode only)."""


class OpenRouterValidationError(OpenRouterError):
    """Raised when the JSON response does not satisfy the Pydantic schema."""
    def __init__(self, message: str, raw: dict) -> None:
        super().__init__(message)
        self.raw = raw


# ─── Configuration dataclass ──────────────────────────────────────────────────

@dataclass
class ClientConfig:
    """All tuneable parameters for the client — all have safe defaults."""
    api_key: str = field(default_factory=lambda: settings.OPENROUTER_API_KEY)
    base_url: str = field(default_factory=lambda: settings.OPENROUTER_BASE_URL)
    default_model: str = field(default_factory=lambda: settings.OPENROUTER_MODEL)
    connect_timeout: float = field(
        default_factory=lambda: settings.OPENROUTER_CONNECT_TIMEOUT
    )
    read_timeout: float = field(
        default_factory=lambda: float(settings.OPENROUTER_TIMEOUT)
    )
    max_retries: int = field(
        default_factory=lambda: settings.OPENROUTER_MAX_RETRIES
    )
    retry_min_wait: float = field(
        default_factory=lambda: settings.OPENROUTER_RETRY_MIN_WAIT
    )
    retry_max_wait: float = field(
        default_factory=lambda: settings.OPENROUTER_RETRY_MAX_WAIT
    )
    site_url: str = "https://shdp.internal"
    site_name: str = "SHDP"

    @property
    def masked_key(self) -> str:
        """Return first-8 chars + '…' — safe for logging."""
        if len(self.api_key) <= 8:
            return "***"
        return self.api_key[:8] + "…"


# ─── Retry helpers ────────────────────────────────────────────────────────────

_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
_AUTH_STATUS      = frozenset({401, 403})


def _backoff(attempt: int, min_wait: float, max_wait: float) -> float:
    """Exponential backoff with full jitter: ``clamp(2^attempt * min_wait, max)``. """
    base = min(max_wait, min_wait * (2 ** attempt))
    return base + random.uniform(0.0, 0.5)  # ≤0.5 s jitter


def _retry_after(response: httpx.Response) -> float | None:
    """Parse ``Retry-After`` header (seconds integer or HTTP-date)."""
    header = response.headers.get("Retry-After")
    if header is None:
        return None
    try:
        return float(header)
    except ValueError:
        # HTTP-date format; compute delta from now
        try:
            from email.utils import parsedate_to_datetime
            target = parsedate_to_datetime(header).timestamp()
            delta = target - time.time()
            return max(0.0, delta)
        except Exception:
            return None


# ─── Core client ─────────────────────────────────────────────────────────────

class OpenRouterClient:
    """
    Async context-manager client for the OpenRouter Chat Completions API.

    Usage
    -----
    async with OpenRouterClient() as client:
        text   = await client.chat_completion(messages)
        data   = await client.json_completion(messages)
        result = await client.json_completion(messages, schema_type=MyModel)
    """

    def __init__(self, config: ClientConfig | None = None) -> None:
        self._cfg = config or ClientConfig()
        self._http: httpx.AsyncClient | None = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def __aenter__(self) -> "OpenRouterClient":
        self._http = httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=self._cfg.connect_timeout,
                read=self._cfg.read_timeout,
                write=self._cfg.read_timeout,
                pool=self._cfg.read_timeout,
            ),
            headers=self._auth_headers,
        )
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    # ── Auth headers ──────────────────────────────────────────────────────────

    @property
    def _auth_headers(self) -> dict[str, str]:
        if not self._cfg.api_key:
            raise OpenRouterAuthError(
                "OPENROUTER_API_KEY is not set. "
                "Add it to your .env file."
            )
        return {
            "Authorization": f"Bearer {self._cfg.api_key}",
            "HTTP-Referer": self._cfg.site_url,
            "X-Title": self._cfg.site_name,
            "Content-Type": "application/json",
        }

    # ── Low-level send with retry ─────────────────────────────────────────────

    async def _send(
        self,
        payload: dict[str, Any],
        *,
        request_id: str,
    ) -> dict[str, Any]:
        """POST to /chat/completions with retry logic. Returns parsed response dict."""
        client = self._http
        if client is None:
            raise RuntimeError(
                "OpenRouterClient must be used as an async context manager."
            )

        url = f"{self._cfg.base_url}/chat/completions"
        last_exc: Exception | None = None

        for attempt in range(self._cfg.max_retries + 1):
            try:
                logger.debug(
                    "openrouter_attempt",
                    attempt=attempt,
                    model=payload.get("model"),
                    request_id=request_id,
                    api_key_prefix=self._cfg.masked_key,
                )
                response = await client.post(url, json=payload)

            except httpx.ConnectTimeout as exc:
                last_exc = exc
                logger.warning(
                    "openrouter_connect_timeout",
                    attempt=attempt,
                    request_id=request_id,
                )
                await self._sleep_before_retry(attempt, None)
                continue

            except httpx.TimeoutException as exc:
                last_exc = exc
                logger.warning(
                    "openrouter_read_timeout",
                    attempt=attempt,
                    request_id=request_id,
                )
                await self._sleep_before_retry(attempt, None)
                continue

            except httpx.NetworkError as exc:
                last_exc = exc
                logger.warning(
                    "openrouter_network_error",
                    error=str(exc),
                    attempt=attempt,
                    request_id=request_id,
                )
                await self._sleep_before_retry(attempt, None)
                continue

            # ── HTTP response received ────────────────────────────────────────
            status = response.status_code

            if status in _AUTH_STATUS:
                # Never retry auth failures
                logger.error(
                    "openrouter_auth_error",
                    status=status,
                    request_id=request_id,
                    # NOTE: key not logged, only prefix
                    api_key_prefix=self._cfg.masked_key,
                )
                raise OpenRouterAuthError(
                    f"Authentication failed (HTTP {status}). "
                    f"Check OPENROUTER_API_KEY (prefix: {self._cfg.masked_key})."
                )

            if status == 429:
                wait = _retry_after(response)
                logger.warning(
                    "openrouter_rate_limit",
                    attempt=attempt,
                    retry_after=wait,
                    request_id=request_id,
                )
                await self._sleep_before_retry(attempt, wait)
                last_exc = OpenRouterRateLimitError(
                    f"Rate limited (HTTP 429) on attempt {attempt + 1}"
                )
                continue

            if status in _RETRYABLE_STATUS:
                body_preview = response.text[:300]
                logger.warning(
                    "openrouter_server_error",
                    status=status,
                    body_preview=body_preview,
                    attempt=attempt,
                    request_id=request_id,
                )
                last_exc = OpenRouterHTTPError(status, body_preview)
                await self._sleep_before_retry(attempt, None)
                continue

            if not response.is_success:
                # Non-retryable client error (400, 404, 422, …)
                body = response.text
                logger.error(
                    "openrouter_client_error",
                    status=status,
                    body_preview=body[:300],
                    request_id=request_id,
                )
                raise OpenRouterHTTPError(status, body)

            # ── Success ───────────────────────────────────────────────────────
            data: dict = response.json()
            logger.debug(
                "openrouter_success",
                model=data.get("model"),
                usage=data.get("usage"),
                request_id=request_id,
            )
            return data

        # All retries exhausted
        if isinstance(last_exc, (httpx.TimeoutException, httpx.ConnectTimeout)):
            raise OpenRouterTimeoutError(
                f"Request timed out after {self._cfg.max_retries + 1} attempts."
            ) from last_exc
        if last_exc is not None:
            raise last_exc
        raise OpenRouterError("Request failed for unknown reason.")

    async def _sleep_before_retry(
        self,
        attempt: int,
        explicit_wait: float | None,
    ) -> None:
        """Sleep before next retry; skips sleep after last attempt."""
        if attempt >= self._cfg.max_retries:
            return
        delay = (
            explicit_wait
            if explicit_wait is not None
            else _backoff(attempt, self._cfg.retry_min_wait, self._cfg.retry_max_wait)
        )
        logger.debug("openrouter_retry_wait", delay_s=round(delay, 2), attempt=attempt)
        await asyncio.sleep(delay)

    # ── Public API ────────────────────────────────────────────────────────────

    async def chat_completion(
        self,
        messages: list[dict[str, str]],
        *,
        model: str | None = None,
        temperature: float = 0.2,
        max_tokens: int = 4096,
        request_id: str = "",
    ) -> str:
        """
        Send a chat request and return the assistant message as a plain string.

        Parameters
        ----------
        messages:    OpenAI-style message list ``[{"role": …, "content": …}]``
        model:       Model identifier; falls back to ``config.default_model``
        temperature: Sampling temperature (0 = deterministic)
        max_tokens:  Maximum tokens in the response
        request_id:  Opaque string threaded through all log entries for tracing
        """
        payload: dict[str, Any] = {
            "model":       model or self._cfg.default_model,
            "messages":    messages,
            "temperature": temperature,
            "max_tokens":  max_tokens,
        }
        data = await self._send(payload, request_id=request_id)
        return self._extract_content(data)

    async def json_completion(
        self,
        messages: list[dict[str, str]],
        *,
        model: str | None = None,
        temperature: float = 0.2,
        max_tokens: int = 4096,
        schema_type: type[BaseModel] | None = None,
        request_id: str = "",
    ) -> dict[str, Any]:
        """
        Send a chat request with ``response_format: {type: json_object}`` and
        validate the result is well-formed JSON.

        Parameters
        ----------
        messages:    OpenAI-style message list
        model:       Override the default model
        temperature: Sampling temperature
        max_tokens:  Maximum response tokens
        schema_type: Optional Pydantic ``BaseModel`` subclass.
                     When provided the parsed dict is passed through
                     ``schema_type.model_validate()`` and the validated
                     model instance's ``model_dump()`` is returned.
                     ``OpenRouterValidationError`` is raised on mismatch.
        request_id:  Tracing string for log correlation

        Returns
        -------
        Validated dict (or ``schema_type.model_dump()`` when schema given).
        """
        payload: dict[str, Any] = {
            "model":           model or self._cfg.default_model,
            "messages":        messages,
            "temperature":     temperature,
            "max_tokens":      max_tokens,
            "response_format": {"type": "json_object"},
        }
        data = await self._send(payload, request_id=request_id)
        raw_content = self._extract_content(data)

        parsed = self._parse_json(raw_content, request_id=request_id)

        if schema_type is not None:
            return self._validate_schema(parsed, schema_type, request_id=request_id)

        return parsed

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _extract_content(data: dict[str, Any]) -> str:
        try:
            return str(data["choices"][0]["message"]["content"])
        except (KeyError, IndexError) as exc:
            raise OpenRouterError(
                f"Unexpected response structure (no choices[0].message.content): "
                f"{str(data)[:200]}"
            ) from exc

    @staticmethod
    def _parse_json(content: str, *, request_id: str = "") -> dict[str, Any]:
        """
        Parse JSON from the model response.  Handles two common pathologies:
          1. Model wraps JSON in a markdown ```json … ``` fence.
          2. Model adds explanatory text before/after the JSON object.
        """
        # Fast path
        stripped = content.strip()
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            pass

        # Strip markdown fences
        fence_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", stripped, re.DOTALL)
        if fence_match:
            try:
                return json.loads(fence_match.group(1))
            except json.JSONDecodeError:
                pass

        # Find the first top-level JSON object
        obj_match = re.search(r"\{.*\}", stripped, re.DOTALL)
        if obj_match:
            try:
                return json.loads(obj_match.group())
            except json.JSONDecodeError:
                pass

        logger.error(
            "openrouter_json_parse_failure",
            preview=stripped[:200],
            request_id=request_id,
        )
        raise OpenRouterJSONError(
            f"Model response is not valid JSON. Preview: {stripped[:200]}"
        )

    @staticmethod
    def _validate_schema(
        data: dict[str, Any],
        schema_type: type[BaseModel],
        *,
        request_id: str = "",
    ) -> dict[str, Any]:
        try:
            instance = schema_type.model_validate(data)
            return instance.model_dump()
        except Exception as exc:
            logger.error(
                "openrouter_schema_validation_failure",
                schema=schema_type.__name__,
                error=str(exc),
                request_id=request_id,
            )
            raise OpenRouterValidationError(
                f"Response did not match schema {schema_type.__name__}: {exc}",
                raw=data,
            ) from exc


# ─── Module-level singleton ───────────────────────────────────────────────────

class _Singleton:
    """Lazy-initialised OpenRouterClient kept open for the process lifetime."""
    _instance: OpenRouterClient | None = None

    @classmethod
    async def get(cls) -> OpenRouterClient:
        if cls._instance is None or cls._instance._http is None:
            client = OpenRouterClient()
            await client.__aenter__()
            cls._instance = client
        return cls._instance

    @classmethod
    async def close(cls) -> None:
        if cls._instance is not None:
            await cls._instance.__aexit__(None, None, None)
            cls._instance = None


# ─── Backward-compatible free functions ──────────────────────────────────────

async def chat_completion(
    messages: list[dict],
    model: str | None = None,
    temperature: float = 0.2,
    max_tokens: int = 4096,
    request_id: str = "",
) -> str:
    """
    Backward-compatible shim: send a chat request, return assistant content string.
    Delegates to the process-level :class:`OpenRouterClient` singleton.
    """
    client = await _Singleton.get()
    return await client.chat_completion(
        messages,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        request_id=request_id,
    )


async def json_completion(
    messages: list[dict],
    model: str | None = None,
    temperature: float = 0.2,
    max_tokens: int = 4096,
    schema_type: type[BaseModel] | None = None,
    request_id: str = "",
) -> dict[str, Any]:
    """
    Send a chat request with JSON mode enforced; optionally validate against a
    Pydantic schema. Delegates to the process-level :class:`OpenRouterClient`.
    """
    client = await _Singleton.get()
    return await client.json_completion(
        messages,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        schema_type=schema_type,
        request_id=request_id,
    )

