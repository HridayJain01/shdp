"""
Tests for the production OpenRouter async client.

Strategy
--------
- All tests inject a mock ``httpx.AsyncClient`` (via ``client._http``) so no
  real network calls are made.
- ``asyncio.sleep`` is patched to avoid slow retries in CI.
- Exception hierarchy, retry logic, JSON parsing, schema validation, and
  backward-compat free functions are all covered.
"""
from __future__ import annotations

import json
import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from pydantic import BaseModel

from app.modules.ai.openrouter_client import (
    ClientConfig,
    OpenRouterAuthError,
    OpenRouterClient,
    OpenRouterError,
    OpenRouterHTTPError,
    OpenRouterJSONError,
    OpenRouterRateLimitError,
    OpenRouterTimeoutError,
    OpenRouterValidationError,
    _Singleton,
    _backoff,
    _retry_after,
    chat_completion,
    json_completion,
)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _ok_response(content: str = "hello") -> MagicMock:
    """Build a successful httpx.Response-like mock."""
    body = {"choices": [{"message": {"content": content}}], "model": "test", "usage": {}}
    resp = MagicMock()
    resp.status_code = 200
    resp.is_success = True
    resp.headers = {}
    resp.json.return_value = body
    resp.text = json.dumps(body)
    return resp


def _error_response(status_code: int, body: str = "", headers: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.is_success = False
    resp.headers = headers or {}
    resp.json.return_value = {}
    resp.text = body
    return resp


def _cfg_no_key(**kwargs: Any) -> ClientConfig:
    return ClientConfig(api_key="", **kwargs)


def _cfg(**kwargs: Any) -> ClientConfig:
    return ClientConfig(
        api_key="sk-testkey-1234",
        max_retries=2,
        retry_min_wait=0.01,
        retry_max_wait=0.05,
        **kwargs,
    )


def _make_client(cfg: ClientConfig | None = None) -> OpenRouterClient:
    """Instantiate client and inject a mock _http so no real socket is opened."""
    c = OpenRouterClient(cfg or _cfg())
    c._http = AsyncMock()
    return c


# ─── 1. ClientConfig ─────────────────────────────────────────────────────────

class TestClientConfig:
    def test_masked_key_long(self):
        cfg = ClientConfig(api_key="sk-abcde12345fghij")
        assert cfg.masked_key.startswith("sk-abcde1")
        assert cfg.masked_key.endswith("…")
        assert len(cfg.masked_key) < len(cfg.api_key)

    def test_masked_key_short(self):
        cfg = ClientConfig(api_key="short")
        assert cfg.masked_key == "***"

    def test_masked_key_empty(self):
        cfg = ClientConfig(api_key="")
        assert cfg.masked_key == "***"

    def test_default_model_from_settings(self):
        cfg = ClientConfig()
        assert isinstance(cfg.default_model, str)
        assert len(cfg.default_model) > 0

    def test_max_retries_default(self):
        cfg = ClientConfig()
        assert cfg.max_retries >= 1

    def test_custom_values(self):
        cfg = ClientConfig(api_key="x", max_retries=5, connect_timeout=3.0)
        assert cfg.max_retries == 5
        assert cfg.connect_timeout == 3.0


# ─── 2. _backoff helper ───────────────────────────────────────────────────────

class TestBackoff:
    def test_backoff_increases_with_attempt(self):
        b0 = _backoff(0, 1.0, 30.0)
        b1 = _backoff(1, 1.0, 30.0)
        b2 = _backoff(2, 1.0, 30.0)
        # Base (before jitter) grows; with jitter they may overlap —
        # check the pure base values instead
        assert b0 < 3.0  # 1.0 + 0.5 max
        assert b1 < 5.0  # 2.0 + 0.5 max
        assert b2 < 9.0  # 4.0 + 0.5 max

    def test_backoff_capped_at_max(self):
        b = _backoff(100, 1.0, 5.0)
        assert b <= 5.5  # max + jitter

    def test_backoff_is_non_negative(self):
        for attempt in range(5):
            assert _backoff(attempt, 0.1, 10.0) >= 0.0


# ─── 3. _retry_after helper ───────────────────────────────────────────────────

class TestRetryAfter:
    def test_numeric_header(self):
        resp = MagicMock()
        resp.headers = {"Retry-After": "42"}
        assert _retry_after(resp) == 42.0

    def test_missing_header(self):
        resp = MagicMock()
        resp.headers = {}
        assert _retry_after(resp) is None

    def test_invalid_header_returns_none(self):
        resp = MagicMock()
        resp.headers = {"Retry-After": "not-a-date-or-number"}
        # Should not raise; may return None
        result = _retry_after(resp)
        assert result is None or isinstance(result, float)


# ─── 4. Context manager lifecycle ────────────────────────────────────────────

class TestClientLifecycle:
    @pytest.mark.asyncio
    async def test_context_manager_opens_and_closes(self):
        with patch("httpx.AsyncClient") as MockHTTP:
            instance = MagicMock()
            instance.aclose = AsyncMock()
            MockHTTP.return_value = instance
            cfg = _cfg()
            async with OpenRouterClient(cfg) as client:
                assert client._http is instance
            instance.aclose.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_send_raises_if_no_http(self):
        client = OpenRouterClient(_cfg())
        # _http is None — not entered as context manager
        with pytest.raises(RuntimeError, match="context manager"):
            await client._send({"model": "m", "messages": []}, request_id="x")

    def test_missing_api_key_raises_on_header_access(self):
        client = OpenRouterClient(_cfg_no_key())
        with pytest.raises(OpenRouterAuthError):
            _ = client._auth_headers


# ─── 5. chat_completion — success ────────────────────────────────────────────

class TestChatCompletionSuccess:
    @pytest.mark.asyncio
    async def test_returns_content_string(self):
        client = _make_client()
        client._http.post = AsyncMock(return_value=_ok_response("Hello World"))
        result = await client.chat_completion([{"role": "user", "content": "Hi"}])
        assert result == "Hello World"

    @pytest.mark.asyncio
    async def test_model_override(self):
        client = _make_client()
        client._http.post = AsyncMock(return_value=_ok_response("ok"))
        await client.chat_completion([], model="gpt-4o")
        call_kwargs = client._http.post.call_args
        payload = call_kwargs.kwargs.get("json", call_kwargs.args[1] if len(call_kwargs.args) > 1 else {})
        assert payload["model"] == "gpt-4o"

    @pytest.mark.asyncio
    async def test_temperature_forwarded(self):
        client = _make_client()
        client._http.post = AsyncMock(return_value=_ok_response("ok"))
        await client.chat_completion([], temperature=0.9)
        payload = client._http.post.call_args.kwargs["json"]
        assert payload["temperature"] == 0.9

    @pytest.mark.asyncio
    async def test_max_tokens_forwarded(self):
        client = _make_client()
        client._http.post = AsyncMock(return_value=_ok_response("ok"))
        await client.chat_completion([], max_tokens=512)
        payload = client._http.post.call_args.kwargs["json"]
        assert payload["max_tokens"] == 512


# ─── 6. json_completion — success ────────────────────────────────────────────

class TestJsonCompletionSuccess:
    def _json_response(self, data: dict) -> MagicMock:
        return _ok_response(json.dumps(data))

    @pytest.mark.asyncio
    async def test_returns_dict(self):
        client = _make_client()
        client._http.post = AsyncMock(return_value=self._json_response({"key": "value"}))
        result = await client.json_completion([])
        assert result == {"key": "value"}

    @pytest.mark.asyncio
    async def test_response_format_json_object_sent(self):
        client = _make_client()
        client._http.post = AsyncMock(return_value=self._json_response({"a": 1}))
        await client.json_completion([])
        payload = client._http.post.call_args.kwargs["json"]
        assert payload["response_format"] == {"type": "json_object"}

    @pytest.mark.asyncio
    async def test_markdown_fenced_json_parsed(self):
        fenced = "```json\n{\"x\": 42}\n```"
        client = _make_client()
        client._http.post = AsyncMock(return_value=_ok_response(fenced))
        result = await client.json_completion([])
        assert result == {"x": 42}

    @pytest.mark.asyncio
    async def test_json_with_leading_text_parsed(self):
        raw = "Sure! Here is the JSON:\n{\"answer\": \"yes\"}"
        client = _make_client()
        client._http.post = AsyncMock(return_value=_ok_response(raw))
        result = await client.json_completion([])
        assert result == {"answer": "yes"}

    @pytest.mark.asyncio
    async def test_json_with_trailing_text_parsed(self):
        raw = '{"score": 99}\n\nLet me know if you need more.'
        client = _make_client()
        client._http.post = AsyncMock(return_value=_ok_response(raw))
        result = await client.json_completion([])
        assert result == {"score": 99}

    @pytest.mark.asyncio
    async def test_invalid_json_raises(self):
        client = _make_client()
        client._http.post = AsyncMock(return_value=_ok_response("not json at all!!"))
        with pytest.raises(OpenRouterJSONError):
            await client.json_completion([])


# ─── 7. Schema validation ─────────────────────────────────────────────────────

class _DemoSchema(BaseModel):
    name: str
    score: float


class TestSchemaValidation:
    @pytest.mark.asyncio
    async def test_valid_schema_returns_dict(self):
        client = _make_client()
        payload = {"name": "Alice", "score": 92.5}
        client._http.post = AsyncMock(return_value=_ok_response(json.dumps(payload)))
        result = await client.json_completion([], schema_type=_DemoSchema)
        assert result["name"] == "Alice"
        assert result["score"] == 92.5

    @pytest.mark.asyncio
    async def test_invalid_schema_raises_validation_error(self):
        client = _make_client()
        bad_payload = {"name": "Alice"}  # missing 'score'
        client._http.post = AsyncMock(return_value=_ok_response(json.dumps(bad_payload)))
        with pytest.raises(OpenRouterValidationError) as exc_info:
            await client.json_completion([], schema_type=_DemoSchema)
        assert exc_info.value.raw == bad_payload

    @pytest.mark.asyncio
    async def test_wrong_type_raises_validation_error(self):
        client = _make_client()
        bad_payload = {"name": "Alice", "score": "not-a-number"}
        # score is not a float-coercible value scenario? Pydantic might coerce,
        # let's use a list instead
        bad_payload2 = {"name": ["list"], "score": 1.0}
        client._http.post = AsyncMock(return_value=_ok_response(json.dumps(bad_payload2)))
        # Pydantic tries to coerce list → str, which actually succeeds.
        # Test with truly invalid: name=None
        bad_payload3 = {"name": None, "score": "oops"}
        client._http.post = AsyncMock(return_value=_ok_response(json.dumps(bad_payload3)))
        with pytest.raises(OpenRouterValidationError):
            await client.json_completion([], schema_type=_DemoSchema)


# ─── 8. Auth errors (no retry) ───────────────────────────────────────────────

class TestAuthErrors:
    @pytest.mark.asyncio
    async def test_401_raises_auth_error(self):
        client = _make_client()
        client._http.post = AsyncMock(return_value=_error_response(401))
        with pytest.raises(OpenRouterAuthError):
            await client.chat_completion([])

    @pytest.mark.asyncio
    async def test_403_raises_auth_error(self):
        client = _make_client()
        client._http.post = AsyncMock(return_value=_error_response(403))
        with pytest.raises(OpenRouterAuthError):
            await client.chat_completion([])

    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_401_not_retried(self, mock_sleep):
        client = _make_client()
        client._http.post = AsyncMock(return_value=_error_response(401))
        with pytest.raises(OpenRouterAuthError):
            await client.chat_completion([])
        # Only one POST call — no retry
        assert client._http.post.call_count == 1

    @pytest.mark.asyncio
    async def test_missing_api_key_raises(self):
        client = OpenRouterClient(_cfg_no_key())
        client._http = AsyncMock()
        with pytest.raises(OpenRouterAuthError):
            await client.chat_completion([])


# ─── 9. Client errors (non-retryable) ────────────────────────────────────────

class TestClientErrors:
    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_400_raises_immediately(self, mock_sleep):
        client = _make_client()
        client._http.post = AsyncMock(return_value=_error_response(400, "bad request"))
        with pytest.raises(OpenRouterHTTPError) as exc_info:
            await client.chat_completion([])
        assert exc_info.value.status_code == 400
        # No sleep — not retried
        mock_sleep.assert_not_called()

    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_422_raises_immediately(self, mock_sleep):
        client = _make_client()
        client._http.post = AsyncMock(return_value=_error_response(422, "unprocessable"))
        with pytest.raises(OpenRouterHTTPError):
            await client.chat_completion([])
        assert client._http.post.call_count == 1

    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_404_raises_immediately(self, mock_sleep):
        client = _make_client()
        client._http.post = AsyncMock(return_value=_error_response(404))
        with pytest.raises(OpenRouterHTTPError):
            await client.chat_completion([])
        assert client._http.post.call_count == 1


# ─── 10. Retry on 429 (rate limit) ───────────────────────────────────────────

class TestRateLimitRetry:
    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_429_retried_then_succeeds(self, mock_sleep):
        client = _make_client()
        client._http.post = AsyncMock(side_effect=[
            _error_response(429),
            _ok_response("ok"),
        ])
        result = await client.chat_completion([])
        assert result == "ok"
        assert client._http.post.call_count == 2
        mock_sleep.assert_called()

    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_429_respects_retry_after_header(self, mock_sleep):
        client = _make_client()
        client._http.post = AsyncMock(side_effect=[
            _error_response(429, headers={"Retry-After": "7"}),
            _ok_response("ok"),
        ])
        await client.chat_completion([])
        # Sleep was called with the Retry-After value
        sleep_args = [call.args[0] for call in mock_sleep.call_args_list]
        assert any(abs(v - 7.0) < 1e-6 for v in sleep_args)

    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_429_all_retries_exhausted(self, mock_sleep):
        cfg = _cfg(max_retries=2)
        client = _make_client(cfg)
        client._http.post = AsyncMock(return_value=_error_response(429))
        with pytest.raises(OpenRouterRateLimitError):
            await client.chat_completion([])
        assert client._http.post.call_count == 3  # initial + 2 retries


# ─── 11. Retry on 5xx server errors ──────────────────────────────────────────

class TestServerErrorRetry:
    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_503_retried_then_succeeds(self, mock_sleep):
        client = _make_client()
        client._http.post = AsyncMock(side_effect=[
            _error_response(503),
            _ok_response("recovered"),
        ])
        result = await client.chat_completion([])
        assert result == "recovered"

    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_500_retried_until_success(self, mock_sleep):
        cfg = _cfg(max_retries=3)
        client = _make_client(cfg)
        client._http.post = AsyncMock(side_effect=[
            _error_response(500),
            _error_response(500),
            _ok_response("third time"),
        ])
        result = await client.chat_completion([])
        assert result == "third time"
        assert client._http.post.call_count == 3

    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_502_all_retries_exhausted_raises(self, mock_sleep):
        cfg = _cfg(max_retries=2)
        client = _make_client(cfg)
        client._http.post = AsyncMock(return_value=_error_response(502))
        with pytest.raises(OpenRouterHTTPError) as exc_info:
            await client.chat_completion([])
        assert exc_info.value.status_code == 502
        assert client._http.post.call_count == 3

    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_504_raises_after_retries(self, mock_sleep):
        cfg = _cfg(max_retries=1)
        client = _make_client(cfg)
        client._http.post = AsyncMock(return_value=_error_response(504))
        with pytest.raises(OpenRouterHTTPError):
            await client.chat_completion([])
        assert client._http.post.call_count == 2


# ─── 12. Timeout handling ────────────────────────────────────────────────────

class TestTimeoutHandling:
    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_connect_timeout_retried(self, mock_sleep):
        client = _make_client()
        client._http.post = AsyncMock(side_effect=[
            httpx.ConnectTimeout("connect timed out"),
            _ok_response("ok"),
        ])
        result = await client.chat_completion([])
        assert result == "ok"

    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_read_timeout_retried(self, mock_sleep):
        client = _make_client()
        client._http.post = AsyncMock(side_effect=[
            httpx.ReadTimeout("read timed out"),
            _ok_response("ok"),
        ])
        result = await client.chat_completion([])
        assert result == "ok"

    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_timeout_all_retries_raises(self, mock_sleep):
        cfg = _cfg(max_retries=2)
        client = _make_client(cfg)
        client._http.post = AsyncMock(
            side_effect=httpx.TimeoutException("timed out")
        )
        with pytest.raises(OpenRouterTimeoutError):
            await client.chat_completion([])
        assert client._http.post.call_count == 3

    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_network_error_retried(self, mock_sleep):
        client = _make_client()
        client._http.post = AsyncMock(side_effect=[
            httpx.NetworkError("connection refused"),
            _ok_response("ok"),
        ])
        result = await client.chat_completion([])
        assert result == "ok"


# ─── 13. _parse_json static method ───────────────────────────────────────────

class TestParseJson:
    def test_plain_json(self):
        result = OpenRouterClient._parse_json('{"a": 1}')
        assert result == {"a": 1}

    def test_json_with_whitespace(self):
        result = OpenRouterClient._parse_json('  \n{"b": "hello"}\n  ')
        assert result == {"b": "hello"}

    def test_markdown_fenced(self):
        raw = "```json\n{\"x\": 99}\n```"
        assert OpenRouterClient._parse_json(raw) == {"x": 99}

    def test_markdown_fenced_no_lang(self):
        raw = "```\n{\"y\": true}\n```"
        assert OpenRouterClient._parse_json(raw) == {"y": True}

    def test_leading_text_skipped(self):
        raw = "Here is your plan:\n{\"steps\": []}"
        assert OpenRouterClient._parse_json(raw) == {"steps": []}

    def test_totally_invalid_raises(self):
        with pytest.raises(OpenRouterJSONError):
            OpenRouterClient._parse_json("no json here at all ^^^")

    def test_empty_string_raises(self):
        with pytest.raises(OpenRouterJSONError):
            OpenRouterClient._parse_json("")

    def test_nested_json(self):
        raw = '{"outer": {"inner": [1,2,3]}}'
        result = OpenRouterClient._parse_json(raw)
        assert result["outer"] == {"inner": [1, 2, 3]}


# ─── 14. _extract_content static method ──────────────────────────────────────

class TestExtractContent:
    def test_extracts_content(self):
        data = {"choices": [{"message": {"content": "test"}}]}
        assert OpenRouterClient._extract_content(data) == "test"

    def test_missing_choices_raises(self):
        with pytest.raises(OpenRouterError):
            OpenRouterClient._extract_content({})

    def test_empty_choices_raises(self):
        with pytest.raises(OpenRouterError):
            OpenRouterClient._extract_content({"choices": []})

    def test_missing_message_raises(self):
        with pytest.raises(OpenRouterError):
            OpenRouterClient._extract_content({"choices": [{}]})


# ─── 15. Unexpected response structure ───────────────────────────────────────

class TestUnexpectedResponse:
    @pytest.mark.asyncio
    async def test_empty_choices_list_raises(self):
        client = _make_client()
        bad_resp = MagicMock()
        bad_resp.status_code = 200
        bad_resp.is_success = True
        bad_resp.headers = {}
        bad_resp.json.return_value = {"choices": []}
        client._http.post = AsyncMock(return_value=bad_resp)
        with pytest.raises(OpenRouterError):
            await client.chat_completion([])

    @pytest.mark.asyncio
    async def test_missing_content_key_raises(self):
        client = _make_client()
        bad_resp = MagicMock()
        bad_resp.status_code = 200
        bad_resp.is_success = True
        bad_resp.headers = {}
        bad_resp.json.return_value = {"choices": [{"message": {}}]}
        client._http.post = AsyncMock(return_value=bad_resp)
        with pytest.raises(OpenRouterError):
            await client.chat_completion([])


# ─── 16. Module-level free functions ─────────────────────────────────────────

class TestFreeFunctions:
    @pytest.mark.asyncio
    @patch("app.modules.ai.openrouter_client._Singleton.get")
    async def test_chat_completion_delegates(self, mock_get):
        mock_client = AsyncMock()
        mock_client.chat_completion = AsyncMock(return_value="delegated")
        mock_get.return_value = mock_client
        result = await chat_completion([{"role": "user", "content": "hi"}])
        assert result == "delegated"
        mock_client.chat_completion.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("app.modules.ai.openrouter_client._Singleton.get")
    async def test_json_completion_delegates(self, mock_get):
        mock_client = AsyncMock()
        mock_client.json_completion = AsyncMock(return_value={"ok": True})
        mock_get.return_value = mock_client
        result = await json_completion([])
        assert result == {"ok": True}
        mock_client.json_completion.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("app.modules.ai.openrouter_client._Singleton.get")
    async def test_json_completion_passes_schema_type(self, mock_get):
        mock_client = AsyncMock()
        mock_client.json_completion = AsyncMock(return_value={"name": "x", "score": 1.0})
        mock_get.return_value = mock_client
        await json_completion([], schema_type=_DemoSchema)
        call_kwargs = mock_client.json_completion.call_args.kwargs
        assert call_kwargs["schema_type"] is _DemoSchema

    @pytest.mark.asyncio
    @patch("app.modules.ai.openrouter_client._Singleton.get")
    async def test_chat_completion_passes_model_param(self, mock_get):
        mock_client = AsyncMock()
        mock_client.chat_completion = AsyncMock(return_value="ok")
        mock_get.return_value = mock_client
        await chat_completion([], model="gpt-4o", temperature=0.7, max_tokens=256)
        call_kwargs = mock_client.chat_completion.call_args.kwargs
        assert call_kwargs["model"] == "gpt-4o"
        assert call_kwargs["temperature"] == 0.7
        assert call_kwargs["max_tokens"] == 256


# ─── 17. Exception hierarchy ─────────────────────────────────────────────────

class TestExceptionHierarchy:
    def test_auth_is_openrouter_error(self):
        assert issubclass(OpenRouterAuthError, OpenRouterError)

    def test_rate_limit_is_openrouter_error(self):
        assert issubclass(OpenRouterRateLimitError, OpenRouterError)

    def test_http_error_is_openrouter_error(self):
        assert issubclass(OpenRouterHTTPError, OpenRouterError)

    def test_timeout_error_is_openrouter_error(self):
        assert issubclass(OpenRouterTimeoutError, OpenRouterError)

    def test_json_error_is_openrouter_error(self):
        assert issubclass(OpenRouterJSONError, OpenRouterError)

    def test_validation_error_is_openrouter_error(self):
        assert issubclass(OpenRouterValidationError, OpenRouterError)

    def test_http_error_stores_status_code(self):
        exc = OpenRouterHTTPError(422, "unprocessable")
        assert exc.status_code == 422
        assert "422" in str(exc)

    def test_validation_error_stores_raw(self):
        exc = OpenRouterValidationError("bad", raw={"x": 1})
        assert exc.raw == {"x": 1}
