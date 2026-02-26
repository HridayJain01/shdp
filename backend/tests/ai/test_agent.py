"""
Tests for:
  - app/models/agent.py         — AgentResponse, HealingStep, ColumnFix,
                                   ValueCorrection, AgentResult
  - app/modules/ai/agent.py     — run_agent, _serialise_profile,
                                   _serialise_anomalies
  - app/modules/ai/prompts.py   — AGENT_SYSTEM, AGENT_USER presence

No real network calls are made; OpenRouterClient is fully mocked.
"""
from __future__ import annotations

import json
import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.models.agent import (
    AgentResponse,
    AgentResult,
    ColumnFix,
    HealingStep,
    ValueCorrection,
)
from app.models.anomaly import Anomaly, AnomalyReport, AnomalyType, Severity
from app.models.profile import (
    ColumnProfile,
    DatasetProfile,
    InferredConstraints,
    NumericStats,
    TopValue,
)
from app.modules.ai.agent import _serialise_anomalies, _serialise_profile, run_agent
from app.modules.ai.openrouter_client import (
    OpenRouterJSONError,
    OpenRouterValidationError,
)
from app.modules.ai.prompts import AGENT_SYSTEM, AGENT_USER


# ─── Fixtures / builders ──────────────────────────────────────────────────────

DATASET_ID = uuid.uuid4()


def _make_column(
    name: str = "col_a",
    dtype: str = "float64",
    null_pct: float = 5.0,
    unique_pct: float = 80.0,
    semantic_type: str | None = None,
    with_numeric: bool = False,
) -> ColumnProfile:
    ns = None
    if with_numeric:
        ns = NumericStats(
            min=0.0, max=100.0, mean=50.0, median=49.5, std=20.0,
            variance=400.0, q1=30.0, q3=70.0, iqr=40.0,
            skewness=0.1, kurtosis=-0.2, zeros=0, negatives=0, inf_count=0,
        )
    return ColumnProfile(
        name=name,
        position=0,
        dtype=dtype,
        dtype_category="numeric" if with_numeric else "categorical",
        semantic_type=semantic_type,
        null_count=int(null_pct),
        null_pct=null_pct,
        non_null_count=100 - int(null_pct),
        unique_count=int(unique_pct),
        unique_pct=unique_pct,
        numeric_stats=ns,
        top_values=[TopValue(value="x", count=10, pct=0.1)],
        constraints=InferredConstraints(
            is_nullable=null_pct > 0,
            is_unique=False,
            is_constant=False,
        ),
    )


def _make_profile(n_cols: int = 3) -> DatasetProfile:
    cols = [
        _make_column(f"col_{i}", with_numeric=(i % 2 == 0))
        for i in range(n_cols)
    ]
    return DatasetProfile(
        dataset_id=DATASET_ID,
        row_count=1000,
        column_count=n_cols,
        memory_mb=1.5,
        duplicate_rows=12,
        duplicate_pct=1.2,
        complete_rows=900,
        complete_row_pct=90.0,
        numeric_column_count=n_cols // 2,
        categorical_column_count=n_cols // 2,
        datetime_column_count=0,
        boolean_column_count=0,
        text_column_count=0,
        columns=cols,
    )


def _make_anomaly_report(n: int = 2) -> AnomalyReport:
    anomalies = [
        Anomaly(
            id=f"anom_{i}",
            column=f"col_{i}",
            anomaly_type=AnomalyType.MISSING_VALUES,
            severity=Severity.HIGH,
            affected_rows=20,
            affected_rate=0.02,
            description=f"20 missing values in col_{i}",
        )
        for i in range(n)
    ]
    return AnomalyReport(
        dataset_id=DATASET_ID,
        total_anomalies=n,
        critical_count=0,
        high_count=n,
        medium_count=0,
        low_count=0,
        anomalies=anomalies,
    )


def _valid_agent_dict() -> dict[str, Any]:
    """Return a dict that satisfies the AgentResponse schema."""
    return {
        "healing_plan": [
            {
                "step_id": "step_01",
                "title": "Impute missing values",
                "strategy": "median_imputation",
                "target_columns": ["col_0"],
                "rationale": "5% nulls in numeric column",
                "priority": 1,
                "estimated_impact": 0.15,
                "parameters": {},
            }
        ],
        "column_fixes": [
            {
                "column": "col_0",
                "detected_issue": "5% missing values",
                "fix_type": "impute",
                "target_dtype": "float64",
                "parameters": {},
                "severity": "high",
                "expected_null_reduction": 1.0,
            }
        ],
        "value_corrections": [
            {
                "column": "col_0",
                "original_value": None,
                "corrected_value": 50.0,
                "correction_type": "imputation",
                "reason": "median fill",
                "row_index": 7,
            }
        ],
        "transformation_order": ["deduplication", "median_imputation"],
        "confidence_score": 0.87,
    }


# ─── 1. HealingStep model ─────────────────────────────────────────────────────

class TestHealingStep:
    def test_valid_construction(self):
        s = HealingStep(
            step_id="s1", title="T", strategy="median_imputation",
            target_columns=["a"], rationale="r", priority=1,
            estimated_impact=0.5,
        )
        assert s.step_id == "s1"
        assert s.estimated_impact == 0.5

    def test_estimated_impact_clamped_above(self):
        s = HealingStep(
            step_id="s1", title="T", strategy="s",
            rationale="r", priority=1, estimated_impact=5.0,
        )
        assert s.estimated_impact == 1.0

    def test_estimated_impact_clamped_below(self):
        s = HealingStep(
            step_id="s1", title="T", strategy="s",
            rationale="r", priority=1, estimated_impact=-2.0,
        )
        assert s.estimated_impact == 0.0

    def test_priority_must_be_ge_1(self):
        with pytest.raises(Exception):
            HealingStep(
                step_id="s", title="T", strategy="s",
                rationale="r", priority=0, estimated_impact=0.1,
            )

    def test_default_parameters_empty(self):
        s = HealingStep(
            step_id="s", title="T", strategy="s",
            rationale="r", priority=1, estimated_impact=0.0,
        )
        assert s.parameters == {}

    def test_default_target_columns_empty(self):
        s = HealingStep(
            step_id="s", title="T", strategy="s",
            rationale="r", priority=1, estimated_impact=0.0,
        )
        assert s.target_columns == []


# ─── 2. ColumnFix model ───────────────────────────────────────────────────────

class TestColumnFix:
    def test_valid_construction(self):
        cf = ColumnFix(
            column="a", detected_issue="nulls", fix_type="impute",
        )
        assert cf.column == "a"
        assert cf.severity == "medium"

    def test_optional_fields_none(self):
        cf = ColumnFix(column="x", detected_issue="i", fix_type="f")
        assert cf.target_dtype is None
        assert cf.expected_null_reduction is None

    def test_expected_null_reduction_bounds(self):
        cf = ColumnFix(
            column="x", detected_issue="i", fix_type="f",
            expected_null_reduction=0.75,
        )
        assert cf.expected_null_reduction == 0.75

    def test_expected_null_reduction_out_of_bounds(self):
        with pytest.raises(Exception):
            ColumnFix(column="x", detected_issue="i", fix_type="f",
                      expected_null_reduction=1.5)


# ─── 3. ValueCorrection model ─────────────────────────────────────────────────

class TestValueCorrection:
    def test_valid_construction(self):
        vc = ValueCorrection(
            column="col_a",
            original_value=None,
            corrected_value=42.0,
            correction_type="imputation",
            reason="median",
        )
        assert vc.column == "col_a"
        assert vc.row_index is None

    def test_row_index_optional(self):
        vc = ValueCorrection(
            column="c", original_value="bad", corrected_value="good",
            correction_type="format_fix", reason="r", row_index=99,
        )
        assert vc.row_index == 99


# ─── 4. AgentResponse model ───────────────────────────────────────────────────

class TestAgentResponse:
    def test_valid_construction(self):
        r = AgentResponse.model_validate(_valid_agent_dict())
        assert len(r.healing_plan) == 1
        assert len(r.column_fixes) == 1
        assert len(r.value_corrections) == 1
        assert r.confidence_score == 0.87
        assert r.transformation_order == ["deduplication", "median_imputation"]

    def test_confidence_score_clamped(self):
        d = _valid_agent_dict()
        d["confidence_score"] = 99.0
        r = AgentResponse.model_validate(d)
        assert r.confidence_score == 1.0

    def test_confidence_score_clamped_below(self):
        d = _valid_agent_dict()
        d["confidence_score"] = -5.0
        r = AgentResponse.model_validate(d)
        assert r.confidence_score == 0.0

    def test_empty_healing_plan_invalid(self):
        d = _valid_agent_dict()
        d["healing_plan"] = []
        with pytest.raises(Exception, match="healing_plan"):
            AgentResponse.model_validate(d)

    def test_empty_transformation_order_invalid(self):
        d = _valid_agent_dict()
        d["transformation_order"] = []
        with pytest.raises(Exception, match="transformation_order"):
            AgentResponse.model_validate(d)

    def test_missing_required_field_raises(self):
        d = _valid_agent_dict()
        del d["column_fixes"]
        with pytest.raises(Exception):
            AgentResponse.model_validate(d)

    def test_summary_dict(self):
        r = AgentResponse.model_validate(_valid_agent_dict())
        s = r.summary()
        assert s["steps"] == 1
        assert s["columns_targeted"] == 1
        assert s["value_corrections"] == 1
        assert s["confidence_score"] == 0.87
        assert s["top_strategy"] == "median_imputation"
        assert s["transformation_order"] == ["deduplication", "median_imputation"]

    def test_model_dump_json_serialisable(self):
        r = AgentResponse.model_validate(_valid_agent_dict())
        dumped = r.model_dump()
        # Must be JSON serialisable
        assert json.dumps(dumped)

    def test_value_corrections_can_be_empty(self):
        d = _valid_agent_dict()
        d["value_corrections"] = []
        r = AgentResponse.model_validate(d)
        assert r.value_corrections == []

    def test_column_fixes_can_be_empty(self):
        d = _valid_agent_dict()
        d["column_fixes"] = []
        r = AgentResponse.model_validate(d)
        assert r.column_fixes == []


# ─── 5. AgentResult model ────────────────────────────────────────────────────

class TestAgentResult:
    def test_valid_construction(self):
        result = AgentResult(
            dataset_id=DATASET_ID,
            model_used="test-model",
            response=AgentResponse.model_validate(_valid_agent_dict()),
        )
        assert result.model_used == "test-model"
        assert result.dataset_id == DATASET_ID
        assert result.latency_ms is None
        assert result.ran_at is not None

    def test_with_token_counts(self):
        result = AgentResult(
            dataset_id=DATASET_ID,
            model_used="m",
            response=AgentResponse.model_validate(_valid_agent_dict()),
            prompt_tokens=512,
            completion_tokens=256,
            latency_ms=1234.5,
        )
        assert result.prompt_tokens == 512
        assert result.completion_tokens == 256
        assert result.latency_ms == 1234.5


# ─── 6. _serialise_profile ───────────────────────────────────────────────────

class TestSerialiseProfile:
    def test_returns_valid_json(self):
        p = _make_profile(3)
        s = _serialise_profile(p)
        data = json.loads(s)
        assert isinstance(data, dict)

    def test_contains_summary_fields(self):
        p = _make_profile(2)
        data = json.loads(_serialise_profile(p))
        assert data["row_count"] == 1000
        assert data["column_count"] == 2
        assert data["duplicate_rows"] == 12
        assert "columns" in data

    def test_column_entries_have_required_fields(self):
        p = _make_profile(2)
        data = json.loads(_serialise_profile(p))
        for col in data["columns"]:
            assert "name" in col
            assert "dtype" in col
            assert "null_pct" in col
            assert "unique_pct" in col

    def test_numeric_stats_included_when_present(self):
        p = _make_profile(1)
        # col_0 has with_numeric=True based on position % 2
        data = json.loads(_serialise_profile(p))
        numeric_cols = [c for c in data["columns"] if "numeric" in c]
        assert len(numeric_cols) >= 1
        assert "mean" in numeric_cols[0]["numeric"]

    def test_columns_capped_at_max(self):
        p = _make_profile(50)
        data = json.loads(_serialise_profile(p))
        assert len(data["columns"]) <= 40
        assert "columns_truncated" in data

    def test_top_values_included(self):
        p = _make_profile(1)
        data = json.loads(_serialise_profile(p))
        col = data["columns"][0]
        assert "top_values" in col

    def test_empty_profile(self):
        p = _make_profile(0)
        data = json.loads(_serialise_profile(p))
        assert data["columns"] == []


# ─── 7. _serialise_anomalies ─────────────────────────────────────────────────

class TestSerialiseAnomalies:
    def test_returns_valid_json(self):
        r = _make_anomaly_report(3)
        s = _serialise_anomalies(r)
        data = json.loads(s)
        assert isinstance(data, dict)

    def test_contains_summary_fields(self):
        r = _make_anomaly_report(2)
        data = json.loads(_serialise_anomalies(r))
        assert data["total_anomalies"] == 2
        assert data["high"] == 2
        assert "anomalies" in data

    def test_anomaly_entries_have_required_fields(self):
        r = _make_anomaly_report(2)
        data = json.loads(_serialise_anomalies(r))
        for a in data["anomalies"]:
            assert "id" in a
            assert "type" in a
            assert "severity" in a
            assert "affected_rows" in a
            assert "description" in a

    def test_anomalies_capped_at_max(self):
        r = _make_anomaly_report(30)
        data = json.loads(_serialise_anomalies(r))
        assert len(data["anomalies"]) <= 25
        assert "anomalies_truncated" in data

    def test_empty_report(self):
        r = _make_anomaly_report(0)
        data = json.loads(_serialise_anomalies(r))
        assert data["anomalies"] == []


# ─── 8. Prompts ───────────────────────────────────────────────────────────────

class TestPrompts:
    def test_agent_system_nonempty(self):
        assert len(AGENT_SYSTEM.strip()) > 100

    def test_agent_system_instructs_json(self):
        assert "JSON" in AGENT_SYSTEM

    def test_agent_system_mentions_senior(self):
        assert "senior" in AGENT_SYSTEM.lower()

    def test_agent_user_has_placeholders(self):
        assert "{profile_json}" in AGENT_USER
        assert "{anomaly_json}" in AGENT_USER

    def test_agent_user_mentions_all_schema_keys(self):
        for key in ("healing_plan", "column_fixes", "value_corrections",
                    "transformation_order", "confidence_score"):
            assert key in AGENT_USER

    def test_agent_user_format_succeeds(self):
        rendered = AGENT_USER.format(
            profile_json='{"row_count": 100}',
            anomaly_json='{"total_anomalies": 0}',
        )
        assert "100" in rendered
        assert "{" not in rendered.split("schema")[0]  # no un-substituted braces

    def test_agent_user_available_strategies_listed(self):
        assert "deduplication" in AGENT_USER
        assert "median_imputation" in AGENT_USER


# ─── 9. run_agent — success ───────────────────────────────────────────────────

class TestRunAgentSuccess:
    @pytest.mark.asyncio
    @patch("app.modules.ai.agent.OpenRouterClient")
    async def test_returns_agent_result(self, MockClient):
        _setup_mock_client(MockClient, _valid_agent_dict())
        result = await run_agent(_make_profile(), _make_anomaly_report(), DATASET_ID)
        assert isinstance(result, AgentResult)

    @pytest.mark.asyncio
    @patch("app.modules.ai.agent.OpenRouterClient")
    async def test_response_is_agent_response(self, MockClient):
        _setup_mock_client(MockClient, _valid_agent_dict())
        result = await run_agent(_make_profile(), _make_anomaly_report(), DATASET_ID)
        assert isinstance(result.response, AgentResponse)

    @pytest.mark.asyncio
    @patch("app.modules.ai.agent.OpenRouterClient")
    async def test_dataset_id_propagated(self, MockClient):
        _setup_mock_client(MockClient, _valid_agent_dict())
        result = await run_agent(_make_profile(), _make_anomaly_report(), DATASET_ID)
        assert result.dataset_id == DATASET_ID

    @pytest.mark.asyncio
    @patch("app.modules.ai.agent.OpenRouterClient")
    async def test_model_default_from_settings(self, MockClient):
        _setup_mock_client(MockClient, _valid_agent_dict())
        result = await run_agent(_make_profile(), _make_anomaly_report(), DATASET_ID)
        assert isinstance(result.model_used, str)
        assert len(result.model_used) > 0

    @pytest.mark.asyncio
    @patch("app.modules.ai.agent.OpenRouterClient")
    async def test_model_override(self, MockClient):
        mock_instance = _setup_mock_client(MockClient, _valid_agent_dict())
        await run_agent(
            _make_profile(), _make_anomaly_report(), DATASET_ID,
            model="gpt-4o",
        )
        call_kwargs = mock_instance.json_completion.call_args.kwargs
        assert call_kwargs["model"] == "gpt-4o"

    @pytest.mark.asyncio
    @patch("app.modules.ai.agent.OpenRouterClient")
    async def test_schema_type_passed_to_client(self, MockClient):
        mock_instance = _setup_mock_client(MockClient, _valid_agent_dict())
        await run_agent(_make_profile(), _make_anomaly_report(), DATASET_ID)
        call_kwargs = mock_instance.json_completion.call_args.kwargs
        assert call_kwargs["schema_type"] is AgentResponse

    @pytest.mark.asyncio
    @patch("app.modules.ai.agent.OpenRouterClient")
    async def test_latency_ms_populated(self, MockClient):
        _setup_mock_client(MockClient, _valid_agent_dict())
        result = await run_agent(_make_profile(), _make_anomaly_report(), DATASET_ID)
        assert result.latency_ms is not None
        assert result.latency_ms >= 0.0

    @pytest.mark.asyncio
    @patch("app.modules.ai.agent.OpenRouterClient")
    async def test_healing_plan_populated(self, MockClient):
        _setup_mock_client(MockClient, _valid_agent_dict())
        result = await run_agent(_make_profile(), _make_anomaly_report(), DATASET_ID)
        assert len(result.response.healing_plan) == 1
        assert result.response.healing_plan[0].strategy == "median_imputation"

    @pytest.mark.asyncio
    @patch("app.modules.ai.agent.OpenRouterClient")
    async def test_column_fixes_populated(self, MockClient):
        _setup_mock_client(MockClient, _valid_agent_dict())
        result = await run_agent(_make_profile(), _make_anomaly_report(), DATASET_ID)
        assert result.response.column_fixes[0].column == "col_0"

    @pytest.mark.asyncio
    @patch("app.modules.ai.agent.OpenRouterClient")
    async def test_confidence_score_in_range(self, MockClient):
        _setup_mock_client(MockClient, _valid_agent_dict())
        result = await run_agent(_make_profile(), _make_anomaly_report(), DATASET_ID)
        assert 0.0 <= result.response.confidence_score <= 1.0

    @pytest.mark.asyncio
    @patch("app.modules.ai.agent.OpenRouterClient")
    async def test_messages_sent_to_client(self, MockClient):
        mock_instance = _setup_mock_client(MockClient, _valid_agent_dict())
        await run_agent(_make_profile(), _make_anomaly_report(), DATASET_ID)
        messages = mock_instance.json_completion.call_args.args[0]
        roles = [m["role"] for m in messages]
        assert roles[0] == "system"
        assert roles[1] == "user"

    @pytest.mark.asyncio
    @patch("app.modules.ai.agent.OpenRouterClient")
    async def test_system_message_content(self, MockClient):
        mock_instance = _setup_mock_client(MockClient, _valid_agent_dict())
        await run_agent(_make_profile(), _make_anomaly_report(), DATASET_ID)
        messages = mock_instance.json_completion.call_args.args[0]
        assert "senior data quality" in messages[0]["content"].lower()

    @pytest.mark.asyncio
    @patch("app.modules.ai.agent.OpenRouterClient")
    async def test_user_message_contains_profile_data(self, MockClient):
        mock_instance = _setup_mock_client(MockClient, _valid_agent_dict())
        profile = _make_profile(2)
        await run_agent(profile, _make_anomaly_report(), DATASET_ID)
        messages = mock_instance.json_completion.call_args.args[0]
        user_content = messages[1]["content"]
        assert "row_count" in user_content

    @pytest.mark.asyncio
    @patch("app.modules.ai.agent.OpenRouterClient")
    async def test_user_message_contains_anomaly_data(self, MockClient):
        mock_instance = _setup_mock_client(MockClient, _valid_agent_dict())
        await run_agent(_make_profile(), _make_anomaly_report(3), DATASET_ID)
        messages = mock_instance.json_completion.call_args.args[0]
        user_content = messages[1]["content"]
        assert "total_anomalies" in user_content


# ─── 10. run_agent — error propagation ────────────────────────────────────────

class TestRunAgentErrors:
    @pytest.mark.asyncio
    @patch("app.modules.ai.agent.OpenRouterClient")
    async def test_json_error_propagated(self, MockClient):
        _setup_mock_client_raises(MockClient, OpenRouterJSONError("bad json"))
        with pytest.raises(OpenRouterJSONError):
            await run_agent(_make_profile(), _make_anomaly_report(), DATASET_ID)

    @pytest.mark.asyncio
    @patch("app.modules.ai.agent.OpenRouterClient")
    async def test_validation_error_propagated(self, MockClient):
        _setup_mock_client_raises(
            MockClient,
            OpenRouterValidationError("schema mismatch", raw={"bad": "data"}),
        )
        with pytest.raises(OpenRouterValidationError):
            await run_agent(_make_profile(), _make_anomaly_report(), DATASET_ID)

    @pytest.mark.asyncio
    @patch("app.modules.ai.agent.OpenRouterClient")
    async def test_openrouter_error_propagated(self, MockClient):
        from app.modules.ai.openrouter_client import OpenRouterError
        _setup_mock_client_raises(MockClient, OpenRouterError("network down"))
        with pytest.raises(OpenRouterError):
            await run_agent(_make_profile(), _make_anomaly_report(), DATASET_ID)


# ─── Helpers used by run_agent tests ──────────────────────────────────────────

def _setup_mock_client(MockClient: MagicMock, response_dict: dict) -> MagicMock:
    """
    Configures MockClient to behave as an async context manager whose
    json_completion() returns response_dict (a valid AgentResponse model_dump).
    Returns the mock instance for further assertions.
    """
    mock_instance = AsyncMock()
    mock_instance.json_completion = AsyncMock(
        return_value=AgentResponse.model_validate(response_dict).model_dump()
    )
    # __aenter__ returns the mock instance; __aexit__ is a no-op
    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_instance)
    mock_cm.__aexit__ = AsyncMock(return_value=False)
    MockClient.return_value = mock_cm
    return mock_instance


def _setup_mock_client_raises(MockClient: MagicMock, exc: Exception) -> None:
    mock_instance = AsyncMock()
    mock_instance.json_completion = AsyncMock(side_effect=exc)
    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_instance)
    mock_cm.__aexit__ = AsyncMock(return_value=False)
    MockClient.return_value = mock_cm
