"""Build LLM context from profile + anomaly report and parse the healing plan."""
from __future__ import annotations

import uuid

from app.models.anomaly import AnomalyReport
from app.models.healing import HealingAction, HealingPlan, HealingStrategy
from app.models.profile import DatasetProfile
from app.modules.ai.openrouter_client import json_completion
from app.modules.ai.prompts import HEALING_PLAN_SYSTEM, HEALING_PLAN_USER
from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)


def _summarise_profile(profile: DatasetProfile) -> str:
    lines = [
        f"rows={profile.row_count}, columns={profile.column_count}, "
        f"memory={profile.memory_mb}MB, duplicates={profile.duplicate_rows}"
    ]
    for col in profile.columns[:30]:   # cap at 30 to avoid huge prompts
        ns = col.numeric_stats
        lines.append(
            f"  {col.name}: dtype={col.dtype}, semantic={col.semantic_type}, "
            f"null_pct={col.null_pct:.1f}%, unique_pct={col.unique_pct:.1f}%"
            + (f", mean={ns.mean:.3g}, std={ns.std:.3g}" if ns is not None else "")
        )
    return "\n".join(lines)


def _summarise_anomalies(report: AnomalyReport) -> str:
    lines = []
    for a in report.anomalies[:20]:    # cap at 20 anomalies
        lines.append(
            f"- [{a.severity.value.upper()}] {a.anomaly_type.value} | "
            f"col={a.column or 'row-level'} | "
            f"affected={a.affected_rate:.1%} | {a.description}"
        )
    return "\n".join(lines) or "No anomalies detected."


async def generate_healing_plan(
    profile: DatasetProfile,
    anomaly_report: AnomalyReport,
    dataset_id: uuid.UUID,
) -> HealingPlan:
    profile_summary = _summarise_profile(profile)
    anomaly_list = _summarise_anomalies(anomaly_report)

    messages = [
        {"role": "system", "content": HEALING_PLAN_SYSTEM},
        {"role": "user", "content": HEALING_PLAN_USER.format(
            profile_summary=profile_summary,
            anomaly_list=anomaly_list,
        )},
    ]

    request_id = str(dataset_id)
    logger.info(
        "healing_plan_request_start",
        model=settings.OPENROUTER_MODEL,
        dataset_id=request_id,
    )

    # json_completion enforces response_format=json_object and validates JSON
    data = await json_completion(
        messages,
        temperature=0.1,   # low temperature for deterministic structured output
        max_tokens=4096,
        request_id=request_id,
    )

    logger.info("healing_plan_request_complete", dataset_id=request_id)

    actions = []
    for a in data.get("actions", []):
        try:
            strategy = HealingStrategy(a["strategy"])
        except (ValueError, KeyError):
            strategy = HealingStrategy.MEDIAN_IMPUTATION  # safe fallback

        actions.append(HealingAction(
            action_id=a.get("action_id", str(uuid.uuid4())[:8]),
            column=a.get("column"),
            strategy=strategy,
            parameters=a.get("parameters", {}),
            rationale=a.get("rationale", ""),
            priority=int(a.get("priority", 99)),
            estimated_impact=float(a.get("estimated_impact", 0.0)),
        ))

    actions.sort(key=lambda x: x.priority)

    return HealingPlan(
        dataset_id=dataset_id,
        llm_model=settings.OPENROUTER_MODEL,
        actions=actions,
        overall_rationale=data.get("overall_rationale", ""),
    )
