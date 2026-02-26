"""Full healing pipeline as a Celery chain."""
from __future__ import annotations

import asyncio
import uuid
import io

import pandas as pd

from app.tasks.worker import celery_app
from app.modules.ingestion.parser import parse
from app.modules.ingestion.validator import validate
from app.modules.profiling.profiler import profile_dataset
from app.modules.anomaly.detector import detect
from app.modules.healing.executor import execute_plan
from app.modules.scoring.scorer import compute_delta
from app.modules.reporting.comparison import build_comparison, build_charts
from app.core.logging import get_logger

logger = get_logger(__name__)


@celery_app.task(bind=True, name="pipeline.run_full")
def run_full_pipeline(
    self,
    dataset_id: str,
    file_bytes_hex: str,
    filename: str,
    file_size: int,
):
    """
    Celery task that runs the entire pipeline synchronously.
    Async sub-calls (LLM) are wrapped with asyncio.run().
    """
    did = uuid.UUID(dataset_id)
    file_bytes = bytes.fromhex(file_bytes_hex)

    # ── 1 · Parse ─────────────────────────────────────────────────
    self.update_state(state="PROGRESS", meta={"step": "parsing"})
    parse_result = parse(file_bytes, filename)
    validate(parse_result.dataframe, file_size)
    df = parse_result.dataframe

    # ── 2 · Profile ───────────────────────────────────────────────
    self.update_state(state="PROGRESS", meta={"step": "profiling"})
    profile = profile_dataset(df, did)

    # ── 3 · Detect ────────────────────────────────────────────────
    self.update_state(state="PROGRESS", meta={"step": "anomaly_detection"})
    anomaly_report = detect(df, did)

    # ── 4 · Plan (async LLM call) ──────────────────────────────────
    self.update_state(state="PROGRESS", meta={"step": "planning"})
    from app.modules.ai.reasoning import generate_healing_plan
    healing_plan = asyncio.run(generate_healing_plan(profile, anomaly_report, did))

    # ── 5 · Heal ──────────────────────────────────────────────────
    self.update_state(state="PROGRESS", meta={"step": "healing"})
    semantic_types = {c.name: c.semantic_type for c in profile.columns if c.semantic_type}
    dtype_categories = {c.name: c.dtype_category for c in profile.columns}
    healed_df, healing_result = execute_plan(
        df, healing_plan,
        column_semantic_types=semantic_types,
        column_dtype_categories=dtype_categories,
    )

    # ── 6 · Score ─────────────────────────────────────────────────
    self.update_state(state="PROGRESS", meta={"step": "scoring"})
    quality_delta = compute_delta(df, healed_df, did)

    # ── 7 · Report ────────────────────────────────────────────────
    self.update_state(state="PROGRESS", meta={"step": "reporting"})
    profile_after = profile_dataset(healed_df, did)
    comparison = build_comparison(df, healed_df)
    charts = build_charts(profile, profile_after, quality_delta)

    logger.info(
        "pipeline_complete",
        dataset_id=dataset_id,
        score_before=quality_delta.before.total_score,
        score_after=quality_delta.after.total_score,
    )

    return {
        "dataset_id": dataset_id,
        "profile": profile.model_dump(),
        "anomaly_report": anomaly_report.model_dump(),
        "healing_plan": healing_plan.model_dump(),
        "healing_result": healing_result.model_dump(),
        "quality_delta": quality_delta.model_dump(),
        "comparison": comparison,
        "charts": charts,
    }
