"""Before/after diff and chart-ready JSON payload generation."""
from __future__ import annotations

import pandas as pd


def build_comparison(df_before: pd.DataFrame, df_after: pd.DataFrame) -> dict:
    """
    Returns a structured diff:
    - changed_cells: list of {row, col, before, after}
    - added_columns / removed_columns
    - row_count_delta
    """
    added_cols   = list(set(df_after.columns) - set(df_before.columns))
    removed_cols = list(set(df_before.columns) - set(df_after.columns))

    common = [c for c in df_before.columns if c in df_after.columns]
    min_rows = min(len(df_before), len(df_after))

    changed_cells = []
    for col in common:
        b = df_before[col].iloc[:min_rows].reset_index(drop=True)
        a = df_after[col].iloc[:min_rows].reset_index(drop=True)
        diff_mask = b.astype(str) != a.astype(str)
        for row_idx in diff_mask[diff_mask].index[:500]:    # cap output
            changed_cells.append({
                "row": int(row_idx),
                "col": col,
                "before": _safe(b.iloc[row_idx]),
                "after":  _safe(a.iloc[row_idx]),
            })

    return {
        "changed_cells_count": len(changed_cells),
        "changed_cells": changed_cells[:200],   # send max 200 to frontend
        "added_columns": added_cols,
        "removed_columns": removed_cols,
        "row_count_before": len(df_before),
        "row_count_after": len(df_after),
        "row_count_delta": len(df_after) - len(df_before),
    }


def build_charts(profile_before, profile_after, quality_delta) -> dict:
    """Return chart-ready JSON for the React dashboard."""
    null_comparison = []
    after_map = {cp.name: cp for cp in profile_after.columns}
    for cp in profile_before.columns:
        after_cp = after_map.get(cp.name)
        null_comparison.append({
            "column": cp.name,
            "before": cp.null_pct,
            "after":  after_cp.null_pct if after_cp else 0.0,
        })

    pillar_comparison = [
        {
            "pillar": p_before.name,
            "before": round(p_before.raw_score * 100, 2),
            "after":  round(p_after.raw_score * 100, 2),
        }
        for p_before, p_after in zip(
            quality_delta.before.pillars, quality_delta.after.pillars
        )
    ]

    return {
        "null_rate_comparison": null_comparison,
        "pillar_score_comparison": pillar_comparison,
        "overall_score": {
            "before": quality_delta.before.total_score,
            "after":  quality_delta.after.total_score,
            "delta":  quality_delta.delta,
        },
    }


def _safe(val):
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    return str(val)
