"""MissingValueHealer — fills NaN / None cells using column-appropriate methods.

Supported operations (driven by HealingAction.strategy + parameters):
  mean_imputation     → numeric columns, fill with column mean
  median_imputation   → numeric columns, fill with column median
  mode_imputation     → any column, fill with most frequent value
  constant_imputation → any column, fill with parameters["value"]
  interpolation       → numeric / datetime, linear or time interpolation
  forward_fill        → any column, propagate last valid value forward
  backward_fill       → any column, propagate next valid value backward
  drop_rows           → drop rows where parameters.get("subset") cols are null

Auto-mode (no matching HealingAction):
  If ``context.config.get("auto_impute") is True`` and no action covers a
  column, falls back to median (numeric) or mode (categorical/text).
"""
from __future__ import annotations

import pandas as pd

from app.models.healing import HealingStrategy
from .base import HealerBase, HealerResult, HealingContext, _count_changed, _make_entry, _sample_values

_STRATEGY_VALUES = {
    HealingStrategy.MEAN_IMPUTATION.value,
    HealingStrategy.MEDIAN_IMPUTATION.value,
    HealingStrategy.MODE_IMPUTATION.value,
    HealingStrategy.CONSTANT_IMPUTATION.value,
    HealingStrategy.INTERPOLATION.value,
    HealingStrategy.FORWARD_FILL.value,
    HealingStrategy.BACKWARD_FILL.value,
    HealingStrategy.DROP_ROWS.value,
}


class MissingValueHealer(HealerBase):
    """Imputes or removes missing values from a DataFrame."""

    name = "MissingValueHealer"

    def can_apply(self, df: pd.DataFrame, context: HealingContext) -> bool:
        has_actions = bool(context.actions_for(*_STRATEGY_VALUES))
        has_nulls = bool(df.isna().any().any())
        return has_actions or (has_nulls and context.config.get("auto_impute", True))

    def apply(self, df: pd.DataFrame, context: HealingContext) -> HealerResult:
        healed = df.copy()
        entries = []

        actions = context.actions_for(*_STRATEGY_VALUES)
        covered_columns: set[str] = set()

        for action in actions:
            col = action.column
            s = action.strategy.value
            p = action.parameters

            # ── DROP_ROWS is dataset-wide ──────────────────────────────────
            if s == HealingStrategy.DROP_ROWS.value:
                subset = p.get("subset") or None
                before_len = len(healed)
                healed = healed.dropna(subset=subset)
                dropped = before_len - len(healed)
                if dropped:
                    entries.append(_make_entry(
                        self.name, "drop_rows", col,
                        corrections=dropped,
                        detail=f"Dropped {dropped} rows with nulls (subset={subset})",
                    ))
                continue

            if col and col in healed.columns:
                covered_columns.add(col)
                before = healed[col].copy()
                healed[col] = self._fill_series(healed[col], s, p)
                changed = _count_changed(before, healed[col])
                if changed:
                    entries.append(_make_entry(
                        self.name, s, col, changed,
                        before=before[before.isna()].reindex(before.index),
                        after=healed[col],
                        detail=f"Filled {changed} nulls with {s}",
                    ))

        # ── Auto-impute remaining columns that still have nulls ────────────
        if context.config.get("auto_impute", True):
            for col in healed.columns:
                if col in covered_columns:
                    continue
                null_count = healed[col].isna().sum()
                if null_count == 0:
                    continue
                before = healed[col].copy()
                if pd.api.types.is_numeric_dtype(healed[col]):
                    healed[col] = healed[col].fillna(healed[col].median())
                    op = "median_imputation (auto)"
                else:
                    mode = healed[col].mode()
                    healed[col] = healed[col].fillna(mode.iloc[0] if not mode.empty else "")
                    op = "mode_imputation (auto)"
                changed = _count_changed(before, healed[col])
                if changed:
                    entries.append(_make_entry(
                        self.name, op, col, changed,
                        before=before[before.isna()].reindex(before.index),
                        after=healed[col],
                        detail=f"Auto-filled {changed} nulls",
                    ))

        return HealerResult(dataframe=healed, entries=entries)

    # ── Internal helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _fill_series(s: pd.Series, strategy: str, params: dict) -> pd.Series:
        if strategy == HealingStrategy.MEAN_IMPUTATION.value:
            return s.fillna(s.mean())
        if strategy == HealingStrategy.MEDIAN_IMPUTATION.value:
            return s.fillna(s.median())
        if strategy == HealingStrategy.MODE_IMPUTATION.value:
            mode = s.mode()
            return s.fillna(mode.iloc[0] if not mode.empty else s)
        if strategy == HealingStrategy.CONSTANT_IMPUTATION.value:
            return s.fillna(params.get("value", 0))
        if strategy == HealingStrategy.INTERPOLATION.value:
            return s.interpolate(method=params.get("method", "linear"))
        if strategy == HealingStrategy.FORWARD_FILL.value:
            return s.ffill()
        if strategy == HealingStrategy.BACKWARD_FILL.value:
            return s.bfill()
        return s
