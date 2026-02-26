"""OutlierCapper — clips extreme values in numeric columns.

Three capping methods, selected via HealingAction.strategy:
  iqr_clamp         Q1 - factor*IQR … Q3 + factor*IQR  (default factor=1.5)
  zscore_clamp      mean ± threshold*σ                  (default threshold=3.0)
  percentile_clamp  [lower_pct, upper_pct] percentiles  (default 1 % / 99 %)

Parameters (all optional):
  factor       float   IQR multiplier (iqr_clamp)
  threshold    float   Z-score cutoff (zscore_clamp)
  lower_pct    float   lower percentile 0-100 (percentile_clamp)
  upper_pct    float   upper percentile 0-100 (percentile_clamp)

Auto-mode (``context.config.get("auto_cap_outliers", False)``):
  Iterates every numeric column with outliers (IQR method) and caps them.
  Disabled by default to avoid silently distorting data without explicit intent.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from app.models.healing import HealingStrategy
from .base import HealerBase, HealerResult, HealingContext, _count_changed, _make_entry

_STRATEGY_VALUES = {
    HealingStrategy.IQR_CLAMP.value,
    HealingStrategy.ZSCORE_CLAMP.value,
    HealingStrategy.PERCENTILE_CLAMP.value,
}


class OutlierCapper(HealerBase):
    """Clips outliers in numeric columns using configurable fence methods."""

    name = "OutlierCapper"

    def can_apply(self, df: pd.DataFrame, context: HealingContext) -> bool:
        return bool(context.actions_for(*_STRATEGY_VALUES)) or context.config.get(
            "auto_cap_outliers", False
        )

    def apply(self, df: pd.DataFrame, context: HealingContext) -> HealerResult:
        healed = df.copy()
        entries = []
        covered: set[str] = set()

        # ── Explicit actions ───────────────────────────────────────────────
        for action in context.actions_for(*_STRATEGY_VALUES):
            col = action.column
            s = action.strategy.value
            p = action.parameters
            if not col or col not in healed.columns:
                continue
            if not pd.api.types.is_numeric_dtype(healed[col]):
                continue
            covered.add(col)
            before = healed[col].copy()
            lower, upper = self._compute_bounds(healed[col], s, p)
            healed[col] = healed[col].clip(lower=lower, upper=upper)
            changed = _count_changed(before, healed[col])
            if changed:
                entries.append(_make_entry(
                    self.name, s, col, changed,
                    before=before, after=healed[col],
                    detail=(
                        f"Capped {changed} outliers to [{lower:.4g}, {upper:.4g}] "
                        f"using {s}"
                    ),
                ))

        # ── Auto-cap remaining numeric columns ─────────────────────────────
        if context.config.get("auto_cap_outliers", False):
            for col in healed.select_dtypes(include="number").columns:
                if col in covered:
                    continue
                before = healed[col].copy()
                lower, upper = self._compute_bounds(
                    healed[col], HealingStrategy.IQR_CLAMP.value, {}
                )
                healed[col] = healed[col].clip(lower=lower, upper=upper)
                changed = _count_changed(before, healed[col])
                if changed:
                    entries.append(_make_entry(
                        self.name, "iqr_clamp (auto)", col, changed,
                        before=before, after=healed[col],
                        detail=f"Auto-capped {changed} outliers to [{lower:.4g}, {upper:.4g}]",
                    ))

        return HealerResult(dataframe=healed, entries=entries)

    # ── Bound calculators ─────────────────────────────────────────────────────

    @staticmethod
    def _compute_bounds(
        series: pd.Series, strategy: str, params: dict
    ) -> tuple[float, float]:
        clean = series.dropna().replace([np.inf, -np.inf], np.nan).dropna()
        if len(clean) < 4:
            return float("-inf"), float("inf")

        if strategy == HealingStrategy.IQR_CLAMP.value:
            factor = float(params.get("factor", 1.5))
            q1 = float(clean.quantile(0.25))
            q3 = float(clean.quantile(0.75))
            iqr = q3 - q1
            return q1 - factor * iqr, q3 + factor * iqr

        if strategy == HealingStrategy.ZSCORE_CLAMP.value:
            threshold = float(params.get("threshold", 3.0))
            mean = float(clean.mean())
            std = float(clean.std())
            if std == 0:
                return float("-inf"), float("inf")
            return mean - threshold * std, mean + threshold * std

        if strategy == HealingStrategy.PERCENTILE_CLAMP.value:
            lower_pct = float(params.get("lower_pct", 1.0)) / 100
            upper_pct = float(params.get("upper_pct", 99.0)) / 100
            return float(clean.quantile(lower_pct)), float(clean.quantile(upper_pct))

        return float("-inf"), float("inf")
