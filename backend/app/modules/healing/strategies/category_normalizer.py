"""CategoryNormalizer — standardises categorical / string column values.

Operations performed (all optional, controlled by parameters):
  strip          strip leading/trailing whitespace           (default: on)
  case           "lower" | "upper" | "title" | None         (default: "lower")
  alias_map      dict mapping raw values → canonical form   (default: {})
  unknown_token  replace values not in alias_map with this  (default: None)

Driven by HealingAction(strategy=CATEGORY_NORMALIZE, column=..., parameters={}).

Auto-mode (``context.config.get("auto_normalize_categories", True)``):
  Applies strip + lower-case to every low-cardinality (≤50 distinct) object
  column that is not covered by an explicit action.

Logs corrections whenever a value changes.
"""
from __future__ import annotations

import pandas as pd

from app.models.healing import HealingStrategy
from .base import HealerBase, HealerResult, HealingContext, _count_changed, _make_entry

_STRATEGY_VALUE = HealingStrategy.CATEGORY_NORMALIZE.value

# Maximum distinct values to treat a column as "categorical" in auto-mode
_AUTO_CARDINALITY_THRESHOLD = 50


class CategoryNormalizer(HealerBase):
    """Normalises categorical string columns to a consistent format."""

    name = "CategoryNormalizer"

    def can_apply(self, df: pd.DataFrame, context: HealingContext) -> bool:
        return bool(context.actions_for(_STRATEGY_VALUE)) or context.config.get(
            "auto_normalize_categories", True
        )

    def apply(self, df: pd.DataFrame, context: HealingContext) -> HealerResult:
        healed = df.copy()
        entries = []
        covered: set[str] = set()

        # ── Explicit CATEGORY_NORMALIZE actions ────────────────────────────
        for action in context.actions_for(_STRATEGY_VALUE):
            col = action.column
            if not col or col not in healed.columns:
                continue
            covered.add(col)
            before = healed[col].copy()
            healed[col] = self._normalise(healed[col], action.parameters)
            changed = _count_changed(before.astype(str), healed[col].astype(str))
            if changed:
                entries.append(_make_entry(
                    self.name, "category_normalize", col, changed,
                    before=before, after=healed[col],
                    detail=self._describe(action.parameters, changed),
                ))

        # ── Auto-normalise object columns ──────────────────────────────────
        if context.config.get("auto_normalize_categories", True):
            for col in healed.select_dtypes(include=["object", "string"]).columns:
                if col in covered:
                    continue
                if healed[col].nunique() > _AUTO_CARDINALITY_THRESHOLD:
                    continue
                before = healed[col].copy()
                healed[col] = self._normalise(healed[col], {"strip": True, "case": "lower"})
                changed = _count_changed(before.astype(str), healed[col].astype(str))
                if changed:
                    entries.append(_make_entry(
                        self.name, "auto_normalize (strip+lower)", col, changed,
                        before=before, after=healed[col],
                        detail=f"Auto-normalised {changed} values (strip + lowercase)",
                    ))

        return HealerResult(dataframe=healed, entries=entries)

    # ── Normalisation logic ────────────────────────────────────────────────

    @staticmethod
    def _normalise(series: pd.Series, params: dict) -> pd.Series:
        result = series.copy()
        non_null = result.notna()

        # 1. Strip whitespace
        if params.get("strip", True):
            result = result.where(~non_null, result.str.strip())

        # 2. Case normalisation
        case = params.get("case", "lower")
        if case == "lower":
            result = result.where(~non_null, result.str.lower())
        elif case == "upper":
            result = result.where(~non_null, result.str.upper())
        elif case == "title":
            result = result.where(~non_null, result.str.title())

        # 3. Alias map (raw_value → canonical)
        alias_map: dict = params.get("alias_map", {})
        if alias_map:
            result = result.replace(alias_map)

        # 4. Replace unrecognised values with unknown token
        unknown_token = params.get("unknown_token")
        known_values: list | None = params.get("allowed_values")
        if unknown_token is not None and known_values:
            known_set = set(known_values)
            mask = non_null & ~result.isin(known_set)
            result = result.where(~mask, unknown_token)

        return result

    @staticmethod
    def _describe(params: dict, changed: int) -> str:
        parts = []
        if params.get("strip", True):
            parts.append("strip")
        if params.get("case"):
            parts.append(f"case={params['case']}")
        if params.get("alias_map"):
            parts.append(f"alias_map({len(params['alias_map'])} rules)")
        if params.get("unknown_token") is not None:
            parts.append(f"unknown→'{params['unknown_token']}'")
        return f"Normalised {changed} values: {', '.join(parts) or 'no-op'}"
