"""TypeMismatchHealer — coerces column values to their expected data type.

Driven by HealingAction(strategy=TYPE_CAST, column=..., parameters={...}).
Parameters understood:
  target_type  str   one of "numeric", "int", "float", "datetime", "bool", "str"
  errors       str   "coerce" (default) | "ignore" — what to do with bad values
  date_format  str   optional strptime format for datetime coercion

If no TYPE_CAST actions exist but the context carries semantic-type metadata,
the healer optionally auto-coerces obvious mismatches (enabled by
``context.config.get("auto_type_cast", True)``).
"""
from __future__ import annotations

import pandas as pd

from app.models.healing import HealingStrategy
from .base import HealerBase, HealerResult, HealingContext, _count_changed, _make_entry

_STRATEGY_VALUE = HealingStrategy.TYPE_CAST.value

# Semantic types that imply a target pandas dtype
_SEMANTIC_TO_TARGET: dict[str, str] = {
    "numeric":    "float",
    "numeric_id": "int",
    "datetime":   "datetime",
    "date":       "datetime",
    "boolean":    "bool",
}


class TypeMismatchHealer(HealerBase):
    """Coerces columns to their correct data types, handling parse failures gracefully."""

    name = "TypeMismatchHealer"

    def can_apply(self, df: pd.DataFrame, context: HealingContext) -> bool:
        return bool(context.actions_for(_STRATEGY_VALUE)) or context.config.get(
            "auto_type_cast", True
        )

    def apply(self, df: pd.DataFrame, context: HealingContext) -> HealerResult:
        healed = df.copy()
        entries = []
        covered: set[str] = set()

        # ── Explicit TYPE_CAST actions ─────────────────────────────────────
        for action in context.actions_for(_STRATEGY_VALUE):
            col = action.column
            if not col or col not in healed.columns:
                continue
            covered.add(col)
            target = action.parameters.get("target_type", "str")
            errors = action.parameters.get("errors", "coerce")
            fmt = action.parameters.get("date_format")
            before = healed[col].copy()
            healed[col] = self._cast(healed[col], target, errors, fmt)
            changed = _count_changed(before.astype(str), healed[col].astype(str))
            if changed:
                entries.append(_make_entry(
                    self.name, f"type_cast→{target}", col, changed,
                    before=before, after=healed[col],
                    detail=f"Coerced {changed} values to {target}",
                ))

        # ── Auto type-cast via semantic metadata ───────────────────────────
        if context.config.get("auto_type_cast", True):
            for col, sem in context.column_semantic_types.items():
                if col in covered or col not in healed.columns:
                    continue
                target = _SEMANTIC_TO_TARGET.get(sem)
                if not target:
                    continue
                # Skip if column already has the right dtype
                if self._already_correct(healed[col], target):
                    continue
                before = healed[col].copy()
                healed[col] = self._cast(healed[col], target, "coerce", None)
                changed = _count_changed(before.astype(str), healed[col].astype(str))
                if changed:
                    entries.append(_make_entry(
                        self.name, f"auto_cast→{target} (semantic={sem})",
                        col, changed,
                        before=before, after=healed[col],
                        detail=f"Auto-coerced {changed} values based on semantic type '{sem}'",
                    ))

        return HealerResult(dataframe=healed, entries=entries)

    # ── Internal helpers ───────────────────────────────────────────────────

    @staticmethod
    def _cast(
        series: pd.Series,
        target: str,
        errors: str,
        date_format: str | None,
    ) -> pd.Series:
        try:
            if target in ("numeric", "float"):
                return pd.to_numeric(series, errors=errors)
            if target == "int":
                numeric = pd.to_numeric(series, errors=errors)
                return numeric.astype("Int64")   # nullable integer
            if target == "datetime":
                kwargs: dict = {"errors": errors, "infer_datetime_format": True}
                if date_format:
                    kwargs["format"] = date_format
                return pd.to_datetime(series, **kwargs)
            if target == "bool":
                lower = series.astype(str).str.lower().str.strip()
                mapping = {
                    "1": True, "true": True, "yes": True, "y": True, "t": True, "on": True,
                    "0": False, "false": False, "no": False, "n": False, "f": False, "off": False,
                }
                return lower.map(mapping).where(lower.isin(mapping), other=pd.NA)
            if target == "str":
                return series.astype(str)
        except Exception:
            pass
        return series

    @staticmethod
    def _already_correct(series: pd.Series, target: str) -> bool:
        if target in ("numeric", "float") and pd.api.types.is_float_dtype(series):
            return True
        if target == "int" and pd.api.types.is_integer_dtype(series):
            return True
        if target == "datetime" and pd.api.types.is_datetime64_any_dtype(series):
            return True
        if target == "bool" and pd.api.types.is_bool_dtype(series):
            return True
        return False
