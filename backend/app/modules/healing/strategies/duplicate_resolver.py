"""DuplicateResolver — detects and removes duplicate rows.

Driven by HealingAction(strategy=DEDUPLICATION, parameters={...}).
Parameters understood:
  subset   list[str] | null   columns to consider (None = all columns)
  keep     "first" | "last" | false    which duplicate to keep; false = drop all
  ignore_columns  list[str]   columns to exclude when comparing rows

If no DEDUPLICATION action exists, the healer still runs in auto-mode
(``context.config.get("auto_deduplicate", True)``) using all columns.

Logs:
  - Number of duplicate rows removed
  - Sample of removed row indices
"""
from __future__ import annotations

import pandas as pd

from app.models.healing import HealingStrategy
from .base import HealerBase, HealerResult, HealingContext, _make_entry

_STRATEGY_VALUE = HealingStrategy.DEDUPLICATION.value


class DuplicateResolver(HealerBase):
    """Removes exact duplicate rows from a DataFrame."""

    name = "DuplicateResolver"

    def can_apply(self, df: pd.DataFrame, context: HealingContext) -> bool:
        has_action = bool(context.actions_for(_STRATEGY_VALUE))
        has_dups = bool(df.duplicated().any())
        return has_action or (has_dups and context.config.get("auto_deduplicate", True))

    def apply(self, df: pd.DataFrame, context: HealingContext) -> HealerResult:
        healed = df.copy()
        entries = []

        actions = context.actions_for(_STRATEGY_VALUE)

        if actions:
            for action in actions:
                p = action.parameters
                healed, entry = self._dedup(
                    healed,
                    subset=p.get("subset") or None,
                    keep=p.get("keep", "first"),
                    ignore_columns=p.get("ignore_columns", []),
                )
                if entry:
                    entries.append(entry)
        elif context.config.get("auto_deduplicate", True):
            healed, entry = self._dedup(healed, subset=None, keep="first")
            if entry:
                entries.append(entry)

        return HealerResult(dataframe=healed, entries=entries)

    # ── Internal helpers ───────────────────────────────────────────────────

    def _dedup(
        self,
        df: pd.DataFrame,
        subset: list[str] | None,
        keep: str | bool,
        ignore_columns: list[str] | None = None,
    ) -> tuple[pd.DataFrame, object | None]:
        """Remove duplicates and return (new_df, entry_or_None)."""
        compare_cols: list[str] | None = subset
        if ignore_columns and compare_cols is None:
            compare_cols = [c for c in df.columns if c not in ignore_columns]

        before_len = len(df)

        # pandas keep param: "first", "last", or False
        pandas_keep: str | bool = keep if keep in ("first", "last") else False
        dup_mask = df.duplicated(subset=compare_cols, keep=pandas_keep)
        removed_indices = df.index[dup_mask].tolist()
        deduped = df.drop(index=removed_indices).reset_index(drop=True)

        removed_count = before_len - len(deduped)
        if removed_count == 0:
            return deduped, None

        sample_indices = removed_indices[:5]
        entry = _make_entry(
            self.name,
            "deduplication",
            column=None,
            corrections=removed_count,
            detail=(
                f"Removed {removed_count} duplicate rows "
                f"(subset={compare_cols}, keep='{keep}'). "
                f"Sample removed indices: {sample_indices}"
            ),
        )
        return deduped, entry
