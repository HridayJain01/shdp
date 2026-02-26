"""Abstract base class for all healing strategies.

Every healer receives a ``HealingContext`` (the active plan + optional profile)
and the *current* DataFrame, and returns a ``HealerResult`` containing the
(possibly modified) DataFrame plus a list of ``TransformationEntry`` records.

The engine composing multiple healers is responsible for chaining results and
building the final ``TransformationLog``.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from app.models.healing import HealingAction, HealingPlan, TransformationEntry


# ── Context passed to every healer ────────────────────────────────────────────

@dataclass
class HealingContext:
    """Carries everything a healer needs to make decisions."""
    plan: HealingPlan
    # Optional: column-level metadata from the profiler
    column_semantic_types: dict[str, str] = field(default_factory=dict)
    column_dtype_categories: dict[str, str] = field(default_factory=dict)
    # Per-run configuration overrides
    config: dict[str, Any] = field(default_factory=dict)

    def actions_for(self, *strategy_values: str) -> list[HealingAction]:
        """Return plan actions whose strategy value is in *strategy_values*."""
        return [
            a for a in sorted(self.plan.actions, key=lambda x: x.priority)
            if a.strategy.value in strategy_values
        ]


# ── Healer result ─────────────────────────────────────────────────────────────

@dataclass
class HealerResult:
    dataframe: pd.DataFrame
    entries: list[TransformationEntry] = field(default_factory=list)

    @property
    def total_corrections(self) -> int:
        return sum(e.corrections for e in self.entries)


# ── Helpers used by multiple healers ─────────────────────────────────────────

def _sample_values(series: pd.Series, n: int = 5) -> list[Any]:
    """Return up to *n* non-null representative values."""
    clean = series.dropna()
    if clean.empty:
        return []
    sample = clean.sample(min(n, len(clean)), random_state=0)
    return [v.item() if hasattr(v, "item") else v for v in sample]


def _count_changed(before: pd.Series, after: pd.Series) -> int:
    """Count cells whose value changed between *before* and *after*."""
    try:
        return int((before != after).sum())
    except TypeError:
        # Mixed-type comparison can raise; fall back to object comparison
        return int((before.astype(object) != after.astype(object)).sum())


def _make_entry(
    strategy_name: str,
    operation: str,
    column: str | None,
    corrections: int,
    before: pd.Series | None = None,
    after: pd.Series | None = None,
    detail: str = "",
) -> TransformationEntry:
    return TransformationEntry(
        strategy_name=strategy_name,
        operation=operation,
        column=column,
        corrections=corrections,
        before_sample=_sample_values(before) if before is not None else [],
        after_sample=_sample_values(after) if after is not None else [],
        detail=detail,
    )


# ── Abstract base ─────────────────────────────────────────────────────────────

class HealerBase(ABC):
    """All healing strategies must inherit from this class.

    Subclasses implement :meth:`apply` and optionally :meth:`can_apply` to
    declare whether they are relevant for a given context.
    """

    #: Override in subclasses with a stable, human-readable identifier.
    name: str = "HealerBase"

    def can_apply(self, df: pd.DataFrame, context: HealingContext) -> bool:  # noqa: ARG002
        """Return False to skip this healer entirely (e.g. no relevant actions)."""
        return True

    @abstractmethod
    def apply(self, df: pd.DataFrame, context: HealingContext) -> HealerResult:
        """Apply the strategy and return a :class:`HealerResult`."""
