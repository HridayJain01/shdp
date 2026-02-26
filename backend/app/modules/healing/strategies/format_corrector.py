"""FormatCorrector — fixes values whose string format is wrong or inconsistent.

Semantic-type-aware fixers applied in this order:
  email       strip whitespace, lowercase
  phone       strip non-digit characters to a canonical form
  date        parse any recognised format → ISO-8601 (YYYY-MM-DD)
  datetime    parse any recognised format → ISO-8601 with time
  url         strip whitespace, lowercase scheme+host
  currency    strip currency symbols + thousands separators → float string
  percentage  strip % sign → float string
  zipcode     zero-pad US zips to 5 digits

Driven by HealingAction(strategy=FORMAT_STANDARDIZE, column=..., parameters={}).
Parameters:
  semantic_type  str   override automatic detection ("email", "phone", etc.)
  date_format    str   explicit strptime format for date/datetime parsing
  phone_format   str   "E164" (default) | "national" | "raw_digits"

Auto-mode (``context.config.get("auto_format_correct", True)``):
  Iterates every column whose semantic type is known in the context metadata
  and applies the matching fixer if it finds any non-conforming values.
"""
from __future__ import annotations

import re
from datetime import datetime as _dt
from typing import Callable

import pandas as pd

from app.models.healing import HealingStrategy
from .base import HealerBase, HealerResult, HealingContext, _count_changed, _make_entry

_STRATEGY_VALUE = HealingStrategy.FORMAT_STANDARDIZE.value

# ── Regex helpers ────────────────────────────────────────────────────────────

_NON_DIGIT = re.compile(r"\D")
_CURRENCY_SYMBOLS = re.compile(r"[£€$¥₹₩฿,\s]")
_DATE_FORMATS = [
    "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y",
    "%Y/%m/%d", "%d-%m-%Y", "%m-%d-%Y",
    "%d.%m.%Y", "%Y.%m.%d",
    "%b %d, %Y", "%B %d, %Y",
    "%d %b %Y", "%d %B %Y",
]
_DATETIME_FORMATS = [
    "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
    "%d/%m/%Y %H:%M:%S", "%m/%d/%Y %H:%M:%S",
]


# ── Value-level fixers ────────────────────────────────────────────────────────

def _fix_email(v: str) -> str:
    return v.strip().lower()


def _fix_phone(v: str, fmt: str = "E164") -> str:
    digits = _NON_DIGIT.sub("", v)
    if fmt == "E164":
        if len(digits) == 10:
            return f"+1{digits}"
        if len(digits) == 11 and digits[0] == "1":
            return f"+{digits}"
        return digits
    return digits


def _parse_date(v: str, explicit_fmt: str | None) -> str | None:
    formats = [explicit_fmt] if explicit_fmt else _DATE_FORMATS
    for fmt in formats:
        try:
            return _dt.strptime(v.strip(), fmt).strftime("%Y-%m-%d")
        except (ValueError, AttributeError):
            pass
    return None


def _parse_datetime(v: str, explicit_fmt: str | None) -> str | None:
    formats = [explicit_fmt] if explicit_fmt else _DATETIME_FORMATS + _DATE_FORMATS
    for fmt in formats:
        try:
            return _dt.strptime(v.strip(), fmt).isoformat()
        except (ValueError, AttributeError):
            pass
    return None


def _fix_currency(v: str) -> str:
    cleaned = _CURRENCY_SYMBOLS.sub("", v).strip()
    try:
        return str(float(cleaned.replace(",", ".")))
    except ValueError:
        return v


def _fix_percentage(v: str) -> str:
    cleaned = v.strip().rstrip("%").strip()
    try:
        return str(float(cleaned))
    except ValueError:
        return v


def _fix_zipcode_us(v: str) -> str:
    digits = _NON_DIGIT.sub("", v)
    if 3 <= len(digits) <= 5:
        return digits.zfill(5)
    return v


def _fix_url(v: str) -> str:
    stripped = v.strip()
    # Lowercase scheme and host only
    if "://" in stripped:
        scheme, rest = stripped.split("://", 1)
        if "/" in rest:
            host, path = rest.split("/", 1)
            return f"{scheme.lower()}://{host.lower()}/{path}"
        return f"{scheme.lower()}://{rest.lower()}"
    return stripped


# ── Semantic-type → fixer map ─────────────────────────────────────────────────

def _make_fixer(sem: str, params: dict) -> Callable[[str], str] | None:
    fmt = params.get("date_format")
    pfmt = params.get("phone_format", "E164")
    mapping: dict[str, Callable[[str], str]] = {
        "email":      _fix_email,
        "phone":      lambda v: _fix_phone(v, pfmt),
        "date":       lambda v: (_parse_date(v, fmt) or v),
        "datetime":   lambda v: (_parse_datetime(v, fmt) or v),
        "url":        _fix_url,
        "currency":   _fix_currency,
        "percentage": _fix_percentage,
        "zipcode":    _fix_zipcode_us,
    }
    return mapping.get(sem)


# ── Healer class ───────────────────────────────────────────────────────────────

class FormatCorrector(HealerBase):
    """Applies semantic-type-aware string format corrections."""

    name = "FormatCorrector"

    def can_apply(self, df: pd.DataFrame, context: HealingContext) -> bool:
        return bool(context.actions_for(_STRATEGY_VALUE)) or context.config.get(
            "auto_format_correct", True
        )

    def apply(self, df: pd.DataFrame, context: HealingContext) -> HealerResult:
        healed = df.copy()
        entries = []
        covered: set[str] = set()

        # ── Explicit FORMAT_STANDARDIZE actions ────────────────────────────
        for action in context.actions_for(_STRATEGY_VALUE):
            col = action.column
            if not col or col not in healed.columns:
                continue
            covered.add(col)
            params = action.parameters
            sem = params.get("semantic_type") or context.column_semantic_types.get(col, "")
            fixer = _make_fixer(sem, params)
            if fixer is None:
                continue
            before = healed[col].copy()
            healed[col] = self._apply_fixer(healed[col], fixer)
            changed = _count_changed(before.astype(str), healed[col].astype(str))
            if changed:
                entries.append(_make_entry(
                    self.name, f"format_standardize ({sem})", col, changed,
                    before=before, after=healed[col],
                    detail=f"Corrected {changed} malformed {sem} values",
                ))

        # ── Auto-correct via context metadata ──────────────────────────────
        if context.config.get("auto_format_correct", True):
            for col, sem in context.column_semantic_types.items():
                if col in covered or col not in healed.columns:
                    continue
                fixer = _make_fixer(sem, {})
                if fixer is None:
                    continue
                before = healed[col].copy()
                healed[col] = self._apply_fixer(healed[col], fixer)
                changed = _count_changed(before.astype(str), healed[col].astype(str))
                if changed:
                    entries.append(_make_entry(
                        self.name, f"auto_format ({sem})", col, changed,
                        before=before, after=healed[col],
                        detail=f"Auto-corrected {changed} malformed {sem} values",
                    ))

        return HealerResult(dataframe=healed, entries=entries)

    # ── Internal helpers ────────────────────────────────────────────────────

    @staticmethod
    def _apply_fixer(series: pd.Series, fixer: Callable[[str], str]) -> pd.Series:
        """Apply *fixer* to every non-null string value, leave null/non-str alone."""
        def safe_fix(v):
            if pd.isna(v):
                return v
            try:
                return fixer(str(v))
            except Exception:
                return v
        return series.apply(safe_fix)
