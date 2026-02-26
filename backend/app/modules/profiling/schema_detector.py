"""Semantic-type detection for a pandas Series.

Detection order (first match wins):
  boolean → uuid → email → phone → ip_address → url → currency →
  percentage → numeric_id → datetime → date_string → zipcode →
  json_string → categorical → numeric → text

Each detector tests a random sample of *non-null* values.
Returns a ``SemanticTypeResult`` with ``type``, ``confidence`` (0-1), and
``evidence`` (human-readable note).
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime as _dt
from typing import Callable

import pandas as pd

# ── Compiled regex patterns ──────────────────────────────────────────────────

_EMAIL = re.compile(
    r"^[a-zA-Z0-9_.+%-]+@[a-zA-Z0-9-]+\.[a-zA-Z]{2,}$"
)
_PHONE = re.compile(
    r"^(?:\+?1[-.\s]?)?"
    r"(?:\(?\d{3}\)?[-.\s]?)?"
    r"\d{3}[-.\s]?\d{4}"
    r"(?:\s?(?:x|ext)\.?\s?\d{1,5})?$"
)
_UUID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
    r"[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
_URL = re.compile(
    r"^https?://[^\s/$.?#].[^\s]*$", re.IGNORECASE
)
_IP_V4 = re.compile(
    r"^(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}"
    r"(?:25[0-5]|2[0-4]\d|[01]?\d\d?)$"
)
_IP_V6 = re.compile(
    r"^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$"
)
_CURRENCY = re.compile(
    r"^[£€$¥₹₩฿]?\s?-?\d{1,3}(?:[,._]\d{3})*(?:[.,]\d{1,4})?$"
    r"|^-?\d{1,3}(?:[,._]\d{3})*(?:[.,]\d{1,4})?\s?"
    r"(?:USD|EUR|GBP|JPY|INR|CNY)$",
    re.IGNORECASE,
)
_PERCENTAGE = re.compile(r"^-?\d+(?:[.,]\d+)?\s?%$")
_ZIP_US = re.compile(r"^\d{5}(?:-\d{4})?$")
_ZIP_CA = re.compile(r"^[A-Z]\d[A-Z]\s?\d[A-Z]\d$", re.IGNORECASE)
_ZIP_UK = re.compile(r"^[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}$", re.IGNORECASE)
_NUMERIC_ID = re.compile(r"^\d+$")
_JSON_START = re.compile(r"^\s*[\[{]")

_DATE_FORMATS = [
    "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y",
    "%Y/%m/%d", "%d-%m-%Y", "%m-%d-%Y",
    "%d.%m.%Y", "%Y.%m.%d",
    "%b %d, %Y", "%B %d, %Y",
    "%d %b %Y", "%d %B %Y",
]
_BOOL_VALUES = frozenset({
    "true", "false", "yes", "no", "y", "n",
    "1", "0", "t", "f", "on", "off",
})


# ── Result type ──────────────────────────────────────────────────────────────

@dataclass
class SemanticTypeResult:
    """Detected semantic type with confidence score and evidence."""
    type: str
    confidence: float       # 0.0 – 1.0
    evidence: str


# ── Helpers ───────────────────────────────────────────────────────────────────

_SAMPLE_SIZE = 200


def _non_null_sample(series: pd.Series, n: int = _SAMPLE_SIZE) -> list[str]:
    """Return up to *n* non-null string-coerced values (random sample)."""
    clean = series.dropna()
    if len(clean) > n:
        clean = clean.sample(n, random_state=42)
    return [str(v).strip() for v in clean]


def _match_rate(values: list[str], pattern: re.Pattern) -> float:
    if not values:
        return 0.0
    return sum(1 for v in values if pattern.fullmatch(v)) / len(values)


def _is_parseable_date(value: str) -> bool:
    for fmt in _DATE_FORMATS:
        try:
            _dt.strptime(value, fmt)
            return True
        except ValueError:
            pass
    return False


def _is_parseable_json(value: str) -> bool:
    if not _JSON_START.match(value):
        return False
    try:
        json.loads(value)
        return True
    except Exception:
        return False


# ── Detector functions ────────────────────────────────────────────────────────

def _detect_boolean(series: pd.Series) -> SemanticTypeResult | None:
    if pd.api.types.is_bool_dtype(series):
        return SemanticTypeResult("boolean", 1.0, "pandas bool dtype")
    values = _non_null_sample(series)
    if not values:
        return None
    lower = {v.lower() for v in values}
    if lower <= _BOOL_VALUES:
        return SemanticTypeResult("boolean", 0.97, f"all values in boolean set: {lower}")
    return None


def _detect_uuid(series: pd.Series) -> SemanticTypeResult | None:
    values = _non_null_sample(series)
    rate = _match_rate(values, _UUID)
    if rate >= 0.90:
        return SemanticTypeResult("uuid", rate, f"UUID match rate {rate:.2%}")
    return None


def _detect_email(series: pd.Series) -> SemanticTypeResult | None:
    values = _non_null_sample(series)
    rate = _match_rate(values, _EMAIL)
    if rate >= 0.85:
        return SemanticTypeResult("email", rate, f"email match rate {rate:.2%}")
    return None


def _detect_phone(series: pd.Series) -> SemanticTypeResult | None:
    values = _non_null_sample(series)
    rate = _match_rate(values, _PHONE)
    if rate >= 0.85:
        return SemanticTypeResult("phone", rate, f"phone match rate {rate:.2%}")
    return None


def _detect_ip(series: pd.Series) -> SemanticTypeResult | None:
    values = _non_null_sample(series)
    rate_v4 = _match_rate(values, _IP_V4)
    rate_v6 = _match_rate(values, _IP_V6)
    best = max(rate_v4, rate_v6)
    if best >= 0.85:
        label = "ipv4" if rate_v4 >= rate_v6 else "ipv6"
        return SemanticTypeResult("ip_address", best, f"{label} match rate {best:.2%}")
    return None


def _detect_url(series: pd.Series) -> SemanticTypeResult | None:
    values = _non_null_sample(series)
    rate = _match_rate(values, _URL)
    if rate >= 0.85:
        return SemanticTypeResult("url", rate, f"URL match rate {rate:.2%}")
    return None


def _detect_currency(series: pd.Series) -> SemanticTypeResult | None:
    values = _non_null_sample(series)
    rate = _match_rate(values, _CURRENCY)
    if rate >= 0.80:
        return SemanticTypeResult("currency", rate, f"currency match rate {rate:.2%}")
    return None


def _detect_percentage(series: pd.Series) -> SemanticTypeResult | None:
    values = _non_null_sample(series)
    rate = _match_rate(values, _PERCENTAGE)
    if rate >= 0.85:
        return SemanticTypeResult("percentage", rate, f"percentage match rate {rate:.2%}")
    return None


def _detect_numeric_id(series: pd.Series) -> SemanticTypeResult | None:
    """All-digit strings with high cardinality — likely surrogate keys."""
    if not (pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series)):
        return None
    values = _non_null_sample(series)
    rate = _match_rate(values, _NUMERIC_ID)
    if rate < 0.92:
        return None
    unique_rate = series.nunique() / max(series.count(), 1)
    if unique_rate >= 0.80:
        return SemanticTypeResult(
            "numeric_id", rate,
            f"all-digit strings, unique_rate={unique_rate:.2%}",
        )
    return None


def _detect_datetime(series: pd.Series) -> SemanticTypeResult | None:
    if pd.api.types.is_datetime64_any_dtype(series):
        return SemanticTypeResult("datetime", 1.0, "pandas datetime64 dtype")
    values = _non_null_sample(series, 50)
    try:
        parsed = pd.to_datetime(pd.Series(values), infer_datetime_format=True, errors="coerce")
        rate = float(parsed.notna().mean())
        if rate >= 0.85:
            return SemanticTypeResult("datetime", rate, f"datetime parse rate {rate:.2%}")
    except Exception:
        pass
    return None


def _detect_date_string(series: pd.Series) -> SemanticTypeResult | None:
    """Plain date strings without a time component."""
    values = _non_null_sample(series, 50)
    hits = sum(1 for v in values if _is_parseable_date(v))
    rate = hits / len(values) if values else 0.0
    if rate >= 0.85:
        return SemanticTypeResult("date", rate, f"date-format parse rate {rate:.2%}")
    return None


def _detect_zipcode(series: pd.Series) -> SemanticTypeResult | None:
    values = _non_null_sample(series)
    for pat, label in [(_ZIP_US, "US"), (_ZIP_CA, "CA"), (_ZIP_UK, "UK")]:
        rate = _match_rate(values, pat)
        if rate >= 0.85:
            return SemanticTypeResult("zipcode", rate, f"{label} zip match rate {rate:.2%}")
    return None


def _detect_json_string(series: pd.Series) -> SemanticTypeResult | None:
    values = _non_null_sample(series, 50)
    hits = sum(1 for v in values if _is_parseable_json(v))
    rate = hits / len(values) if values else 0.0
    if rate >= 0.70:
        return SemanticTypeResult("json_string", rate, f"JSON parse rate {rate:.2%}")
    return None


def _detect_categorical(series: pd.Series) -> SemanticTypeResult | None:
    """Low-cardinality: ≤5 % unique rate OR ≤20 distinct values."""
    non_null = series.dropna()
    if len(non_null) == 0:
        return None
    unique_rate = non_null.nunique() / len(non_null)
    n_unique = non_null.nunique()
    if unique_rate <= 0.05 or n_unique <= 20:
        conf = 0.90 if (unique_rate <= 0.02 or n_unique <= 10) else 0.75
        return SemanticTypeResult(
            "categorical", conf,
            f"unique_rate={unique_rate:.2%}, n_distinct={n_unique}",
        )
    return None


def _detect_numeric(series: pd.Series) -> SemanticTypeResult | None:
    if pd.api.types.is_numeric_dtype(series):
        return SemanticTypeResult("numeric", 1.0, f"numeric dtype {series.dtype}")
    values = _non_null_sample(series, 50)
    numeric_count = 0
    for v in values:
        try:
            float(v.replace(",", "").replace(" ", ""))
            numeric_count += 1
        except ValueError:
            pass
    rate = numeric_count / len(values) if values else 0.0
    if rate >= 0.90:
        return SemanticTypeResult("numeric", rate, f"numeric string parse rate {rate:.2%}")
    return None


# ── Detection pipeline ────────────────────────────────────────────────────────

_DETECTORS: list[Callable[[pd.Series], SemanticTypeResult | None]] = [
    _detect_boolean,
    _detect_uuid,
    _detect_email,
    _detect_phone,
    _detect_ip,
    _detect_url,
    _detect_currency,
    _detect_percentage,
    _detect_numeric_id,
    _detect_datetime,
    _detect_date_string,
    _detect_zipcode,
    _detect_json_string,
    _detect_categorical,
    _detect_numeric,
]


def detect_semantic_type(series: pd.Series) -> SemanticTypeResult:
    """Run every detector in priority order; return the first match.

    Falls back to ``SemanticTypeResult("text", 0.5, …)`` when nothing fires.
    """
    for detector in _DETECTORS:
        result = detector(series)
        if result is not None:
            return result
    return SemanticTypeResult("text", 0.5, "no pattern matched — default")
