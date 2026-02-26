"""Column-name normalisation: any string → valid, readable snake_case.

Rules applied in order
──────────────────────
1. Decode to str & strip surrounding whitespace.
2. Transliterate accented / non-ASCII characters to closest ASCII equivalent
   (using ``unicodedata`` — no third-party dependency).
3. Replace any run of non-alphanumeric characters with a single underscore.
4. Collapse repeated underscores.
5. Strip leading / trailing underscores.
6. Lowercase everything.
7. Prefix with ``col_`` if the result starts with a digit (invalid Python id).
8. Rename empty names to ``unnamed_<N>``.
9. Deduplicate: if two columns normalise to the same name, append ``_2``, ``_3``…
"""
from __future__ import annotations

import re
import unicodedata


# Compiled once at import time for speed.
_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_MULTI_UNDER = re.compile(r"_+")


def _transliterate(text: str) -> str:
    """Replace accented characters with their ASCII base letter."""
    # NFD decomposes e.g. "é" → "e" + combining accent; then we drop non-ASCII.
    return (
        unicodedata.normalize("NFD", text)
        .encode("ascii", errors="ignore")
        .decode("ascii")
    )


def _normalise_single(name: str, index: int) -> str:
    """Normalise a single column name (no deduplication at this stage)."""
    name = str(name).strip()

    if not name or name.lower() in ("nan", "none", "null"):
        return f"unnamed_{index}"

    name = _transliterate(name)
    name = name.lower()
    name = _NON_ALNUM.sub("_", name)
    name = _MULTI_UNDER.sub("_", name)
    name = name.strip("_")

    if not name:
        return f"unnamed_{index}"

    # Python identifiers cannot start with a digit.
    if name[0].isdigit():
        name = f"col_{name}"

    return name


def normalise_columns(columns: list[str]) -> list[str]:
    """
    Normalise and deduplicate a list of column names.

    Args:
        columns: Raw column names from the parsed DataFrame.

    Returns:
        List of clean, unique, snake_case column names of the same length.

    Examples:
        >>> normalise_columns(["First Name", "first_name", "ÀgeÉ", "123id", ""])
        ['first_name', 'first_name_2', 'agee', 'col_123id', 'unnamed_4']
    """
    seen: dict[str, int] = {}
    result: list[str] = []

    for i, raw in enumerate(columns):
        base = _normalise_single(raw, i)

        if base not in seen:
            seen[base] = 1
            result.append(base)
        else:
            seen[base] += 1
            deduped = f"{base}_{seen[base]}"
            # Edge case: deduped name itself already exists
            while deduped in seen:
                seen[base] += 1
                deduped = f"{base}_{seen[base]}"
            seen[deduped] = 1
            result.append(deduped)

    return result
