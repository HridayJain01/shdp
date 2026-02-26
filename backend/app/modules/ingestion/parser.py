"""Production-ready dataset parser.

Supports CSV, JSON (records / array-of-objects), and Excel (.xlsx / .xls).

Key features
────────────
- Auto-detects encoding (BOM → chardet → latin-1 fallback).
- Tries multiple CSV dialects (separator sniffing, quoting modes).
- Handles corrupted rows gracefully with configurable tolerance.
- Normalises all column names to snake_case.
- Raises structured :mod:`exceptions` instead of bare Python errors.
- Zero global state — safe for concurrent use.
"""
from __future__ import annotations

import io
import json
import logging
import csv as _csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from app.modules.ingestion.encoding import detect as detect_encoding
from app.modules.ingestion.exceptions import (
    CorruptRowsError,
    EmptyDatasetError,
    ParseError,
    SchemaError,
    UnsupportedFormatError,
)
from app.modules.ingestion.normalizer import normalise_columns

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────

_SUPPORTED_EXTENSIONS: dict[str, str] = {
    ".csv":    "csv",
    ".tsv":    "csv",
    ".txt":    "csv",
    ".json":   "json",
    ".jsonl":  "jsonl",
    ".ndjson": "jsonl",
    ".xlsx":   "excel",
    ".xls":    "excel",
    ".xlsm":   "excel",
}

# Separators tried when sniffing fails.
_CSV_SEPARATOR_CANDIDATES = [",", ";", "\t", "|", ":"]

# Pandas CSV quoting constants.
_QUOTING_MODES = [_csv.QUOTE_MINIMAL, _csv.QUOTE_NONE, _csv.QUOTE_ALL]


# ── Public result type ─────────────────────────────────────────────────────

@dataclass
class ParseResult:
    """Returned by :func:`parse`. Always contains a valid DataFrame."""

    dataframe: pd.DataFrame
    format: str                          # "csv" | "json" | "jsonl" | "excel"
    encoding: str                        # detected encoding
    encoding_confidence: float
    rows_total: int                      # rows in source (before corruption drop)
    rows_dropped: int = 0
    corrupt_row_indices: list[int] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


# ── Format detection ───────────────────────────────────────────────────────

def detect_format(filename: str) -> str:
    """Map filename → format string.  Raises :exc:`UnsupportedFormatError`."""
    ext = Path(filename).suffix.lower()
    fmt = _SUPPORTED_EXTENSIONS.get(ext)
    if fmt is None:
        raise UnsupportedFormatError(
            message=f"Unsupported file extension '{ext}'.",
            details={"filename": filename, "supported": list(_SUPPORTED_EXTENSIONS)},
        )
    return fmt


# ── Column normalisation helper ────────────────────────────────────────────

def _apply_column_normalisation(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = normalise_columns([str(c) for c in df.columns])
    return df


# ── CSV parsing ────────────────────────────────────────────────────────────

def _sniff_separator(sample: str) -> str | None:
    """Use csv.Sniffer on a text sample; return None if inconclusive."""
    try:
        dialect = _csv.Sniffer().sniff(sample, delimiters=",;\t|:")
        return dialect.delimiter
    except _csv.Error:
        return None


def _read_csv_attempt(
    raw: bytes,
    encoding: str,
    sep: str,
    quoting: int,
    on_bad_lines: str,
) -> pd.DataFrame:
    return pd.read_csv(
        io.BytesIO(raw),
        sep=sep,
        encoding=encoding,
        encoding_errors="replace",
        quoting=quoting,
        on_bad_lines=on_bad_lines,   # "warn" | "skip" | "error"
        low_memory=False,
        dtype_backend="numpy_nullable",
    )


def _parse_csv(raw: bytes, encoding: str) -> tuple[pd.DataFrame, int, list[int], list[str]]:
    """
    Multi-strategy CSV parser.

    Returns:
        (DataFrame, rows_total, corrupt_row_indices, warnings)
    """
    warnings: list[str] = []
    sample_text = raw[:8192].decode(encoding, errors="replace")

    # ── Determine separator ───────────────────────────────────────────────
    sep = _sniff_separator(sample_text)
    if sep is None:
        for candidate in _CSV_SEPARATOR_CANDIDATES:
            if candidate in sample_text:
                sep = candidate
                break
        if sep is None:
            sep = ","

    logger.debug("csv_separator_detected", sep=repr(sep), encoding=encoding)

    # ── Attempt 1: strict parse (raises on bad lines) ─────────────────────
    try:
        df = _read_csv_attempt(raw, encoding, sep, _csv.QUOTE_MINIMAL, "error")
        return df, len(df), [], warnings
    except Exception as strict_exc:
        warnings.append(
            f"Strict CSV parse failed ({type(strict_exc).__name__}: {strict_exc}); "
            "retrying with corruption tolerance."
        )
        logger.warning("csv_strict_parse_failed", error=str(strict_exc))

    # ── Attempt 2: skip bad lines across quoting modes ────────────────────
    for quoting in _QUOTING_MODES:
        try:
            df = _read_csv_attempt(raw, encoding, sep, quoting, "skip")
            if not df.empty:
                total_lines = raw.count(b"\n")
                data_lines = max(total_lines - 1, 0)   # subtract header
                rows_dropped = max(data_lines - len(df), 0)
                if rows_dropped:
                    warnings.append(f"Skipped ~{rows_dropped} corrupt/malformed row(s).")
                return df, data_lines, [], warnings
        except Exception as exc:
            logger.debug("csv_quoting_attempt_failed", quoting=quoting, error=str(exc))
            continue

    # ── Attempt 3: row-by-row fallback ────────────────────────────────────
    corrupt_indices: list[int] = []
    clean_lines: list[str] = []

    decoded = raw.decode(encoding, errors="replace")
    all_lines = decoded.splitlines()

    if not all_lines:
        raise ParseError(
            message="CSV file is empty or unreadable.",
            details={"encoding": encoding},
        )

    header = all_lines[0]
    clean_lines.append(header)
    header_cols = len(next(_csv.reader([header], delimiter=sep), []))

    for i, line in enumerate(all_lines[1:], start=1):
        try:
            row = next(_csv.reader([line], delimiter=sep))
            if len(row) == header_cols:
                clean_lines.append(line)
            else:
                corrupt_indices.append(i)
        except _csv.Error:
            corrupt_indices.append(i)

    if len(clean_lines) <= 1:   # only the header survived
        raise ParseError(
            message="All data rows are corrupt — cannot recover any data.",
            details={"encoding": encoding, "total_lines": len(all_lines)},
        )

    recovered_raw = "\n".join(clean_lines).encode(encoding, errors="replace")
    try:
        df = pd.read_csv(
            io.BytesIO(recovered_raw),
            sep=sep,
            encoding=encoding,
            encoding_errors="replace",
            low_memory=False,
        )
    except Exception as final_exc:
        raise ParseError(
            message=f"CSV could not be parsed even after row-by-row recovery: {final_exc}",
            details={"encoding": encoding},
        ) from final_exc

    total_data_lines = len(all_lines) - 1
    warnings.append(
        f"Row-by-row recovery: dropped {len(corrupt_indices)} corrupt row(s) "
        f"out of {total_data_lines}."
    )
    return df, total_data_lines, corrupt_indices, warnings


# ── JSON / JSONL parsing ───────────────────────────────────────────────────

def _flatten_if_needed(obj: Any) -> list[dict]:
    """
    Normalise a JSON value to a list of flat dicts (records).

    Handles:
      - list of dicts           → pass through
      - dict with one list key  → unwrap that key
      - plain dict              → wrap in list
    """
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        list_keys = [k for k, v in obj.items() if isinstance(v, list)]
        if len(list_keys) == 1:
            return obj[list_keys[0]]
        if len(list_keys) > 1:
            preferred = [
                k for k in list_keys
                if k.lower() in ("data", "records", "items", "rows", "results")
            ]
            return obj[preferred[0] if preferred else list_keys[0]]
        return [obj]
    raise SchemaError(
        message=f"JSON root must be an array or an object, got {type(obj).__name__}.",
        details={"root_type": type(obj).__name__},
    )


def _parse_json(raw: bytes, encoding: str) -> tuple[pd.DataFrame, int, list[int], list[str]]:
    warnings: list[str] = []
    text = raw.decode(encoding, errors="replace")

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ParseError(
            message=f"Invalid JSON: {exc.msg} (line {exc.lineno}, col {exc.colno}).",
            details={"lineno": exc.lineno, "colno": exc.colno},
        ) from exc

    records = _flatten_if_needed(parsed)
    total = len(records)

    clean: list[dict] = []
    corrupt_indices: list[int] = []
    for i, rec in enumerate(records):
        if isinstance(rec, dict):
            clean.append(rec)
        else:
            corrupt_indices.append(i)

    if corrupt_indices:
        sample = corrupt_indices[:10]
        warnings.append(
            f"Skipped {len(corrupt_indices)} non-object JSON element(s) at indices "
            f"{sample}{'…' if len(corrupt_indices) > 10 else ''}."
        )

    if not clean:
        raise EmptyDatasetError(
            message="JSON contains no valid object records.",
            details={"total_elements": total},
        )

    try:
        df = pd.json_normalize(clean, max_level=1)
    except Exception as exc:
        raise ParseError(
            message=f"Could not normalise JSON records into a DataFrame: {exc}",
            details={},
        ) from exc

    return df, total, corrupt_indices, warnings


def _parse_jsonl(raw: bytes, encoding: str) -> tuple[pd.DataFrame, int, list[int], list[str]]:
    """Parse newline-delimited JSON (one JSON object per line)."""
    warnings: list[str] = []
    text = raw.decode(encoding, errors="replace")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    total = len(lines)

    clean: list[dict] = []
    corrupt_indices: list[int] = []

    for i, line in enumerate(lines):
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                clean.append(obj)
            else:
                corrupt_indices.append(i)
        except json.JSONDecodeError:
            corrupt_indices.append(i)

    if corrupt_indices:
        warnings.append(f"Skipped {len(corrupt_indices)} unparseable JSONL line(s).")

    if not clean:
        raise EmptyDatasetError(
            message="JSONL file contains no valid object records.",
            details={"total_lines": total},
        )

    df = pd.json_normalize(clean, max_level=1)
    return df, total, corrupt_indices, warnings


# ── Excel parsing ──────────────────────────────────────────────────────────

def _parse_excel(raw: bytes) -> tuple[pd.DataFrame, int, list[int], list[str]]:
    warnings: list[str] = []

    def _try_open(engine: str) -> pd.ExcelFile:
        return pd.ExcelFile(io.BytesIO(raw), engine=engine)

    xl: pd.ExcelFile | None = None
    for engine in ("openpyxl", "xlrd"):
        try:
            xl = _try_open(engine)
            break
        except Exception as exc:
            logger.debug("excel_engine_failed", engine=engine, error=str(exc))

    if xl is None:
        raise ParseError(
            message="Cannot open Excel file with any supported engine (openpyxl, xlrd).",
            details={},
        )

    sheet_names = xl.sheet_names
    if not sheet_names:
        raise EmptyDatasetError(message="Excel workbook has no sheets.", details={})

    df: pd.DataFrame | None = None
    used_sheet: str = sheet_names[0]

    for sheet in sheet_names:
        try:
            candidate = xl.parse(sheet, dtype_backend="numpy_nullable")
            if not candidate.empty:
                df = candidate
                used_sheet = sheet
                break
        except Exception as exc:
            warnings.append(f"Sheet '{sheet}' could not be read: {exc}")

    if df is None or df.empty:
        raise EmptyDatasetError(
            message="All Excel sheets are empty or unreadable.",
            details={"sheets": sheet_names},
        )

    if len(sheet_names) > 1:
        skipped = [s for s in sheet_names if s != used_sheet]
        warnings.append(
            f"Multi-sheet workbook: using sheet '{used_sheet}'. Ignored: {skipped}."
        )

    before = len(df)
    df = df.dropna(how="all")
    blank_dropped = before - len(df)
    if blank_dropped:
        warnings.append(f"Dropped {blank_dropped} completely blank row(s) from Excel sheet.")

    return df, before, [], warnings


# ── Public API ─────────────────────────────────────────────────────────────

def parse(
    content: bytes,
    filename: str,
    corrupt_row_threshold: float = 0.20,
) -> ParseResult:
    """
    Parse *content* bytes into a :class:`ParseResult`.

    Args:
        content:               Raw file bytes.
        filename:              Original filename — used only for format detection.
        corrupt_row_threshold: Fraction of rows that may be corrupt before
                               raising :exc:`CorruptRowsError` (default 20 %).
                               Set to ``1.0`` to always recover whatever is left.

    Returns:
        :class:`ParseResult` with a fully normalised DataFrame.

    Raises:
        :exc:`UnsupportedFormatError`  — unknown file extension.
        :exc:`ParseError`              — file is structurally unreadable.
        :exc:`EmptyDatasetError`       — file has no rows / columns after parsing.
        :exc:`CorruptRowsError`        — too many rows are corrupt.
        :exc:`SchemaError`             — JSON topology is non-tabular.
    """
    if not content:
        raise ParseError(
            message="File content is empty (0 bytes).",
            details={"filename": filename},
        )

    fmt = detect_format(filename)

    # Encoding detection (not meaningful for Excel binary formats).
    enc_result = detect_encoding(content)
    encoding = enc_result.encoding
    logger.info(
        "encoding_resolved",
        filename=filename,
        encoding=encoding,
        confidence=enc_result.confidence,
        method=enc_result.method,
    )

    # ── Dispatch ──────────────────────────────────────────────────────────
    try:
        if fmt == "csv":
            df, rows_total, corrupt_indices, parse_warnings = _parse_csv(content, encoding)
        elif fmt == "json":
            df, rows_total, corrupt_indices, parse_warnings = _parse_json(content, encoding)
        elif fmt == "jsonl":
            df, rows_total, corrupt_indices, parse_warnings = _parse_jsonl(content, encoding)
        elif fmt == "excel":
            df, rows_total, corrupt_indices, parse_warnings = _parse_excel(content)
            encoding = "binary"
        else:
            raise UnsupportedFormatError(
                message=f"No parser implemented for format '{fmt}'.",
                details={"format": fmt},
            )
    except (ParseError, EmptyDatasetError, SchemaError, UnsupportedFormatError, CorruptRowsError):
        raise
    except Exception as exc:
        raise ParseError(
            message=f"Unexpected error while parsing '{filename}': {exc}",
            details={"filename": filename, "format": fmt},
        ) from exc

    # ── Post-parse guards ──────────────────────────────────────────────────
    if df is None or df.empty:
        raise EmptyDatasetError(
            message=f"Parsed DataFrame from '{filename}' is empty.",
            details={"filename": filename, "format": fmt},
        )

    if len(df.columns) == 0:
        raise EmptyDatasetError(
            message="DataFrame has no columns.",
            details={"filename": filename},
        )

    # ── Corruption threshold ───────────────────────────────────────────────
    rows_dropped = len(corrupt_indices)
    if rows_total > 0 and rows_dropped / rows_total > corrupt_row_threshold:
        raise CorruptRowsError(
            message=(
                f"{rows_dropped}/{rows_total} rows "
                f"({rows_dropped / rows_total:.1%}) are corrupt, "
                f"exceeding the {corrupt_row_threshold:.0%} tolerance threshold."
            ),
            details={
                "rows_total": rows_total,
                "rows_dropped": rows_dropped,
                "threshold": corrupt_row_threshold,
                "dataframe": df,    # caller may still use it
            },
        )

    # ── Column normalisation ───────────────────────────────────────────────
    df = _apply_column_normalisation(df)
    df = df.reset_index(drop=True)

    logger.info(
        "parse_complete",
        filename=filename,
        format=fmt,
        rows=len(df),
        columns=len(df.columns),
        rows_dropped=rows_dropped,
        encoding=encoding,
    )

    return ParseResult(
        dataframe=df,
        format=fmt,
        encoding=encoding,
        encoding_confidence=enc_result.confidence,
        rows_total=rows_total,
        rows_dropped=rows_dropped,
        corrupt_row_indices=corrupt_indices,
        warnings=parse_warnings,
    )
