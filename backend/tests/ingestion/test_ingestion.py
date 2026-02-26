"""
Tests for the ingestion module.

Covers: parser, encoding detector, column normaliser, validator, and exceptions.

Run with:
    pytest backend/tests/ingestion/ -v
"""
from __future__ import annotations

import codecs
import csv
import io
import json

import pandas as pd
import pytest

# ── Helpers ────────────────────────────────────────────────────────────────

def _csv_bytes(rows: list[list], sep: str = ",", encoding: str = "utf-8") -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=sep)
    writer.writerows(rows)
    return buf.getvalue().encode(encoding)


def _json_bytes(data, encoding: str = "utf-8") -> bytes:
    return json.dumps(data).encode(encoding)


def _jsonl_bytes(records: list[dict], encoding: str = "utf-8") -> bytes:
    return "\n".join(json.dumps(r) for r in records).encode(encoding)


# ══════════════════════════════════════════════════════════════════════════════
# normalizer
# ══════════════════════════════════════════════════════════════════════════════

class TestNormaliseColumns:
    from app.modules.ingestion.normalizer import normalise_columns

    def test_basic_lowercase_and_space(self):
        from app.modules.ingestion.normalizer import normalise_columns
        assert normalise_columns(["First Name", "Last Name"]) == ["first_name", "last_name"]

    def test_special_chars_become_underscore(self):
        from app.modules.ingestion.normalizer import normalise_columns
        assert normalise_columns(["price ($)", "% change"]) == ["price_", "_change"]

    def test_accented_characters_transliterated(self):
        from app.modules.ingestion.normalizer import normalise_columns
        result = normalise_columns(["Ãge", "naïve"])
        assert result == ["age", "naive"]

    def test_leading_digit_prefixed(self):
        from app.modules.ingestion.normalizer import normalise_columns
        assert normalise_columns(["123id"]) == ["col_123id"]

    def test_empty_name_becomes_unnamed(self):
        from app.modules.ingestion.normalizer import normalise_columns
        assert normalise_columns([""]) == ["unnamed_0"]

    def test_nan_name_becomes_unnamed(self):
        from app.modules.ingestion.normalizer import normalise_columns
        assert normalise_columns(["nan"]) == ["unnamed_0"]

    def test_deduplication(self):
        from app.modules.ingestion.normalizer import normalise_columns
        result = normalise_columns(["Name", "name", "NAME"])
        assert result[0] == "name"
        assert result[1] == "name_2"
        assert result[2] == "name_3"

    def test_multiple_underscores_collapsed(self):
        from app.modules.ingestion.normalizer import normalise_columns
        assert normalise_columns(["a  b  c"]) == ["a_b_c"]

    def test_unicode_symbols_removed(self):
        from app.modules.ingestion.normalizer import normalise_columns
        result = normalise_columns(["revenue€", "count★"])
        assert result == ["revenue", "count"]


# ══════════════════════════════════════════════════════════════════════════════
# encoding detector
# ══════════════════════════════════════════════════════════════════════════════

class TestEncodingDetection:
    def test_utf8_bom_detected(self):
        from app.modules.ingestion.encoding import detect
        raw = codecs.BOM_UTF8 + b"hello"
        result = detect(raw)
        assert result.encoding == "utf-8-sig"
        assert result.method == "bom"

    def test_utf16_le_bom_detected(self):
        from app.modules.ingestion.encoding import detect
        raw = codecs.BOM_UTF16_LE + "hello".encode("utf-16-le")
        result = detect(raw)
        assert result.encoding == "utf-16-le"
        assert result.method == "bom"

    def test_plain_ascii_falls_back_to_utf8(self):
        from app.modules.ingestion.encoding import detect
        raw = b"id,name\n1,Alice\n2,Bob"
        result = detect(raw)
        # chardet or fallback — either way should be utf-8 or ascii→utf-8
        assert "utf" in result.encoding.lower() or result.encoding.lower() == "ascii"

    def test_latin1_bytes_handled(self):
        from app.modules.ingestion.encoding import detect
        raw = "café,naïve".encode("latin-1")
        result = detect(raw)
        assert result.encoding is not None


# ══════════════════════════════════════════════════════════════════════════════
# parser — CSV
# ══════════════════════════════════════════════════════════════════════════════

class TestParseCSV:
    def test_basic_csv(self):
        from app.modules.ingestion.parser import parse
        raw = _csv_bytes([["id", "name", "score"], ["1", "Alice", "95"], ["2", "Bob", "87"]])
        result = parse(raw, "data.csv")
        assert list(result.dataframe.columns) == ["id", "name", "score"]
        assert len(result.dataframe) == 2
        assert result.format == "csv"

    def test_semicolon_separator(self):
        from app.modules.ingestion.parser import parse
        raw = _csv_bytes([["a", "b"], ["1", "2"]], sep=";")
        result = parse(raw, "data.csv")
        assert list(result.dataframe.columns) == ["a", "b"]

    def test_tab_separated(self):
        from app.modules.ingestion.parser import parse
        raw = _csv_bytes([["x", "y"], ["10", "20"]], sep="\t")
        result = parse(raw, "data.tsv")
        assert list(result.dataframe.columns) == ["x", "y"]

    def test_column_names_normalised(self):
        from app.modules.ingestion.parser import parse
        raw = _csv_bytes([["First Name", "Last Name"], ["Alice", "Smith"]])
        result = parse(raw, "data.csv")
        assert "first_name" in result.dataframe.columns
        assert "last_name" in result.dataframe.columns

    def test_corrupt_rows_skipped_within_threshold(self):
        from app.modules.ingestion.parser import parse
        # Row 3 has too many fields (corrupt)
        content = b"id,name,score\n1,Alice,95\n2,Bob,87,extra\n3,Carol,71"
        result = parse(content, "data.csv", corrupt_row_threshold=0.5)
        # Should still succeed — 1 corrupt row out of 3 = 33% < 50%
        assert len(result.dataframe) >= 2
        assert result.rows_dropped >= 0

    def test_corrupt_rows_exceed_threshold_raises(self):
        from app.modules.ingestion.parser import parse
        from app.modules.ingestion.exceptions import CorruptRowsError
        # All rows have wrong column count
        content = b"id,name\n1,Alice,extra\n2,Bob,extra\n3,Carol,extra"
        with pytest.raises(CorruptRowsError) as exc_info:
            parse(content, "data.csv", corrupt_row_threshold=0.0)
        assert exc_info.value.code == "CORRUPT_ROWS_EXCEEDED_THRESHOLD"

    def test_empty_file_raises(self):
        from app.modules.ingestion.parser import parse
        from app.modules.ingestion.exceptions import ParseError
        with pytest.raises(ParseError):
            parse(b"", "data.csv")

    def test_utf8_bom_csv(self):
        from app.modules.ingestion.parser import parse
        raw = codecs.BOM_UTF8 + b"id,name\n1,Alice"
        result = parse(raw, "data.csv")
        assert len(result.dataframe) == 1

    def test_latin1_csv(self):
        from app.modules.ingestion.parser import parse
        raw = "id,city\n1,München\n2,Zürich".encode("latin-1")
        result = parse(raw, "data.csv")
        assert len(result.dataframe) == 2

    def test_unsupported_extension_raises(self):
        from app.modules.ingestion.parser import parse
        from app.modules.ingestion.exceptions import UnsupportedFormatError
        with pytest.raises(UnsupportedFormatError) as exc_info:
            parse(b"data", "data.parquet")
        assert exc_info.value.code == "UNSUPPORTED_FORMAT"


# ══════════════════════════════════════════════════════════════════════════════
# parser — JSON
# ══════════════════════════════════════════════════════════════════════════════

class TestParseJSON:
    def test_array_of_objects(self):
        from app.modules.ingestion.parser import parse
        raw = _json_bytes([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
        result = parse(raw, "data.json")
        assert len(result.dataframe) == 2
        assert "id" in result.dataframe.columns

    def test_nested_data_key_unwrapped(self):
        from app.modules.ingestion.parser import parse
        raw = _json_bytes({"data": [{"a": 1}, {"a": 2}], "meta": {"count": 2}})
        result = parse(raw, "data.json")
        assert len(result.dataframe) == 2

    def test_plain_dict_becomes_single_row(self):
        from app.modules.ingestion.parser import parse
        raw = _json_bytes({"id": 1, "name": "Alice"})
        result = parse(raw, "data.json")
        assert len(result.dataframe) == 1

    def test_invalid_json_raises(self):
        from app.modules.ingestion.parser import parse
        from app.modules.ingestion.exceptions import ParseError
        with pytest.raises(ParseError) as exc_info:
            parse(b"{bad json[[", "data.json")
        assert exc_info.value.code == "PARSE_FAILED"

    def test_non_object_elements_skipped(self):
        from app.modules.ingestion.parser import parse
        raw = _json_bytes([{"a": 1}, "corrupted", {"a": 3}])
        result = parse(raw, "data.json")
        assert len(result.dataframe) == 2
        assert result.rows_dropped == 1

    def test_all_non_object_raises(self):
        from app.modules.ingestion.parser import parse
        from app.modules.ingestion.exceptions import EmptyDatasetError
        raw = _json_bytes(["a", "b", "c"])
        with pytest.raises(EmptyDatasetError):
            parse(raw, "data.json")

    def test_column_names_normalised(self):
        from app.modules.ingestion.parser import parse
        raw = _json_bytes([{"First Name": "Alice", "Last Name": "Smith"}])
        result = parse(raw, "data.json")
        assert "first_name" in result.dataframe.columns

    def test_unsupported_json_root_type(self):
        from app.modules.ingestion.parser import parse
        from app.modules.ingestion.exceptions import SchemaError
        raw = b'"just a string"'
        with pytest.raises(SchemaError):
            parse(raw, "data.json")


# ══════════════════════════════════════════════════════════════════════════════
# parser — JSONL
# ══════════════════════════════════════════════════════════════════════════════

class TestParseJSONL:
    def test_basic_jsonl(self):
        from app.modules.ingestion.parser import parse
        raw = _jsonl_bytes([{"id": 1, "v": "a"}, {"id": 2, "v": "b"}])
        result = parse(raw, "data.jsonl")
        assert len(result.dataframe) == 2
        assert result.format == "jsonl"

    def test_corrupt_line_skipped(self):
        from app.modules.ingestion.parser import parse
        content = b'{"id": 1}\n{bad line}\n{"id": 3}'
        result = parse(content, "data.ndjson", corrupt_row_threshold=1.0)
        assert len(result.dataframe) == 2
        assert result.rows_dropped == 1


# ══════════════════════════════════════════════════════════════════════════════
# exceptions
# ══════════════════════════════════════════════════════════════════════════════

class TestExceptions:
    def test_to_dict(self):
        from app.modules.ingestion.exceptions import ParseError
        exc = ParseError(message="oops", details={"file": "x.csv"})
        d = exc.to_dict()
        assert d["code"] == "PARSE_FAILED"
        assert d["message"] == "oops"
        assert d["details"]["file"] == "x.csv"

    def test_str(self):
        from app.modules.ingestion.exceptions import UnsupportedFormatError
        exc = UnsupportedFormatError(message="bad ext")
        assert str(exc) == "bad ext"

    def test_is_base_type(self):
        from app.modules.ingestion.exceptions import IngestionError, ValidationError
        exc = ValidationError(message="too big")
        assert isinstance(exc, IngestionError)
        assert isinstance(exc, Exception)

    def test_corrupt_rows_carries_dataframe(self):
        from app.modules.ingestion.exceptions import CorruptRowsError
        df = pd.DataFrame({"a": [1, 2]})
        exc = CorruptRowsError(message="too many", details={"dataframe": df})
        assert exc.details["dataframe"] is df


# ══════════════════════════════════════════════════════════════════════════════
# validator
# ══════════════════════════════════════════════════════════════════════════════

class TestValidator:
    def _make_df(self, rows: int = 10, cols: int = 3) -> pd.DataFrame:
        import numpy as np
        data = {f"col_{i}": range(rows) for i in range(cols)}
        return pd.DataFrame(data)

    def test_valid_df_passes(self):
        from app.modules.ingestion.validator import validate
        validate(self._make_df(), file_size_bytes=100)

    def test_file_too_large_raises(self):
        from app.modules.ingestion.validator import validate
        from app.modules.ingestion.exceptions import ValidationError
        with pytest.raises(ValidationError) as exc_info:
            validate(self._make_df(), file_size_bytes=200 * 1024 * 1024)
        assert exc_info.value.code == "FILE_TOO_LARGE"

    def test_empty_df_raises(self):
        from app.modules.ingestion.validator import validate
        from app.modules.ingestion.exceptions import ValidationError
        with pytest.raises(ValidationError) as exc_info:
            validate(pd.DataFrame(), file_size_bytes=0)
        assert exc_info.value.code == "EMPTY_DATASET"

    def test_too_many_rows_raises(self, monkeypatch):
        from app.modules.ingestion.validator import validate
        from app.modules.ingestion.exceptions import ValidationError
        from app.core import config
        monkeypatch.setattr(config.settings, "MAX_ROWS", 5)
        with pytest.raises(ValidationError) as exc_info:
            validate(self._make_df(rows=10), file_size_bytes=100)
        assert exc_info.value.code == "TOO_MANY_ROWS"

    def test_too_many_columns_raises(self, monkeypatch):
        from app.modules.ingestion.validator import validate
        from app.modules.ingestion.exceptions import ValidationError
        from app.core import config
        monkeypatch.setattr(config.settings, "MAX_COLUMNS", 2)
        with pytest.raises(ValidationError) as exc_info:
            validate(self._make_df(cols=5), file_size_bytes=100)
        assert exc_info.value.code == "TOO_MANY_COLUMNS"
