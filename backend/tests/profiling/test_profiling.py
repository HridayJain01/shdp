"""Tests for the profiling module.

Covers:
  • Schema / semantic-type detection (schema_detector.py)
  • Per-column profiling             (profiler.py — profile_column)
  • Dataset-level profiling          (profiler.py — profile_dataset)
  • Inferred constraints             (profiler.py — _build_constraints)
  • Output models                    (models/profile.py)
"""
from __future__ import annotations

import uuid
from typing import Any

import numpy as np
import pandas as pd
import pytest

from app.models.profile import (
    ColumnProfile,
    DatasetProfile,
    HistogramBucket,
    InferredConstraints,
    NumericStats,
    StringStats,
    DatetimeStats,
    TopValue,
)
from app.modules.profiling.profiler import profile_column, profile_dataset
from app.modules.profiling.schema_detector import SemanticTypeResult, detect_semantic_type


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

def _series(values: list[Any], name: str = "col", dtype=None) -> pd.Series:
    s = pd.Series(values, name=name)
    return s.astype(dtype) if dtype else s


# ─────────────────────────────────────────────────────────────────────────────
# 1. Semantic-type detection
# ─────────────────────────────────────────────────────────────────────────────

class TestSemanticTypeDetection:
    def test_boolean_pandas_dtype(self):
        s = _series([True, False, True], dtype=bool)
        r = detect_semantic_type(s)
        assert r.type == "boolean"
        assert r.confidence == 1.0

    def test_boolean_string_values(self):
        s = _series(["yes", "no", "YES", "NO"] * 25)
        r = detect_semantic_type(s)
        assert r.type == "boolean"

    def test_uuid_detection(self):
        uuids = [str(uuid.uuid4()) for _ in range(50)]
        s = _series(uuids)
        r = detect_semantic_type(s)
        assert r.type == "uuid"
        assert r.confidence >= 0.90

    def test_email_detection(self):
        emails = [f"user{i}@example.com" for i in range(100)]
        s = _series(emails)
        r = detect_semantic_type(s)
        assert r.type == "email"

    def test_phone_detection(self):
        phones = ["555-123-4567", "555-987-6543", "(555) 111-2222"] * 40
        s = _series(phones)
        r = detect_semantic_type(s)
        assert r.type == "phone"

    def test_ip_v4_detection(self):
        ips = [f"192.168.{i}.{j}" for i in range(10) for j in range(10)]
        s = _series(ips)
        r = detect_semantic_type(s)
        assert r.type == "ip_address"

    def test_url_detection(self):
        urls = [f"https://example.com/page/{i}" for i in range(100)]
        s = _series(urls)
        r = detect_semantic_type(s)
        assert r.type == "url"

    def test_currency_detection(self):
        amounts = ["$1,200.00", "$450.99", "€350.00", "$99.99"] * 30
        s = _series(amounts)
        r = detect_semantic_type(s)
        assert r.type == "currency"

    def test_percentage_detection(self):
        pcts = ["12%", "45.5%", "100%", "0%"] * 30
        s = _series(pcts)
        r = detect_semantic_type(s)
        assert r.type == "percentage"

    def test_datetime_pandas_dtype(self):
        dates = pd.Series(pd.date_range("2020-01-01", periods=50, freq="D"), name="ts")
        r = detect_semantic_type(dates)
        assert r.type == "datetime"
        assert r.confidence == 1.0

    def test_date_string_detection(self):
        dates = [f"2024-{m:02d}-{d:02d}" for m in range(1, 13) for d in range(1, 5)]
        s = _series(dates)
        r = detect_semantic_type(s)
        assert r.type in ("date", "datetime")

    def test_zipcode_us_detection(self):
        zips = [f"{90000 + i:05d}" for i in range(100)]
        s = _series(zips)
        r = detect_semantic_type(s)
        assert r.type == "zipcode"

    def test_categorical_low_cardinality(self):
        values = ["red", "green", "blue"] * 100
        s = _series(values)
        r = detect_semantic_type(s)
        assert r.type == "categorical"

    def test_numeric_dtype_detection(self):
        s = _series([1.5, 2.3, 4.7, 0.1], dtype=float)
        r = detect_semantic_type(s)
        assert r.type == "numeric"

    def test_numeric_id_high_cardinality(self):
        ids = [str(i) for i in range(1000)]
        s = _series(ids)
        r = detect_semantic_type(s)
        assert r.type == "numeric_id"

    def test_json_string_detection(self):
        jsons = ['{"key": "value"}', '[1, 2, 3]', '{"a": 1}'] * 30
        s = _series(jsons)
        r = detect_semantic_type(s)
        assert r.type == "json_string"

    def test_text_fallback(self):
        texts = [f"Product description number {i} with some long text." for i in range(100)]
        s = _series(texts)
        r = detect_semantic_type(s)
        assert r.type == "text"

    def test_result_has_all_fields(self):
        s = _series(["hello", "world"] * 50)
        r = detect_semantic_type(s)
        assert isinstance(r, SemanticTypeResult)
        assert isinstance(r.type, str)
        assert 0.0 <= r.confidence <= 1.0
        assert isinstance(r.evidence, str)


# ─────────────────────────────────────────────────────────────────────────────
# 2. Null / completeness stats
# ─────────────────────────────────────────────────────────────────────────────

class TestNullStats:
    def test_no_nulls(self):
        cp = profile_column(_series([1, 2, 3, 4, 5]))
        assert cp.null_count == 0
        assert cp.null_pct == 0.0
        assert cp.non_null_count == 5

    def test_all_nulls(self):
        cp = profile_column(_series([None, None, None]))
        assert cp.null_count == 3
        assert cp.null_pct == 100.0
        assert cp.non_null_count == 0

    def test_partial_nulls(self):
        cp = profile_column(_series([1.0, None, 3.0, None]))
        assert cp.null_count == 2
        assert cp.null_pct == 50.0
        assert cp.non_null_count == 2

    def test_null_pct_is_percentage_not_fraction(self):
        """Ensure null_pct is 0–100, not 0–1."""
        cp = profile_column(_series([None, 1, 2, 3]))
        assert 0.0 <= cp.null_pct <= 100.0
        assert cp.null_pct == 25.0


# ─────────────────────────────────────────────────────────────────────────────
# 3. Cardinality
# ─────────────────────────────────────────────────────────────────────────────

class TestCardinality:
    def test_unique_pct(self):
        cp = profile_column(_series(["a", "a", "b", "c"]))
        # 3 unique out of 4 non-null = 75 %
        assert cp.unique_count == 3
        assert cp.unique_pct == 75.0

    def test_all_unique(self):
        cp = profile_column(_series([10, 20, 30, 40]))
        assert cp.unique_count == 4
        assert cp.unique_pct == 100.0


# ─────────────────────────────────────────────────────────────────────────────
# 4. Top values
# ─────────────────────────────────────────────────────────────────────────────

class TestTopValues:
    def test_top_5_cap(self):
        values = list("abcdefghij") * 10     # 10 unique chars, evenly distributed
        cp = profile_column(_series(values))
        assert len(cp.top_values) <= 5

    def test_top_value_structure(self):
        cp = profile_column(_series(["x", "x", "y"]))
        tv = cp.top_values[0]
        assert isinstance(tv, TopValue)
        assert tv.value == "x"
        assert tv.count == 2
        # pct of total rows (3)
        assert tv.pct == pytest.approx(2 / 3 * 100, abs=0.1)

    def test_most_frequent_first(self):
        cp = profile_column(_series(["a"] * 10 + ["b"] * 5 + ["c"]))
        assert cp.top_values[0].value == "a"


# ─────────────────────────────────────────────────────────────────────────────
# 5. Numeric stats
# ─────────────────────────────────────────────────────────────────────────────

class TestNumericStats:
    @pytest.fixture
    def col(self):
        return profile_column(_series(list(range(1, 11)), dtype=float))

    def test_stats_populated(self, col):
        assert col.numeric_stats is not None

    def test_min_max(self, col):
        assert col.numeric_stats.min == 1.0
        assert col.numeric_stats.max == 10.0

    def test_mean_median(self, col):
        assert col.numeric_stats.mean == pytest.approx(5.5)
        assert col.numeric_stats.median == pytest.approx(5.5)

    def test_std(self, col):
        assert col.numeric_stats.std > 0

    def test_quartiles(self, col):
        assert col.numeric_stats.q1 < col.numeric_stats.median
        assert col.numeric_stats.q3 > col.numeric_stats.median

    def test_iqr(self, col):
        ns = col.numeric_stats
        assert ns.iqr == pytest.approx(ns.q3 - ns.q1)

    def test_variance(self, col):
        ns = col.numeric_stats
        assert ns.variance == pytest.approx(ns.std ** 2, rel=1e-4)

    def test_zeros_and_negatives(self):
        cp = profile_column(_series([-3.0, -1.0, 0.0, 1.0, 2.0]))
        assert cp.numeric_stats.zeros == 1
        assert cp.numeric_stats.negatives == 2

    def test_inf_handling(self):
        cp = profile_column(_series([1.0, float("inf"), 2.0, float("-inf"), 3.0]))
        assert cp.numeric_stats is not None
        assert cp.numeric_stats.inf_count >= 2   # at least 2 non-finite values

    def test_no_numeric_stats_for_strings(self):
        cp = profile_column(_series(["a", "b", "c"]))
        assert cp.numeric_stats is None


# ─────────────────────────────────────────────────────────────────────────────
# 6. String stats
# ─────────────────────────────────────────────────────────────────────────────

class TestStringStats:
    @pytest.fixture
    def col(self):
        return profile_column(_series(["hi", "hello", "hey", ""], name="greet"))

    def test_string_stats_populated(self, col):
        assert col.string_stats is not None

    def test_blank_count(self, col):
        assert col.string_stats.blank_count == 1

    def test_lengths(self, col):
        # "hello" is the longest
        assert col.string_stats.max_length == 5

    def test_no_string_stats_for_numeric(self):
        cp = profile_column(_series([1, 2, 3], dtype=float))
        assert cp.string_stats is None


# ─────────────────────────────────────────────────────────────────────────────
# 7. Datetime stats
# ─────────────────────────────────────────────────────────────────────────────

class TestDatetimeStats:
    def test_datetime_stats_populated(self):
        s = pd.Series(pd.date_range("2023-01-01", periods=10, freq="D"), name="date")
        cp = profile_column(s)
        assert cp.datetime_stats is not None
        assert "2023-01-01" in cp.datetime_stats.min
        assert cp.datetime_stats.range_days == 9.0

    def test_no_datetime_stats_for_numeric(self):
        cp = profile_column(_series([1.0, 2.0, 3.0]))
        assert cp.datetime_stats is None


# ─────────────────────────────────────────────────────────────────────────────
# 8. Histogram
# ─────────────────────────────────────────────────────────────────────────────

class TestHistogram:
    def test_numeric_histogram(self):
        cp = profile_column(_series(list(range(100)), dtype=float))
        assert len(cp.histogram) > 0
        assert all(isinstance(b, HistogramBucket) for b in cp.histogram)

    def test_histogram_buckets_cover_range(self):
        cp = profile_column(_series(list(range(1, 11)), dtype=float))
        assert cp.histogram[0].bucket_start >= 1.0
        assert cp.histogram[-1].bucket_end <= 10.1   # small float tolerance

    def test_no_histogram_for_text(self):
        cp = profile_column(_series(["foo", "bar", "baz"]))
        assert cp.histogram == []


# ─────────────────────────────────────────────────────────────────────────────
# 9. Inferred constraints
# ─────────────────────────────────────────────────────────────────────────────

class TestInferredConstraints:
    def test_nullable_true_when_nulls_present(self):
        cp = profile_column(_series([1.0, None, 3.0]))
        assert cp.constraints.is_nullable is True

    def test_nullable_false_when_no_nulls(self):
        cp = profile_column(_series([1.0, 2.0, 3.0]))
        assert cp.constraints.is_nullable is False

    def test_unique_flag(self):
        cp = profile_column(_series([1, 2, 3, 4]))
        assert cp.constraints.is_unique is True

    def test_not_unique_when_duplicates(self):
        cp = profile_column(_series([1, 1, 2, 3]))
        assert cp.constraints.is_unique is False

    def test_constant_flag(self):
        cp = profile_column(_series([42, 42, 42]))
        assert cp.constraints.is_constant is True

    def test_min_max_for_numeric(self):
        cp = profile_column(_series([5.0, 10.0, 15.0]))
        assert cp.constraints.min_value == 5.0
        assert cp.constraints.max_value == 15.0

    def test_allowed_values_low_cardinality(self):
        cp = profile_column(_series(["red", "green", "blue"] * 10))
        assert cp.constraints.allowed_values is not None
        assert set(cp.constraints.allowed_values) == {"red", "green", "blue"}

    def test_no_allowed_values_high_cardinality(self):
        cp = profile_column(_series([str(i) for i in range(100)]))
        assert cp.constraints.allowed_values is None

    def test_email_pattern_in_constraints(self):
        emails = [f"user{i}@test.com" for i in range(100)]
        cp = profile_column(_series(emails))
        assert cp.constraints.pattern is not None
        assert "@" in cp.constraints.pattern

    def test_string_length_constraints(self):
        cp = profile_column(_series(["ab", "abc", "abcd"]))
        assert cp.constraints.max_length == 4

    def test_constraints_model_type(self):
        cp = profile_column(_series([1, 2, 3]))
        assert isinstance(cp.constraints, InferredConstraints)


# ─────────────────────────────────────────────────────────────────────────────
# 10. Sample values
# ─────────────────────────────────────────────────────────────────────────────

class TestSampleValues:
    def test_sample_values_count(self):
        cp = profile_column(_series(list(range(100))))
        assert 1 <= len(cp.sample_values) <= 5

    def test_sample_values_empty_for_all_null(self):
        cp = profile_column(_series([None, None, None]))
        assert cp.sample_values == []


# ─────────────────────────────────────────────────────────────────────────────
# 11. Column profile – dtype / position metadata
# ─────────────────────────────────────────────────────────────────────────────

class TestColumnMetadata:
    def test_position_set(self):
        cp = profile_column(_series([1, 2, 3]), position=3)
        assert cp.position == 3

    def test_dtype_captured(self):
        cp = profile_column(_series([1.0, 2.0], dtype="float64"))
        assert "float64" in cp.dtype

    def test_dtype_category_numeric(self):
        cp = profile_column(_series([1, 2, 3], dtype=float))
        assert cp.dtype_category == "numeric"

    def test_dtype_category_boolean(self):
        cp = profile_column(_series([True, False, True], dtype=bool))
        assert cp.dtype_category == "boolean"

    def test_dtype_category_datetime(self):
        s = pd.Series(pd.date_range("2020-01-01", periods=5), name="d")
        cp = profile_column(s)
        assert cp.dtype_category == "datetime"

    def test_dtype_category_text(self):
        cp = profile_column(_series(["lorem ipsum " * 5] * 100))
        assert cp.dtype_category == "text"


# ─────────────────────────────────────────────────────────────────────────────
# 12. Dataset-level profiling
# ─────────────────────────────────────────────────────────────────────────────

class TestDatasetProfile:
    @pytest.fixture
    def df(self):
        return pd.DataFrame({
            "id":     list(range(1, 101)),
            "name":   [f"Name {i}" for i in range(100)],
            "score":  [float(i) * 0.5 for i in range(100)],
            "status": ["active", "inactive"] * 50,
            "joined": pd.date_range("2020-01-01", periods=100, freq="D"),
        })

    @pytest.fixture
    def dp(self, df):
        return profile_dataset(df, uuid.uuid4())

    def test_returns_dataset_profile(self, dp):
        assert isinstance(dp, DatasetProfile)

    def test_row_and_column_count(self, df, dp):
        assert dp.row_count == len(df)
        assert dp.column_count == len(df.columns)

    def test_column_count_in_profiles(self, df, dp):
        assert len(dp.columns) == len(df.columns)

    def test_duplicate_rows(self, dp):
        assert dp.duplicate_rows == 0

    def test_duplicate_rows_detected(self):
        df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
        dp = profile_dataset(df, uuid.uuid4())
        assert dp.duplicate_rows == 1
        assert dp.duplicate_pct == pytest.approx(1 / 3 * 100, abs=0.1)

    def test_complete_rows(self, dp):
        assert dp.complete_rows == 100
        assert dp.complete_row_pct == 100.0

    def test_complete_rows_with_nulls(self):
        df = pd.DataFrame({"a": [1, None, 3], "b": [4, 5, 6]})
        dp = profile_dataset(df, uuid.uuid4())
        assert dp.complete_rows == 2

    def test_numeric_column_count(self, dp):
        assert dp.numeric_column_count >= 1   # score + id (numeric_id or numeric)

    def test_datetime_column_count(self, dp):
        assert dp.datetime_column_count == 1

    def test_memory_mb_positive(self, dp):
        assert dp.memory_mb > 0

    def test_column_positions(self, df, dp):
        for i, col in enumerate(dp.columns):
            assert col.position == i

    def test_json_serialisable(self, dp):
        """model_dump() should produce a plain dict without errors."""
        data = dp.model_dump()
        assert isinstance(data, dict)
        assert "columns" in data
        # Every column should have the key fields
        for col in data["columns"]:
            assert "name" in col
            assert "null_pct" in col
            assert "constraints" in col
            assert "top_values" in col

    def test_column_name_matches_df(self, df, dp):
        df_cols = list(df.columns)
        profile_cols = [c.name for c in dp.columns]
        assert df_cols == profile_cols
