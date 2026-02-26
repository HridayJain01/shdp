"""Versioned, parameterised prompt templates for LLM healing plan generation."""
from __future__ import annotations

# ─── Healing plan prompts (legacy reasoning.py) ───────────────────────────────

HEALING_PLAN_SYSTEM = """\
You are an expert data quality engineer. Your job is to analyse a dataset profile and
a list of detected anomalies, then produce a precise, ordered healing plan.

Rules:
- Return ONLY valid JSON — no markdown, no commentary.
- Each action must target a SPECIFIC column or be row-level.
- Order actions by priority (1 = must fix first).
- Choose the most conservative strategy that resolves the issue.
- Provide a clear, human-readable rationale for each action.
"""

HEALING_PLAN_USER = """\
Dataset Profile Summary:
{profile_summary}

Detected Anomalies:
{anomaly_list}

Return a JSON object with this exact structure:
{{
  "overall_rationale": "<1-2 sentence executive summary>",
  "actions": [
    {{
      "action_id": "<short_id>",
      "column": "<column_name or null for row-level>",
      "strategy": "<strategy_name>",
      "parameters": {{ }},
      "rationale": "<why this strategy>",
      "priority": <int>,
      "estimated_impact": <float 0-1>
    }}
  ]
}}

Available strategies:
mean_imputation, median_imputation, mode_imputation, constant_imputation,
interpolation, drop_rows, drop_column, zscore_clamp, iqr_clamp,
deduplication, type_cast, format_standardize, normalize
"""


# ─── AI agent prompts ─────────────────────────────────────────────────────────

AGENT_SYSTEM = """\
You are a senior data quality engineer AI agent.

Your task is to analyse a dataset profile and an anomaly report, then produce a
complete, structured healing strategy that can be executed automatically.

Strict rules:
- Return ONLY a valid JSON object — no markdown fences, no prose, no commentary.
- Every field listed in the schema is required.
- healing_plan must contain at least one step.
- transformation_order must list strategy names in the exact order they should run.
- confidence_score must be a float between 0.0 and 1.0 reflecting your certainty.
- value_corrections should be representative examples (up to 20); not every row.
- All column names must exactly match those in the dataset profile.
- Choose the most conservative yet effective strategy for each issue.
"""

AGENT_USER = """\
## Dataset Profile
{profile_json}

## Anomaly Report
{anomaly_json}

Respond with a JSON object that matches this EXACT schema — all keys are required:

{{
  "healing_plan": [
    {{
      "step_id":          "<step_01, step_02, …>",
      "title":            "<short human-readable title>",
      "strategy":         "<strategy_name>",
      "target_columns":   ["<col>", …],
      "rationale":        "<why this step>",
      "priority":         <int starting at 1>,
      "estimated_impact": <float 0.0–1.0>,
      "parameters":       {{}}
    }}
  ],
  "column_fixes": [
    {{
      "column":                  "<column_name>",
      "detected_issue":          "<description>",
      "fix_type":                "<impute|cast|normalize|deduplicate|cap|format_fix|drop>",
      "target_dtype":            "<dtype string or null>",
      "parameters":              {{}},
      "severity":                "<critical|high|medium|low>",
      "expected_null_reduction": <float 0.0–1.0 or null>
    }}
  ],
  "value_corrections": [
    {{
      "column":           "<column_name>",
      "original_value":   <value>,
      "corrected_value":  <value>,
      "correction_type":  "<format_fix|outlier_cap|imputation|type_cast>",
      "reason":           "<brief explanation>",
      "row_index":        <int or null>
    }}
  ],
  "transformation_order": ["<strategy_1>", "<strategy_2>", …],
  "confidence_score": <float 0.0–1.0>
}}

Available strategy names:
deduplication, median_imputation, mean_imputation, mode_imputation,
constant_imputation, interpolation, forward_fill, backward_fill,
drop_rows, drop_column, iqr_clamp, zscore_clamp, percentile_clamp,
type_cast, format_standardize, category_normalize, normalize
"""

