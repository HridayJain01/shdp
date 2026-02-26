// ── Upload ────────────────────────────────────────────────────────────────────
export interface UploadJobResponse {
  dataset_id?: string;
  job_id: string;
  filename?: string;
  row_count?: number;
  column_count?: number;
  rows?: number;
  columns?: number;
  size_bytes?: number;
  status?: string;
  message?: string;
}

export interface JobStatusResponse {
  job_id: string;
  state?: string;
  step?: string;
  error?: string;
  message?: string;
}

// ── Profile ───────────────────────────────────────────────────────────────────
export interface TopValue {
  value: unknown;
  count?: number;
  frequency: number; // 0-1
}

export interface NumericStats {
  min?: number;
  max?: number;
  mean?: number;
  median?: number;
  std?: number;
  variance?: number;
  q1?: number;
  q3?: number;
  iqr?: number;
  skewness?: number;
  kurtosis?: number;
  // allow both naming conventions
  zeros?: number;
  zero_count?: number;
  negatives?: number;
}

export interface StringStats {
  min_length?: number;
  max_length?: number;
  mean_length?: number;
  avg_length?: number;
  blank_count?: number;
  empty_count?: number;
  whitespace_count?: number;
}

export interface ColumnProfile {
  name: string;
  dtype?: string;
  dtype_category?: string;
  semantic_type?: string | null;
  null_count?: number;
  null_pct?: number;
  unique_count?: number;
  unique_pct?: number;
  sample_values?: unknown[];
  top_values?: TopValue[];
  numeric_stats?: NumericStats;
  string_stats?: StringStats;
}

export interface DatasetProfile {
  dataset_id?: string;
  row_count?: number;
  column_count?: number;
  total_null_count?: number;
  null_pct?: number;
  duplicate_row_count?: number;
  duplicate_pct?: number;
  memory_mb?: number;
  columns?: ColumnProfile[];
}

// ── Anomalies ─────────────────────────────────────────────────────────────────
export type Severity = "critical" | "high" | "medium" | "low";

export interface Anomaly {
  id?: string;
  column?: string | null;
  column_name?: string | null;
  anomaly_type?: string;
  severity: Severity;
  affected_rows?: number;
  affected_rate?: number;
  description?: string;
  sample_values?: unknown[];
}

export interface AnomalyReport {
  dataset_id?: string;
  total_anomalies?: number;
  columns_affected?: number;
  critical_count?: number;
  high_count?: number;
  medium_count?: number;
  low_count?: number;
  anomalies?: Anomaly[];
}

// ── Healing ───────────────────────────────────────────────────────────────────
export interface HealingAction {
  action_id?: string;
  action_type?: string;
  column?: string | null;
  strategy?: string;
  parameters?: Record<string, unknown>;
  rationale?: string;
  description?: string;
  priority?: number;
  estimated_impact?: number;
  estimated_impact_pct?: number;
}

export interface HealingStep {
  step_id?: string;
  title?: string;
  strategy?: string;
  strategy_name?: string;
  column_name?: string;
  target_columns?: string[];
  rationale?: string;
  priority?: number;
  estimated_impact?: number;
  parameters?: Record<string, unknown>;
  actions?: HealingAction[];
}

export interface HealingPlan {
  dataset_id?: string;
  llm_model?: string;
  overall_rationale?: string;
  confidence_score?: number;
  steps?: HealingStep[];
  actions?: HealingAction[];
}

export interface AgentResponse {
  healing_plan: HealingStep[];
  column_fixes: unknown[];
  value_corrections: unknown[];
  transformation_order: string[];
  confidence_score: number;
}

export interface AgentResult {
  dataset_id: string;
  model_used: string;
  response: AgentResponse;
  prompt_tokens: number;
  completion_tokens: number;
  latency_ms: number;
  ran_at: string;
}

export interface TransformationEntry {
  strategy_name: string;
  operation: string;
  column: string | null;
  corrections: number;
  source: string;
  detail: string;
  validation_warnings: string[];
  applied_at: string;
}

export interface TransformationLog {
  entries: TransformationEntry[];
  total_corrections: number;
  strategies_applied: string[];
  ai_entries: TransformationEntry[];
  ai_total_corrections: number;
}

export interface HealingResult {
  dataset_id: string;
  actions_applied: number;
  actions_skipped: number;
  rows_modified: number;
  healed_dataset_path: string;
  transformation_log: TransformationLog;
  execution_log: Record<string, unknown>[];
  ai_execution_log: Record<string, unknown>[];
  validation_warnings: string[];
}

// ── Quality ───────────────────────────────────────────────────────────────────
export interface QualityDimension {
  dimension?: string;
  label?: string;
  score: number;
  ratio?: number;
  weight: number;
  issue_count?: number;
  affected_columns?: string[];
}

export interface ScoringBreakdown {
  completeness?: QualityDimension;
  uniqueness?: QualityDimension;
  outlier_health?: QualityDimension;
  format_validity?: QualityDimension;
  schema_consistency?: QualityDimension;
  [key: string]: QualityDimension | undefined;
}

export interface PillarScore {
  name: string;
  raw_score: number;
  weighted_score?: number;
  weight?: number;
}

export interface ImprovementSuggestion {
  title?: string;
  description?: string;
  dimension?: string;
  action?: string;
  estimated_gain?: number;
  affected_columns?: string[];
}

export interface QualityDelta {
  before: number;
  after: number;
  delta: number;
  improvement_pct?: number;
}

export interface QualityScore {
  dataset_id?: string;
  overall_score?: number;
  total_score?: number;
  grade?: string;
  pillars?: PillarScore[];
  breakdown?: QualityDimension[];
  improvement_suggestions?: ImprovementSuggestion[];
  improvement_potential?: ImprovementSuggestion[];
  delta?: QualityDelta;
  scored_at?: string;
}

// ── Comparison / Charts ───────────────────────────────────────────────────────
export interface ChangedCell {
  row_index?: number;
  row?: number;
  column?: string;
  col?: string;
  before_value?: unknown;
  before?: unknown;
  after_value?: unknown;
  after?: unknown;
}

export interface ComparisonResult {
  rows_before?: number;
  rows_after?: number;
  row_delta?: number;
  cells_changed?: number;
  changed_cells_count?: number;
  row_count_before?: number;
  row_count_after?: number;
  row_count_delta?: number;
  changed_cells?: ChangedCell[];
  null_rate_chart?: Array<{ column: string; before: number; after: number }>;
  pillar_score_chart?: Array<{ pillar: string; before: number; after: number }>;
}

export interface ChartData {
  null_rate_comparison?: Array<{ column: string; before: number; after: number }>;
  pillar_score_comparison?: Array<{ pillar: string; before: number; after: number }>;
  overall_score?: { before: number; after: number; delta: number };
}

// ── Health ────────────────────────────────────────────────────────────────────
export interface ServiceStatus {
  name?: string;
  status: "ok" | "degraded" | "down" | string;
  latency_ms?: number | null;
  detail?: string | null;
}

export interface HealthMetrics {
  status: string;
  version?: string;
  uptime_seconds?: number;
  services?: Record<string, ServiceStatus> | ServiceStatus[];
  active_workers?: number;
  worker_count?: number;
  cpu_count?: number;
  checked_at?: string;
}

// ── Full pipeline result ──────────────────────────────────────────────────────
export interface PipelineResult {
  dataset_id?: string;
  profile?: DatasetProfile;
  anomaly_report?: AnomalyReport;
  healing_plan?: HealingPlan;
  healing_result?: HealingResult;
  quality_score?: QualityScore;
  quality_delta?: { before: number; after: number; delta: number };
  comparison?: ComparisonResult;
  charts?: ChartData;
}
