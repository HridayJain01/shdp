import axios from "axios";
import type {
  UploadJobResponse,
  JobStatusResponse,
  PipelineResult,
  HealthMetrics,
} from "../types";

// ─── Re-export response types consumers need ───────────────────────────────
export type AIPlanResponse = {
  dataset_id: string;
  job_id: string;
  model_used: string;
  confidence_score: number;
  step_count: number;
  transformation_order: string[];
  agent_result: Record<string, unknown>;
};

export type HealingApplyResponse = {
  dataset_id: string;
  actions_applied: number;
  actions_skipped: number;
  rows_before: number;
  rows_after: number;
  total_corrections: number;
  ai_corrections: number;
  strategies_applied: string[];
  validation_warnings: string[];
  healing_result: Record<string, unknown>;
};

const BASE = import.meta.env.VITE_API_BASE_URL ?? "/api/v1";

const api = axios.create({
  baseURL: BASE,
  headers: { "X-API-Key": import.meta.env.VITE_API_KEY ?? "" },
});

// ── Upload ────────────────────────────────────────────────────────────────────
export const uploadDataset = (file: File): Promise<UploadJobResponse> => {
  const form = new FormData();
  form.append("file", file);
  return api
    .post<UploadJobResponse>("/upload", form, {
      headers: { "Content-Type": "multipart/form-data" },
    })
    .then((r) => r.data);
};

export const getJobStatus = (jobId: string): Promise<JobStatusResponse> =>
  api.get<JobStatusResponse>(`/upload/${jobId}/status`).then((r) => r.data);

export const getFullResult = (jobId: string): Promise<PipelineResult> =>
  api.get<PipelineResult>(`/upload/${jobId}/result`).then((r) => r.data);

// ── Profile ───────────────────────────────────────────────────────────────────
export const getProfile = (jobId: string) =>
  api.get(`/profile`, { params: { job_id: jobId } }).then((r) => r.data);

// ── Anomalies ─────────────────────────────────────────────────────────────────
export const getAnomalies = (jobId: string) =>
  api.get(`/anomalies`, { params: { job_id: jobId } }).then((r) => r.data);

// ── AI plan ───────────────────────────────────────────────────────────────────
export const generateAIPlan = (
  jobId: string,
  opts: { model?: string; temperature?: number; max_tokens?: number } = {}
): Promise<AIPlanResponse> =>
  api
    .post<AIPlanResponse>("/generate-ai-plan", {
      job_id: jobId,
      model: opts.model ?? "anthropic/claude-3.5-sonnet",
      temperature: opts.temperature ?? 0.2,
      max_tokens: opts.max_tokens ?? 2048,
    })
    .then((r) => r.data);

// ── Healing ───────────────────────────────────────────────────────────────────
export const applyHealing = (
  datasetId: string,
  opts: { agent_result?: Record<string, unknown>; use_ai_plan?: boolean } = {}
): Promise<HealingApplyResponse> =>
  api
    .post<HealingApplyResponse>("/apply-healing", {
      dataset_id: datasetId,
      agent_result: opts.agent_result ?? null,
      use_ai_plan: opts.use_ai_plan ?? false,
    })
    .then((r) => r.data);

// ── Quality ───────────────────────────────────────────────────────────────────
export const getQualityScore = (jobId: string) =>
  api.get(`/quality-score`, { params: { job_id: jobId } }).then((r) => r.data);

// ── Health ────────────────────────────────────────────────────────────────────
export const getHealthMetrics = (): Promise<HealthMetrics> =>
  api.get<HealthMetrics>("/health-metrics").then((r) => r.data);

// ── Download ──────────────────────────────────────────────────────────────────
export const downloadCleaned = (datasetId: string) =>
  api
    .get("/download-cleaned", {
      params: { dataset_id: datasetId },
      responseType: "blob",
    })
    .then((r) => r.data);

// ── Reports ───────────────────────────────────────────────────────────────────
export const getComparison = (jobId: string) =>
  api.get(`/reports/${jobId}/comparison`).then((r) => r.data);
export const getCharts = (jobId: string) =>
  api.get(`/reports/${jobId}/charts`).then((r) => r.data);

export default api;
