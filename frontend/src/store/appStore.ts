import { create } from "zustand";
import type { PipelineResult } from "../types";

export type PipelineStep =
  | "idle"
  | "uploading"
  | "parsing"
  | "profiling"
  | "anomaly_detection"
  | "planning"
  | "healing"
  | "scoring"
  | "reporting"
  | "complete"
  | "error";

interface AppState {
  // Upload state
  jobId: string | null;
  datasetId: string | null;
  filename: string | null;
  fileRows: number | null;
  fileColumns: number | null;
  fileSizeBytes: number | null;

  // Pipeline
  step: PipelineStep;
  error: string | null;
  result: PipelineResult | null;

  // Actions
  setJobId: (id: string) => void;
  setUploadMeta: (meta: {
    datasetId: string;
    filename: string;
    rows: number;
    columns: number;
    sizeBytes: number;
  }) => void;
  setStep: (s: PipelineStep) => void;
  setError: (e: string | null) => void;
  setResult: (r: PipelineResult) => void;
  reset: () => void;
}

export const useAppStore = create<AppState>((set) => ({
  jobId: null,
  datasetId: null,
  filename: null,
  fileRows: null,
  fileColumns: null,
  fileSizeBytes: null,
  step: "idle",
  error: null,
  result: null,

  setJobId: (id) => set({ jobId: id }),
  setUploadMeta: ({ datasetId, filename, rows, columns, sizeBytes }) =>
    set({
      datasetId,
      filename,
      fileRows: rows,
      fileColumns: columns,
      fileSizeBytes: sizeBytes,
    }),
  setStep: (step) => set({ step }),
  setError: (error) => set({ error, step: "error" }),
  setResult: (result) => set({ result, step: "complete" }),
  reset: () =>
    set({
      jobId: null,
      datasetId: null,
      filename: null,
      fileRows: null,
      fileColumns: null,
      fileSizeBytes: null,
      step: "idle",
      error: null,
      result: null,
    }),
}));
