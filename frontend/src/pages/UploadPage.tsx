import { useCallback, useEffect, useRef } from "react";
import { useDropzone } from "react-dropzone";
import { useNavigate } from "react-router-dom";
import toast from "react-hot-toast";
import { useAppStore } from "../store/appStore";
import { uploadDataset, getJobStatus, getFullResult } from "../services/api";
import Spinner from "../components/ui/Spinner";

const STEPS = [
  { key: "parsing",           label: "Parsing" },
  { key: "profiling",         label: "Profiling" },
  { key: "anomaly_detection", label: "Anomalies" },
  { key: "planning",          label: "AI Plan" },
  { key: "healing",           label: "Healing" },
  { key: "scoring",           label: "Scoring" },
  { key: "reporting",         label: "Report" },
];

function fmt(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

export default function UploadPage() {
  const navigate = useNavigate();
  const { jobId, step, result, error, filename, fileRows, fileColumns, fileSizeBytes, setJobId, setUploadMeta, setStep, setError, setResult } = useAppStore();
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const isRunning = jobId !== null && !["idle", "complete", "error"].includes(step);

  // Poll job status
  useEffect(() => {
    if (!jobId || step === "complete" || step === "error" || step === "idle") return;

    clearInterval(pollRef.current!);
    pollRef.current = setInterval(async () => {
      try {
        const status = await getJobStatus(jobId);
        setStep(status.step as any);

        if (status.step === "complete") {
          clearInterval(pollRef.current!);
          try {
            const fullResult = await getFullResult(jobId);
            setResult(fullResult as any);
            toast.success("Pipeline complete!");
          } catch {
            setError("Failed to load results.");
            toast.error("Failed to load results.");
          }
        } else if (status.step === "error") {
          clearInterval(pollRef.current!);
          setError(status.message ?? "Pipeline error.");
          toast.error(status.message ?? "Pipeline error.");
        }
      } catch {
        clearInterval(pollRef.current!);
        setError("Connection lost.");
      }
    }, 1500);

    return () => clearInterval(pollRef.current!);
  }, [jobId, step]);

  const onDrop = useCallback(async (accepted: File[]) => {
    const file = accepted[0];
    if (!file) return;

    setStep("parsing");
    setError(null);

    try {
      const resp = await uploadDataset(file);
      setJobId(resp.job_id);
      setUploadMeta({
        datasetId: resp.dataset_id ?? resp.job_id,
        filename: file.name,
        rows: resp.row_count ?? 0,
        columns: resp.column_count ?? 0,
        sizeBytes: file.size,
      });
      toast("Processing started…", { icon: "🚀" });
    } catch (e: any) {
      setError(e?.message ?? "Upload failed.");
      setStep("error");
      toast.error("Upload failed.");
    }
  }, []);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: { "text/csv": [".csv"], "application/json": [".json"] },
    multiple: false,
    disabled: isRunning,
  });

  const currentStepIdx = STEPS.findIndex((s) => s.key === step);
  const pct = currentStepIdx < 0 ? 0 : Math.round(((currentStepIdx + 1) / STEPS.length) * 100);

  return (
    <div className="max-w-2xl mx-auto">
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-slate-900">Upload Dataset</h1>
        <p className="text-sm text-slate-500 mt-1">Upload a CSV or JSON file to start the self-healing pipeline.</p>
      </div>

      {/* Drop zone */}
      {!result && (
        <div
          {...getRootProps()}
          className={`relative border-2 border-dashed rounded-2xl p-12 text-center cursor-pointer transition-all duration-200
            ${isDragActive
              ? "border-brand-500 bg-brand-50 scale-[1.01]"
              : isRunning
                ? "border-slate-200 bg-slate-50 cursor-not-allowed"
                : "border-slate-300 bg-white hover:border-brand-400 hover:bg-slate-50"
            }`}
        >
          <input {...getInputProps()} />
          {isRunning ? (
            <div className="flex flex-col items-center gap-3">
              <Spinner size={36} className="text-brand-600" />
              <p className="text-slate-600 font-medium">Processing…</p>
            </div>
          ) : isDragActive ? (
            <div>
              <span className="text-4xl">📥</span>
              <p className="mt-3 text-brand-700 font-semibold">Drop to upload</p>
            </div>
          ) : (
            <div>
              <span className="text-5xl">📂</span>
              <p className="mt-4 text-slate-700 font-semibold text-base">Drag &amp; drop a CSV or JSON file</p>
              <p className="mt-1 text-slate-400 text-sm">or click to browse</p>
              <p className="mt-4 text-[11px] text-slate-400 bg-slate-100 inline-block px-3 py-1 rounded-full">
                .csv · .json  ·  No size limit
              </p>
            </div>
          )}
        </div>
      )}

      {/* Error */}
      {error && (
        <div className="mt-4 p-4 bg-red-50 border border-red-200 rounded-xl text-sm text-red-700">
          {error}
        </div>
      )}

      {/* Progress stepper */}
      {isRunning && (
        <div className="mt-8 bg-white rounded-xl shadow-card border border-slate-100 p-6">
          <div className="flex justify-between text-xs text-slate-500 mb-2">
            <span className="capitalize font-medium text-slate-700">{step.replace(/_/g, " ")}…</span>
            <span>{pct}%</span>
          </div>
          {/* Progress bar */}
          <div className="h-2 bg-slate-100 rounded-full overflow-hidden mb-5">
            <div
              className="h-full bg-brand-600 rounded-full transition-all duration-500"
              style={{ width: `${pct}%` }}
            />
          </div>
          {/* Step pills */}
          <div className="flex justify-between gap-1">
            {STEPS.map((s, i) => (
              <div key={s.key} className="flex flex-col items-center gap-1 flex-1">
                <div className={`w-5 h-5 rounded-full flex items-center justify-center text-[10px] font-bold
                  ${i < currentStepIdx
                    ? "bg-brand-600 text-white"
                    : i === currentStepIdx
                      ? "bg-brand-200 text-brand-700 ring-2 ring-brand-400 animate-pulse"
                      : "bg-slate-200 text-slate-400"
                  }`}
                >
                  {i < currentStepIdx ? "✓" : i + 1}
                </div>
                <span className={`text-[9px] text-center leading-tight ${i <= currentStepIdx ? "text-slate-700" : "text-slate-400"}`}>
                  {s.label}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Complete summary */}
      {result && (
        <div className="mt-6 bg-white rounded-xl shadow-card border border-slate-100 p-6">
          <div className="flex items-center gap-2 mb-4">
            <span className="text-emerald-500 text-xl">✓</span>
            <h2 className="font-semibold text-slate-800">Pipeline complete</h2>
          </div>

          <div className="grid grid-cols-2 gap-3 text-sm mb-6">
            {filename && (
              <div className="bg-slate-50 rounded-lg p-3">
                <span className="text-[10px] uppercase tracking-wider text-slate-500 block mb-0.5">File</span>
                <span className="font-medium text-slate-800 truncate block">{filename}</span>
              </div>
            )}
            {fileRows != null && (
              <div className="bg-slate-50 rounded-lg p-3">
                <span className="text-[10px] uppercase tracking-wider text-slate-500 block mb-0.5">Rows</span>
                <span className="font-medium text-slate-800">{fileRows.toLocaleString()}</span>
              </div>
            )}
            {fileColumns != null && (
              <div className="bg-slate-50 rounded-lg p-3">
                <span className="text-[10px] uppercase tracking-wider text-slate-500 block mb-0.5">Columns</span>
                <span className="font-medium text-slate-800">{fileColumns}</span>
              </div>
            )}
            {fileSizeBytes != null && (
              <div className="bg-slate-50 rounded-lg p-3">
                <span className="text-[10px] uppercase tracking-wider text-slate-500 block mb-0.5">Size</span>
                <span className="font-medium text-slate-800">{fmt(fileSizeBytes)}</span>
              </div>
            )}
          </div>

          <div className="grid grid-cols-2 gap-2">
            {[
              { to: "/preview",    icon: "📋", label: "Preview" },
              { to: "/profile",    icon: "📊", label: "Profiling" },
              { to: "/anomalies",  icon: "⚠️",  label: "Anomalies" },
              { to: "/ai-plan",    icon: "🤖", label: "AI Plan" },
              { to: "/quality",    icon: "🎯", label: "Quality Score" },
              { to: "/comparison", icon: "🔀", label: "Comparison" },
            ].map(({ to, icon, label }) => (
              <button
                key={to}
                onClick={() => navigate(to)}
                className="flex items-center gap-2 px-3 py-2.5 rounded-lg bg-slate-50 hover:bg-brand-50 hover:text-brand-700 text-slate-700 text-sm font-medium border border-slate-200 hover:border-brand-200 transition-colors"
              >
                <span>{icon}</span> {label}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
