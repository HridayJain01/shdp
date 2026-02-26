import { useState } from "react";
import { useAppStore } from "../store/appStore";
import EmptyState from "../components/ui/EmptyState";
import Card from "../components/ui/Card";
import StatCard from "../components/ui/StatCard";
import NullBarChart from "../components/charts/NullBarChart";
import DTypeDistPieChart from "../components/charts/DTypeDistPieChart";
import OutlierBoxPlot from "../components/charts/OutlierBoxPlot";
import type { ColumnProfile } from "../types";

function NullBar({ pct }: { pct: number }) {
  return (
    <div className="flex items-center gap-2 text-xs text-slate-500">
      <div className="w-20 h-1.5 bg-slate-100 rounded-full overflow-hidden flex-shrink-0">
        <div className="h-full bg-amber-400 rounded-full" style={{ width: `${Math.min(pct, 100)}%` }} />
      </div>
      <span>{pct.toFixed(1)}%</span>
    </div>
  );
}

function ColumnDetail({ col, rowCount }: { col: ColumnProfile; rowCount: number }) {
  const nullPct = rowCount > 0 ? (col.null_count ?? 0) / rowCount * 100 : 0;
  const numeric = col.numeric_stats;
  const strings = col.string_stats;
  const topVals = col.top_values ?? [];

  return (
    <div className="space-y-5">
      {/* Info grid */}
      <div className="grid grid-cols-2 gap-3 text-sm">
        <Info label="Data type" value={col.dtype ?? "—"} mono />
        <Info label="Category" value={col.dtype_category ?? "—"} />
        <Info label="Semantic" value={col.semantic_type ?? "—"} />
        <Info label="Null count" value={(col.null_count ?? 0).toLocaleString()} />
        <Info label="Null rate" value={`${nullPct.toFixed(1)}%`} />
        <Info label="Unique count" value={(col.unique_count ?? 0).toLocaleString()} />
      </div>

      {/* Numeric stats */}
      {numeric && (
        <Card title="Numeric Distribution">
          <div className="grid grid-cols-3 gap-3 text-sm">
            {[
              ["Min", numeric.min],
              ["Max", numeric.max],
              ["Mean", numeric.mean?.toFixed(3)],
              ["Median", numeric.median?.toFixed(3)],
              ["Stdev", numeric.std?.toFixed(3)],
              ["Zeros", numeric.zero_count],
            ].map(([k, v]) => (
              <div key={k as string} className="bg-slate-50 rounded-lg px-3 py-2">
                <p className="text-[10px] uppercase text-slate-500 tracking-wider">{k}</p>
                <p className="font-semibold text-slate-800">{v ?? "—"}</p>
              </div>
            ))}
          </div>
        </Card>
      )}

      {/* String stats */}
      {strings && (
        <Card title="String Stats">
          <div className="grid grid-cols-3 gap-3 text-sm">
            {[
              ["Min len", strings.min_length],
              ["Max len", strings.max_length],
              ["Avg len", strings.avg_length?.toFixed(1)],
              ["Empty", strings.empty_count],
              ["Spaces", strings.whitespace_count],
            ].map(([k, v]) => (
              <div key={k as string} className="bg-slate-50 rounded-lg px-3 py-2">
                <p className="text-[10px] uppercase text-slate-500 tracking-wider">{k}</p>
                <p className="font-semibold text-slate-800">{v ?? "—"}</p>
              </div>
            ))}
          </div>
        </Card>
      )}

      {/* Top values */}
      {topVals.length > 0 && (
        <Card title="Top Values">
          <div className="space-y-2">
            {topVals.slice(0, 10).map((tv) => {
              const barPct = tv.frequency * 100;
              return (
                <div key={String(tv.value)} className="flex items-center gap-2 text-xs">
                  <span className="text-slate-700 w-28 truncate flex-shrink-0 font-mono">{String(tv.value ?? "null")}</span>
                  <div className="flex-1 h-1.5 bg-slate-100 rounded-full overflow-hidden">
                    <div className="h-full bg-brand-500 rounded-full" style={{ width: `${barPct}%` }} />
                  </div>
                  <span className="text-slate-500 w-10 text-right">{(tv.frequency * 100).toFixed(1)}%</span>
                </div>
              );
            })}
          </div>
        </Card>
      )}
    </div>
  );
}

function Info({ label, value, mono }: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="bg-slate-50 rounded-lg px-3 py-2">
      <p className="text-[10px] uppercase text-slate-500 tracking-wider mb-0.5">{label}</p>
      <p className={`text-sm font-medium text-slate-800 ${mono ? "font-mono" : ""}`}>{value}</p>
    </div>
  );
}

export default function ProfilingPage() {
  const result = useAppStore((s) => s.result);
  const [selected, setSelected] = useState<string | null>(null);
  const [chartTab, setChartTab] = useState<"nulls" | "types" | "outliers">("nulls");

  if (!result?.profile) {
    return (
      <EmptyState
        icon="📊"
        title="No profiling data"
        description="Upload and process a dataset to view profiling results."
      />
    );
  }

  const profile = result.profile;
  const cols = profile.columns ?? [];
  const rowCount = profile.row_count ?? 0;
  const selectedCol = cols.find((c) => c.name === selected) ?? cols[0];

  const totalNulls = cols.reduce((a, c) => a + (c.null_count ?? 0), 0);
  const overallNullPct = rowCount > 0 && cols.length > 0 ? (totalNulls / (rowCount * cols.length) * 100).toFixed(1) : "—";

  return (
    <div className="max-w-6xl mx-auto space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-900">Profiling Analytics</h1>
        <p className="text-sm text-slate-500 mt-1">Per-column statistics and distribution analysis</p>
      </div>

      {/* Top stats */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <StatCard label="Rows"          icon="📏" value={rowCount.toLocaleString()} />
        <StatCard label="Columns"       icon="🗂️"  value={cols.length} />
        <StatCard label="Memory"        icon="💾" value={profile.memory_mb != null ? `${profile.memory_mb.toFixed(2)} MB` : "—"} />
        <StatCard label="Overall Null%" icon="◻️"  value={`${overallNullPct}%`} />
      </div>

      {/* Chart tabs */}
      <Card>
        <div className="flex gap-1 mb-5 border-b border-slate-100 -mx-6 px-6">
          {(["nulls", "types", "outliers"] as const).map((tab) => {
            const labels = { nulls: "Null Rates", types: "Data Types", outliers: "Outlier View" };
            return (
              <button
                key={tab}
                onClick={() => setChartTab(tab)}
                className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors -mb-px ${
                  chartTab === tab
                    ? "border-brand-600 text-brand-700"
                    : "border-transparent text-slate-500 hover:text-slate-700"
                }`}
              >
                {labels[tab]}
              </button>
            );
          })}
        </div>
        {chartTab === "nulls" && (
          <NullBarChart columns={cols} rowCount={rowCount} height={Math.max(220, cols.filter(c => (c.null_count ?? 0) > 0).length * 26 + 40)} />
        )}
        {chartTab === "types" && (
          <DTypeDistPieChart columns={cols} height={280} />
        )}
        {chartTab === "outliers" && (
          <OutlierBoxPlot columns={cols} height={300} />
        )}
      </Card>

      <div className="flex gap-5">
        {/* Column list */}
        <div className="w-56 flex-shrink-0">
          <div className="bg-white rounded-xl shadow-card border border-slate-100 overflow-auto max-h-[600px]">
            <p className="px-4 py-3 text-xs font-semibold uppercase tracking-wider text-slate-500 border-b border-slate-100">
              Columns
            </p>
            {cols.map((col) => {
              const nullPct = rowCount > 0 ? (col.null_count ?? 0) / rowCount * 100 : 0;
              const isActive = (selected ?? cols[0]?.name) === col.name;
              return (
                <button
                  key={col.name}
                  onClick={() => setSelected(col.name)}
                  className={`w-full text-left px-4 py-2.5 hover:bg-slate-50 transition-colors ${
                    isActive ? "bg-brand-50 border-r-2 border-brand-600" : ""
                  }`}
                >
                  <p className={`text-xs font-medium truncate ${isActive ? "text-brand-700" : "text-slate-700"}`}>{col.name}</p>
                  <NullBar pct={nullPct} />
                </button>
              );
            })}
          </div>
        </div>

        {/* Detail panel */}
        <div className="flex-1 min-w-0">
          {selectedCol ? (
            <Card title={selectedCol.name} subtitle={`${selectedCol.dtype ?? ""} · ${selectedCol.dtype_category ?? ""}`}>
              <ColumnDetail col={selectedCol} rowCount={rowCount} />
            </Card>
          ) : (
            <div className="flex items-center justify-center h-48 text-slate-400 text-sm">Select a column</div>
          )}
        </div>
      </div>
    </div>
  );
}
