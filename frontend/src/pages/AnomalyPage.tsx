import { useMemo, useState } from "react";
import { useAppStore } from "../store/appStore";
import EmptyState from "../components/ui/EmptyState";
import Badge from "../components/ui/Badge";
import Card from "../components/ui/Card";
import type { Severity } from "../types";

const SEVERITY_ORDER: Severity[] = ["critical", "high", "medium", "low"];

const SEVERITY_VARIANT: Record<Severity, "critical" | "high" | "medium" | "low"> = {
  critical: "critical",
  high:     "high",
  medium:   "medium",
  low:      "low",
};

export default function AnomalyPage() {
  const result = useAppStore((s) => s.result);
  const [filter, setFilter] = useState<Severity | "all">("all");
  const [search, setSearch] = useState("");

  if (!result?.anomaly_report) {
    return (
      <EmptyState
        icon="⚠️"
        title="No anomaly data"
        description="Upload and process a dataset to view anomaly reports."
      />
    );
  }

  const report = result.anomaly_report;
  const anomalies = report.anomalies ?? [];

  // Counts per severity
  const counts = useMemo(() => {
    const c: Record<string, number> = { all: anomalies.length };
    for (const a of anomalies) {
      c[a.severity] = (c[a.severity] ?? 0) + 1;
    }
    return c;
  }, [anomalies]);

  const filtered = useMemo(() => {
    return anomalies.filter((a) => {
      const matchSev = filter === "all" || a.severity === filter;
      const matchSearch =
        search === "" ||
        a.column_name?.toLowerCase().includes(search.toLowerCase()) ||
        a.description?.toLowerCase().includes(search.toLowerCase()) ||
        a.anomaly_type?.toLowerCase().includes(search.toLowerCase());
      return matchSev && matchSearch;
    });
  }, [anomalies, filter, search]);

  return (
    <div className="max-w-5xl mx-auto space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-900">Anomaly Report</h1>
        <p className="text-sm text-slate-500 mt-1">
          {anomalies.length} anomalies detected across {report.columns_affected ?? "??"} columns
        </p>
      </div>

      {/* Severity summary */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        {SEVERITY_ORDER.map((sev) => (
          <button
            key={sev}
            onClick={() => setFilter(sev === filter ? "all" : sev)}
            className={`bg-white rounded-xl shadow-card border px-4 py-3 text-left transition-all ${
              filter === sev ? "border-brand-400 ring-1 ring-brand-400" : "border-slate-100"
            }`}
          >
            <Badge variant={SEVERITY_VARIANT[sev]}>{sev}</Badge>
            <p className="text-2xl font-bold mt-1 text-slate-900">{counts[sev] ?? 0}</p>
          </button>
        ))}
      </div>

      {/* Filter + search row */}
      <div className="flex flex-wrap items-center gap-3">
        <div className="flex gap-1 bg-white rounded-lg border border-slate-200 p-1">
          {(["all", ...SEVERITY_ORDER] as const).map((sv) => (
            <button
              key={sv}
              onClick={() => setFilter(sv)}
              className={`px-3 py-1 text-xs font-medium rounded capitalize transition-colors ${
                filter === sv
                  ? "bg-brand-600 text-white"
                  : "text-slate-600 hover:bg-slate-100"
              }`}
            >
              {sv} {sv !== "all" && counts[sv] != null ? `(${counts[sv]})` : ""}
            </button>
          ))}
        </div>
        <input
          type="text"
          placeholder="Search column, type, description…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="flex-1 min-w-[200px] text-sm border border-slate-200 rounded-lg px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-brand-400"
        />
      </div>

      {/* Table */}
      <Card>
        {filtered.length === 0 ? (
          <p className="text-center text-slate-400 py-8 text-sm">No anomalies match your filter.</p>
        ) : (
          <div className="overflow-x-auto -mx-6 px-6">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-100">
                  {["Severity", "Type", "Column", "Affected%", "Description"].map((h) => (
                    <th key={h} className="text-left py-2.5 pr-4 text-xs font-semibold uppercase tracking-wider text-slate-500">
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-50">
                {filtered.map((a, i) => {
                  const affectedPct = a.affected_rate != null ? (a.affected_rate * 100).toFixed(1) : null;
                  return (
                    <tr key={i} className="hover:bg-slate-50 transition-colors">
                      <td className="py-3 pr-4">
                        <Badge variant={SEVERITY_VARIANT[a.severity as Severity] ?? "default"}>
                          {a.severity}
                        </Badge>
                      </td>
                      <td className="py-3 pr-4 font-mono text-xs text-slate-600">{a.anomaly_type ?? "—"}</td>
                      <td className="py-3 pr-4 font-medium text-slate-800">{a.column_name ?? "—"}</td>
                      <td className="py-3 pr-4">
                        {affectedPct != null ? (
                          <div className="flex items-center gap-2">
                            <div className="w-16 h-1.5 bg-slate-100 rounded-full overflow-hidden">
                              <div
                                className="h-full bg-orange-400 rounded-full"
                                style={{ width: `${affectedPct}%` }}
                              />
                            </div>
                            <span className="text-xs text-slate-600">{affectedPct}%</span>
                          </div>
                        ) : "—"}
                      </td>
                      <td className="py-3 text-slate-600 max-w-xs truncate" title={a.description ?? ""}>
                        {a.description ?? "—"}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </Card>
    </div>
  );
}
