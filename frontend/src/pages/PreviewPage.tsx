import { useAppStore } from "../store/appStore";
import EmptyState from "../components/ui/EmptyState";
import StatCard from "../components/ui/StatCard";
import Card from "../components/ui/Card";
import DuplicateMetricCard from "../components/charts/DuplicateMetricCard";

const DTYPE_COLORS: Record<string, string> = {
  numeric:   "bg-blue-100 text-blue-700",
  string:    "bg-green-100 text-green-700",
  datetime:  "bg-purple-100 text-purple-700",
  boolean:   "bg-orange-100 text-orange-700",
  unknown:   "bg-slate-100 text-slate-500",
};

export default function PreviewPage() {
  const result = useAppStore((s) => s.result);

  if (!result?.profile) {
    return (
      <EmptyState
        icon="📋"
        title="No dataset loaded"
        description="Upload a dataset first to see the preview."
      />
    );
  }

  const profile = result.profile;
  const cols = profile.columns ?? [];
  const nullTotal = cols.reduce((acc, c) => acc + (c.null_count ?? 0), 0);
  const totalCells = (profile.row_count ?? 0) * cols.length;
  const overallNullPct = totalCells > 0 ? ((nullTotal / totalCells) * 100).toFixed(1) : "—";

  return (
    <div className="max-w-5xl mx-auto space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-900">Dataset Preview</h1>
        <p className="text-sm text-slate-500 mt-1">Column-level metadata and dataset overview</p>
      </div>

      {/* Stats row */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <StatCard label="Rows"        icon="📏" value={(profile.row_count ?? 0).toLocaleString()} />
        <StatCard label="Columns"     icon="🗂️"  value={cols.length} />
        <StatCard label="Memory"      icon="💾" value={profile.memory_mb != null ? `${profile.memory_mb.toFixed(2)} MB` : "—"} />
        <StatCard label="Null Rate"   icon="◻️"  value={`${overallNullPct}%`} />
      </div>

      {/* Column table */}
      <Card title="Columns" subtitle={`${cols.length} columns in dataset`}>
        <div className="overflow-x-auto -mx-6 px-6">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-100">
                {["Column", "Type", "Category", "Semantic", "Null %", "Unique"].map((h) => (
                  <th key={h} className="text-left py-2.5 pr-4 text-xs font-semibold uppercase tracking-wider text-slate-500">
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-50">
              {cols.map((col) => {
                const nullPct = (profile.row_count ?? 0) > 0
                  ? ((col.null_count ?? 0) / (profile.row_count ?? 1) * 100).toFixed(1)
                  : "—";
                const catKey = col.dtype_category ?? "unknown";
                const colorCls = DTYPE_COLORS[catKey] ?? DTYPE_COLORS.unknown;
                return (
                  <tr key={col.name} className="hover:bg-slate-50 transition-colors">
                    <td className="py-2.5 pr-4 font-medium text-slate-800">{col.name}</td>
                    <td className="py-2.5 pr-4 text-slate-600 font-mono text-xs">{col.dtype ?? "—"}</td>
                    <td className="py-2.5 pr-4">
                      <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-medium ${colorCls}`}>
                        {catKey}
                      </span>
                    </td>
                    <td className="py-2.5 pr-4 text-slate-500 text-xs">{col.semantic_type ?? "—"}</td>
                    <td className="py-2.5 pr-4">
                      <div className="flex items-center gap-2">
                        <span className="text-slate-700">{nullPct}%</span>
                        <div className="flex-1 min-w-[40px] h-1.5 bg-slate-100 rounded-full overflow-hidden">
                          <div
                            className="h-full bg-amber-400 rounded-full"
                            style={{ width: `${nullPct}%` }}
                          />
                        </div>
                      </div>
                    </td>
                    <td className="py-2.5 text-slate-600">{col.unique_count?.toLocaleString() ?? "—"}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </Card>

      {/* Duplicate metric */}
      {profile.duplicate_row_count != null && (
        <DuplicateMetricCard
          duplicateCount={profile.duplicate_row_count}
          totalRows={profile.row_count ?? 0}
          duplicatePct={profile.duplicate_pct}
        />
      )}
    </div>
  );
}
