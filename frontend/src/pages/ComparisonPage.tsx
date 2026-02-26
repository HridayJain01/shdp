import { useAppStore } from "../store/appStore";
import EmptyState from "../components/ui/EmptyState";
import StatCard from "../components/ui/StatCard";
import Card from "../components/ui/Card";
import BeforeAfterDiffTable from "../components/charts/BeforeAfterDiffTable";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from "recharts";

export default function ComparisonPage() {
  const result = useAppStore((s) => s.result);

  if (!result?.comparison) {
    return (
      <EmptyState
        icon="🔀"
        title="No comparison data"
        description="Upload and process a dataset to see before/after comparison."
      />
    );
  }

  const cmp = result.comparison;
  const changedCells = cmp.changed_cells ?? [];
  const nullChartData = cmp.null_rate_chart ?? [];
  const pillarData = cmp.pillar_score_chart ?? [];

  return (
    <div className="max-w-5xl mx-auto space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-900">Before vs After Comparison</h1>
        <p className="text-sm text-slate-500 mt-1">Impact of the healing pipeline on your dataset</p>
      </div>

      {/* Summary stats */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <StatCard
          label="Rows Before"
          icon="📏"
          value={(cmp.rows_before ?? 0).toLocaleString()}
        />
        <StatCard
          label="Rows After"
          icon="📐"
          value={(cmp.rows_after ?? 0).toLocaleString()}
          trend={cmp.rows_after != null && cmp.rows_before != null
            ? cmp.rows_after > cmp.rows_before ? "up" : cmp.rows_after < cmp.rows_before ? "down" : "neutral"
            : undefined}
          trendValue={cmp.rows_before && cmp.rows_after
            ? `${Math.abs(cmp.rows_after - cmp.rows_before)} rows`
            : undefined}
        />
        <StatCard
          label="Cells Changed"
          icon="✏️"
          value={(cmp.cells_changed ?? changedCells.length).toLocaleString()}
        />
        <StatCard
          label="Row Delta"
          icon="Δ"
          value={cmp.row_delta != null
            ? `${cmp.row_delta >= 0 ? "+" : ""}${cmp.row_delta}`
            : "—"}
        />
      </div>

      {/* Null rate chart */}
      {nullChartData.length > 0 && (
        <Card title="Null Rate per Column" subtitle="Before (red) vs After (emerald)">
          <div className="h-56">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={nullChartData} margin={{ top: 4, right: 8, left: -10, bottom: 4 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
                <XAxis dataKey="column" tick={{ fontSize: 10 }} interval={0} angle={-30} textAnchor="end" height={48} />
                <YAxis tick={{ fontSize: 10 }} unit="%" />
                <Tooltip formatter={(v: number) => `${v.toFixed(1)}%`} />
                <Legend iconSize={10} wrapperStyle={{ fontSize: 11 }} />
                <Bar dataKey="before" name="Before" fill="#f87171" radius={[3, 3, 0, 0]} />
                <Bar dataKey="after"  name="After"  fill="#34d399" radius={[3, 3, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Card>
      )}

      {/* Pillar score chart */}
      {pillarData.length > 0 && (
        <Card title="Quality Pillar Scores" subtitle="Before vs After">
          <div className="h-56">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={pillarData} margin={{ top: 4, right: 8, left: -10, bottom: 4 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
                <XAxis dataKey="pillar" tick={{ fontSize: 10 }} />
                <YAxis domain={[0, 100]} tick={{ fontSize: 10 }} />
                <Tooltip />
                <Legend iconSize={10} wrapperStyle={{ fontSize: 11 }} />
                <Bar dataKey="before" name="Before" fill="#94a3b8" radius={[3, 3, 0, 0]} />
                <Bar dataKey="after"  name="After"  fill="#6366f1" radius={[3, 3, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Card>
      )}

      {/* Changed cells diff table */}
      <Card
        title="Changed Cells"
        subtitle={`${changedCells.length} cell${changedCells.length !== 1 ? "s" : ""} modified`}
      >
        <BeforeAfterDiffTable
          changedCells={changedCells}
          maxRows={500}
          tableHeight={360}
        />
      </Card>
    </div>
  );
}
