/**
 * NullBarChart
 * Renders a horizontal bar chart of null percentages per column,
 * sorted descending. Only columns with null_count > 0 are shown.
 */
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
  LabelList,
  type TooltipProps,
} from "recharts";
import type { ColumnProfile } from "../../types";

// ── colour helper ─────────────────────────────────────────────────────────────
function nullColor(pct: number): string {
  if (pct >= 40) return "#ef4444"; // red-500
  if (pct >= 20) return "#f97316"; // orange-500
  if (pct >= 10) return "#f59e0b"; // amber-500
  return "#fbbf24";                // amber-400
}

// ── custom tooltip ────────────────────────────────────────────────────────────
function NullTooltip({ active, payload }: TooltipProps<number, string>) {
  if (!active || !payload?.length) return null;
  const d = payload[0].payload as NullBarDatum;
  return (
    <div className="bg-slate-800 text-slate-100 text-xs px-3 py-2 rounded-lg shadow-lg">
      <p className="font-semibold mb-0.5">{d.column}</p>
      <p>Null rate: <span className="font-bold">{d.pct.toFixed(1)}%</span></p>
      <p className="text-slate-400">{d.count.toLocaleString()} / {d.total.toLocaleString()} rows</p>
    </div>
  );
}

// ── types ─────────────────────────────────────────────────────────────────────
interface NullBarDatum {
  column: string;
  pct: number;
  count: number;
  total: number;
}

export interface NullBarChartProps {
  /** Column profiles from DatasetProfile */
  columns: ColumnProfile[];
  /** Total row count of the dataset */
  rowCount: number;
  /** Chart height in px (default 280) */
  height?: number;
  /** Maximum number of bars to display (default 20, sorted by null%) */
  maxBars?: number;
  /** Show inline percentage labels on bars */
  showLabels?: boolean;
}

// ── component ─────────────────────────────────────────────────────────────────
export default function NullBarChart({
  columns,
  rowCount,
  height = 280,
  maxBars = 20,
  showLabels = true,
}: NullBarChartProps) {
  const data: NullBarDatum[] = columns
    .filter((c) => (c.null_count ?? 0) > 0)
    .map((c) => ({
      column: c.name,
      pct: rowCount > 0 ? ((c.null_count ?? 0) / rowCount) * 100 : (c.null_pct ?? 0),
      count: c.null_count ?? 0,
      total: rowCount,
    }))
    .sort((a, b) => b.pct - a.pct)
    .slice(0, maxBars);

  if (data.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-10 text-slate-400 text-sm">
        <span className="text-3xl mb-2">✅</span>
        <p>No nulls detected across all columns</p>
      </div>
    );
  }

  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart
        data={data}
        layout="vertical"
        margin={{ top: 4, right: showLabels ? 40 : 12, left: 0, bottom: 4 }}
      >
        <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" horizontal={false} />
        <XAxis
          type="number"
          domain={[0, 100]}
          tick={{ fontSize: 10, fill: "#94a3b8" }}
          tickFormatter={(v) => `${v}%`}
          axisLine={false}
          tickLine={false}
        />
        <YAxis
          type="category"
          dataKey="column"
          width={110}
          tick={{ fontSize: 11, fill: "#475569" }}
          axisLine={false}
          tickLine={false}
        />
        <Tooltip content={<NullTooltip />} cursor={{ fill: "#f8fafc" }} />
        <Bar dataKey="pct" radius={[0, 4, 4, 0]} maxBarSize={20}>
          {data.map((d) => (
            <Cell key={d.column} fill={nullColor(d.pct)} />
          ))}
          {showLabels && (
            <LabelList
              dataKey="pct"
              position="right"
              formatter={(v: number) => `${v.toFixed(1)}%`}
              style={{ fontSize: 10, fill: "#64748b" }}
            />
          )}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
}
