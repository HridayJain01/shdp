/**
 * DTypeDistPieChart
 * Donut chart showing the breakdown of column data-type categories
 * (numeric, string, datetime, boolean, unknown).
 */
import {
  PieChart,
  Pie,
  Cell,
  Tooltip,
  Legend,
  ResponsiveContainer,
  type TooltipProps,
} from "recharts";
import type { ColumnProfile } from "../../types";

const CATEGORY_COLORS: Record<string, string> = {
  numeric:   "#6366f1", // indigo
  string:    "#22c55e", // green
  datetime:  "#a855f7", // purple
  boolean:   "#f97316", // orange
  unknown:   "#94a3b8", // slate
  object:    "#06b6d4", // cyan
  integer:   "#3b82f6", // blue
  float:     "#8b5cf6", // violet
  text:      "#10b981", // emerald
};

function categoryColor(cat: string): string {
  return CATEGORY_COLORS[cat.toLowerCase()] ?? "#94a3b8";
}

interface PieDatum {
  name: string;
  value: number;
}

function DTypeTooltip({ active, payload }: TooltipProps<number, string>) {
  if (!active || !payload?.length) return null;
  const d = payload[0];
  return (
    <div className="bg-slate-800 text-slate-100 text-xs px-3 py-2 rounded-lg shadow-lg">
      <p className="font-semibold" style={{ color: d.payload.fill }}>{d.name}</p>
      <p>{d.value} column{d.value !== 1 ? "s" : ""}</p>
      <p className="text-slate-400">{((d.value as number / payload[0].payload.total) * 100).toFixed(1)}%</p>
    </div>
  );
}

function renderCustomLabel({
  cx, cy, midAngle, innerRadius, outerRadius, percent,
}: {
  cx: number; cy: number; midAngle: number;
  innerRadius: number; outerRadius: number; percent: number;
}) {
  if (percent < 0.06) return null;
  const RADIAN = Math.PI / 180;
  const r = innerRadius + (outerRadius - innerRadius) * 0.55;
  const x = cx + r * Math.cos(-midAngle * RADIAN);
  const y = cy + r * Math.sin(-midAngle * RADIAN);
  return (
    <text x={x} y={y} fill="white" textAnchor="middle" dominantBaseline="central"
      style={{ fontSize: 11, fontWeight: 600 }}>
      {`${(percent * 100).toFixed(0)}%`}
    </text>
  );
}

export interface DTypeDistPieChartProps {
  /** Column profiles from DatasetProfile */
  columns: ColumnProfile[];
  /** Outer radius of the donut (default 80) */
  outerRadius?: number;
  /** Inner radius of the donut — 0 for a full pie (default 52) */
  innerRadius?: number;
  /** Chart height in px (default 260) */
  height?: number;
}

export default function DTypeDistPieChart({
  columns,
  outerRadius = 90,
  innerRadius = 56,
  height = 260,
}: DTypeDistPieChartProps) {
  // Group by dtype_category (fallback to dtype, then "unknown")
  const counts: Record<string, number> = {};
  for (const col of columns) {
    const cat = col.dtype_category ?? col.dtype ?? "unknown";
    counts[cat] = (counts[cat] ?? 0) + 1;
  }

  const total = columns.length;
  const data: PieDatum[] = Object.entries(counts)
    .sort((a, b) => b[1] - a[1])
    .map(([name, value]) => ({ name, value }));

  if (data.length === 0) {
    return (
      <div className="flex items-center justify-center py-10 text-slate-400 text-sm">
        No column data available
      </div>
    );
  }

  return (
    <ResponsiveContainer width="100%" height={height}>
      <PieChart>
        <Pie
          data={data.map((d) => ({ ...d, total }))}
          cx="50%"
          cy="50%"
          innerRadius={innerRadius}
          outerRadius={outerRadius}
          dataKey="value"
          paddingAngle={2}
          labelLine={false}
          label={renderCustomLabel}
        >
          {data.map((d) => (
            <Cell key={d.name} fill={categoryColor(d.name)} stroke="transparent" />
          ))}
        </Pie>
        <Tooltip content={<DTypeTooltip />} />
        <Legend
          iconType="circle"
          iconSize={8}
          formatter={(value: string) => (
            <span style={{ fontSize: 11, color: "#475569" }}>
              {value} <span style={{ color: "#94a3b8" }}>({counts[value]})</span>
            </span>
          )}
        />
      </PieChart>
    </ResponsiveContainer>
  );
}
