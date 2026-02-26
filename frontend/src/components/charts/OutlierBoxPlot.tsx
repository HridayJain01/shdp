/**
 * OutlierBoxPlot
 * Renders a box-and-whisker plot for numeric columns using a Recharts
 * ComposedChart with a custom Bar shape.
 *
 * Layout per column:
 *   ───  max cap
 *    │   upper whisker
 *   ┌─┐  ─────── Q3
 *   │ │  IQR box
 *   │═│  ═══════ median (thick line)
 *   │ │  IQR box
 *   └─┘  ─────── Q1
 *    │   lower whisker
 *   ───  min cap
 */
import {
  ComposedChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  type TooltipProps,
} from "recharts";
import type { ColumnProfile } from "../../types";

// ── types ─────────────────────────────────────────────────────────────────────
interface BoxDatum {
  name: string;
  /** transparent spacer bar value (Q1) */
  spacer: number;
  /** IQR bar value (Q3 - Q1) */
  box: number;
  /** stored for tooltip / custom shape */
  _min: number;
  _q1: number;
  _median: number;
  _q3: number;
  _max: number;
}

// ── custom box shape ──────────────────────────────────────────────────────────
// Recharts passes: x, y (top of bar in SVG), width, height (px), payload (full row)
function BoxShape(props: {
  x?: number; y?: number; width?: number; height?: number; payload?: BoxDatum;
}) {
  const { x = 0, y = 0, width = 0, height = 0, payload } = props;
  if (!payload || height <= 0 || width <= 0) return null;

  const { _q1, _q3, _median, _min, _max } = payload;
  const iqr = _q3 - _q1;

  // Pixel scale: how many SVG px per data unit
  const scale = iqr > 0 ? height / iqr : 0;

  const cx = x + width / 2;
  const boxLeft = x + width * 0.15;
  const boxRight = x + width * 0.85;
  const boxWidth = boxRight - boxLeft;
  const capHalf = width * 0.2;

  // SVG y coords (larger y = lower on screen = smaller data value)
  const q3y = y;              // top of IQR bar
  const q1y = y + height;     // bottom of IQR bar
  const medianY = scale > 0 ? q1y - (_median - _q1) * scale : (q1y + q3y) / 2;
  const maxY = scale > 0 ? q3y - (_max - _q3) * scale : q3y - 8;
  const minY = scale > 0 ? q1y + (_q1 - _min) * scale : q1y + 8;

  return (
    <g>
      {/* ── upper whisker ── */}
      <line x1={cx} y1={q3y} x2={cx} y2={maxY} stroke="#94a3b8" strokeWidth={1.5} strokeDasharray="3 2" />
      <line x1={cx - capHalf} y1={maxY} x2={cx + capHalf} y2={maxY} stroke="#64748b" strokeWidth={1.5} />

      {/* ── IQR box ── */}
      <rect
        x={boxLeft} y={q3y}
        width={boxWidth} height={height}
        fill="#6366f1" fillOpacity={0.18}
        stroke="#6366f1" strokeWidth={1.5}
        rx={3}
      />

      {/* ── median line ── */}
      <line
        x1={boxLeft} y1={medianY}
        x2={boxRight} y2={medianY}
        stroke="#4f46e5" strokeWidth={2.5}
      />

      {/* ── lower whisker ── */}
      <line x1={cx} y1={q1y} x2={cx} y2={minY} stroke="#94a3b8" strokeWidth={1.5} strokeDasharray="3 2" />
      <line x1={cx - capHalf} y1={minY} x2={cx + capHalf} y2={minY} stroke="#64748b" strokeWidth={1.5} />
    </g>
  );
}

// ── tooltip ───────────────────────────────────────────────────────────────────
function BoxTooltip({ active, payload }: TooltipProps<number, string>) {
  if (!active || !payload?.length) return null;
  const d = payload.find((p) => p.dataKey === "box")?.payload as BoxDatum | undefined;
  if (!d) return null;
  const rows: Array<[string, number | undefined]> = [
    ["Max",    d._max],
    ["Q3",     d._q3],
    ["Median", d._median],
    ["Q1",     d._q1],
    ["Min",    d._min],
  ];
  return (
    <div className="bg-slate-800 text-slate-100 text-xs px-3 py-2.5 rounded-lg shadow-lg space-y-0.5">
      <p className="font-semibold mb-1">{d.name}</p>
      {rows.map(([label, val]) => (
        <div key={label} className="flex justify-between gap-4">
          <span className="text-slate-400">{label}</span>
          <span className="font-mono tabular-nums">{val != null ? val.toFixed(3) : "—"}</span>
        </div>
      ))}
      <div className="flex justify-between gap-4 border-t border-slate-700 pt-0.5 mt-0.5">
        <span className="text-slate-400">IQR</span>
        <span className="font-mono tabular-nums">{(d._q3 - d._q1).toFixed(3)}</span>
      </div>
    </div>
  );
}

// ── main component ────────────────────────────────────────────────────────────
export interface OutlierBoxPlotProps {
  /** Column profiles; only numeric columns with stats are rendered */
  columns: ColumnProfile[];
  /** Chart height in px (default 320) */
  height?: number;
  /** Max columns to show (default 12) */
  maxCols?: number;
  /** Accent colour for box fill */
  accentColor?: string;
}

export default function OutlierBoxPlot({
  columns,
  height = 320,
  maxCols = 12,
}: OutlierBoxPlotProps) {
  const numericCols = columns
    .filter(
      (c) =>
        (c.dtype_category === "numeric" ||
          c.dtype_category === "integer" ||
          c.dtype_category === "float") &&
        c.numeric_stats?.q1 != null &&
        c.numeric_stats?.q3 != null
    )
    .slice(0, maxCols);

  if (numericCols.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-10 text-slate-400 text-sm">
        <span className="text-3xl mb-2">📈</span>
        <p>No numeric columns with quartile data available</p>
      </div>
    );
  }

  const rawData: BoxDatum[] = numericCols.map((c) => {
    const s = c.numeric_stats!;
    const q1 = s.q1 ?? 0;
    const q3 = s.q3 ?? 0;
    return {
      name: c.name,
      spacer: q1,           // transparent bar from 0 → Q1
      box: q3 - q1,         // visible IQR bar from Q1 → Q3
      _min: s.min ?? q1,
      _q1: q1,
      _median: s.median ?? (q1 + q3) / 2,
      _q3: q3,
      _max: s.max ?? q3,
    };
  });

  // Compute a shared y-domain with 5% padding
  const allMins = rawData.map((d) => d._min);
  const allMaxs = rawData.map((d) => d._max);
  const domainMin = Math.min(...allMins);
  const domainMax = Math.max(...allMaxs);
  const padding = (domainMax - domainMin) * 0.08 || 1;

  return (
    <ResponsiveContainer width="100%" height={height}>
      <ComposedChart data={rawData} margin={{ top: 16, right: 16, left: 0, bottom: 8 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
        <XAxis
          dataKey="name"
          tick={{ fontSize: 11, fill: "#475569" }}
          axisLine={false}
          tickLine={false}
          interval={0}
          angle={numericCols.length > 6 ? -35 : 0}
          textAnchor={numericCols.length > 6 ? "end" : "middle"}
          height={numericCols.length > 6 ? 52 : 24}
        />
        <YAxis
          tick={{ fontSize: 10, fill: "#94a3b8" }}
          domain={[domainMin - padding, domainMax + padding]}
          axisLine={false}
          tickLine={false}
          tickFormatter={(v) => (Math.abs(v) >= 1000 ? `${(v / 1000).toFixed(1)}k` : v.toFixed(1))}
          width={48}
        />
        <Tooltip content={<BoxTooltip />} cursor={false} />

        {/* Spacer: transparent bar from 0 to Q1 */}
        <Bar dataKey="spacer" stackId="box" fill="transparent" />

        {/* IQR box: custom shape draws the full plot including whiskers */}
        <Bar
          dataKey="box"
          stackId="box"
          shape={<BoxShape />}
          maxBarSize={52}
        />
      </ComposedChart>
    </ResponsiveContainer>
  );
}
