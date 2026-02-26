/**
 * DuplicateMetricCard
 * Visual card showing duplicate row statistics with a radial progress ring
 * and contextual severity colouring.
 */
import { PieChart, Pie, Cell, ResponsiveContainer } from "recharts";

// ── severity helpers ──────────────────────────────────────────────────────────
function severity(pct: number): { label: string; color: string; bg: string; ring: string } {
  if (pct === 0)   return { label: "None",     color: "#10b981", bg: "#ecfdf5", ring: "#a7f3d0" };
  if (pct < 1)     return { label: "Minimal",  color: "#22c55e", bg: "#f0fdf4", ring: "#86efac" };
  if (pct < 5)     return { label: "Low",      color: "#f59e0b", bg: "#fffbeb", ring: "#fcd34d" };
  if (pct < 15)    return { label: "Moderate", color: "#f97316", bg: "#fff7ed", ring: "#fdba74" };
  return           { label: "High",      color: "#ef4444", bg: "#fef2f2", ring: "#fca5a5" };
}

// ── ring chart (mini donut) ───────────────────────────────────────────────────
function RingChart({ pct, color, ringColor }: { pct: number; color: string; ringColor: string }) {
  const filled = Math.max(0, Math.min(100, pct));
  const data = [
    { value: filled },
    { value: 100 - filled },
  ];
  return (
    <div className="relative w-20 h-20 flex-shrink-0">
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Pie
            data={data}
            cx="50%" cy="50%"
            innerRadius={26} outerRadius={36}
            startAngle={90} endAngle={-270}
            dataKey="value"
            strokeWidth={0}
            paddingAngle={filled > 0 && filled < 100 ? 2 : 0}
          >
            <Cell fill={color} />
            <Cell fill={ringColor} />
          </Pie>
        </PieChart>
      </ResponsiveContainer>
      {/* Center text */}
      <div className="absolute inset-0 flex flex-col items-center justify-center leading-none">
        <span className="text-sm font-bold" style={{ color }}>{pct.toFixed(1)}%</span>
      </div>
    </div>
  );
}

// ── main component ────────────────────────────────────────────────────────────
export interface DuplicateMetricCardProps {
  /** Number of duplicate rows detected */
  duplicateCount: number;
  /** Total rows in dataset */
  totalRows: number;
  /** Pre-computed percentage (overrides calculation if provided) */
  duplicatePct?: number;
  /** Extra Tailwind classes */
  className?: string;
}

export default function DuplicateMetricCard({
  duplicateCount,
  totalRows,
  duplicatePct,
  className = "",
}: DuplicateMetricCardProps) {
  const pct = duplicatePct ?? (totalRows > 0 ? (duplicateCount / totalRows) * 100 : 0);
  const sev = severity(pct);
  const uniqueRows = totalRows - duplicateCount;

  return (
    <div
      className={`bg-white rounded-xl border border-slate-100 shadow-card overflow-hidden ${className}`}
      style={{ borderTop: `3px solid ${sev.color}` }}
    >
      <div className="p-5">
        {/* Header */}
        <div className="flex items-start justify-between mb-4">
          <div>
            <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">
              Duplicate Rows
            </p>
            <p className="text-3xl font-bold text-slate-900 mt-0.5">
              {duplicateCount.toLocaleString()}
            </p>
          </div>
          <RingChart pct={pct} color={sev.color} ringColor={sev.ring} />
        </div>

        {/* Severity badge */}
        <div
          className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold mb-4"
          style={{ background: sev.bg, color: sev.color }}
        >
          <span
            className="w-1.5 h-1.5 rounded-full"
            style={{ background: sev.color }}
          />
          {sev.label} duplication
        </div>

        {/* Stats row */}
        <div className="grid grid-cols-3 gap-2 text-center">
          {[
            { label: "Total rows",  value: totalRows.toLocaleString() },
            { label: "Unique rows", value: uniqueRows.toLocaleString() },
            { label: "Dup rate",    value: `${pct.toFixed(2)}%` },
          ].map(({ label, value }) => (
            <div key={label} className="bg-slate-50 rounded-lg py-2 px-1">
              <p className="text-[10px] text-slate-500 uppercase tracking-wider leading-tight">
                {label}
              </p>
              <p className="text-sm font-semibold text-slate-800 mt-0.5">{value}</p>
            </div>
          ))}
        </div>

        {/* Progress bar */}
        {pct > 0 && (
          <div className="mt-4">
            <div className="h-1.5 bg-slate-100 rounded-full overflow-hidden">
              <div
                className="h-full rounded-full transition-all duration-500"
                style={{ width: `${Math.min(pct, 100)}%`, background: sev.color }}
              />
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
