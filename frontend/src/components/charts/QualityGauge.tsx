/**
 * QualityGauge
 * A Recharts RadialBarChart-based circular gauge showing an overall
 * quality score (0–100) with grade colouring. Optionally renders a
 * dimensional breakdown as concentric arc rings.
 */
import {
  RadialBarChart,
  RadialBar,
  PolarAngleAxis,
  ResponsiveContainer,
  type TooltipProps,
  Tooltip,
} from "recharts";
import type { QualityDimension } from "../../types";

// ── colour / grade helpers ────────────────────────────────────────────────────
export function scoreColor(score: number): string {
  if (score >= 90) return "#10b981"; // emerald
  if (score >= 75) return "#3b82f6"; // blue
  if (score >= 60) return "#f59e0b"; // amber
  if (score >= 45) return "#f97316"; // orange
  return "#ef4444";                  // red
}

export function scoreGrade(score: number): string {
  if (score >= 90) return "A";
  if (score >= 75) return "B";
  if (score >= 60) return "C";
  if (score >= 45) return "D";
  return "F";
}

// ── tooltip ───────────────────────────────────────────────────────────────────
function GaugeTooltip({ active, payload }: TooltipProps<number, string>) {
  if (!active || !payload?.length) return null;
  const d = payload[0];
  return (
    <div className="bg-slate-800 text-slate-100 text-xs px-3 py-2 rounded-lg shadow-lg">
      <p className="font-semibold" style={{ color: d.fill }}>{d.name}</p>
      <p>Score: <span className="font-bold">{(d.value as number).toFixed(1)}</span> / 100</p>
    </div>
  );
}

// ── main component ────────────────────────────────────────────────────────────
export interface QualityGaugeProps {
  /** Overall quality score 0–100 */
  score: number;
  /** Chart container height in px (default 220) */
  size?: number;
  /** Label displayed below the score */
  label?: string;
  /** Optional dimensional breakdown for concentric rings */
  breakdown?: QualityDimension[];
  /** Show the legend when breakdown is provided */
  showLegend?: boolean;
}

const DIM_COLORS = ["#6366f1", "#22c55e", "#f59e0b", "#a855f7", "#06b6d4"];

export default function QualityGauge({
  score,
  size = 220,
  label,
  breakdown,
  showLegend = true,
}: QualityGaugeProps) {
  const mainColor = scoreColor(score);
  const grade = scoreGrade(score);

  // When breakdown is provided, render each dimension as a separate ring
  const hasBreakdown = breakdown && breakdown.length > 0;

  const radialData = hasBreakdown
    ? breakdown!.map((d, i) => ({
        name: (d.dimension ?? d.label ?? `Dim ${i + 1}`).replace(/_/g, " "),
        value: d.score,
        fill: DIM_COLORS[i % DIM_COLORS.length],
      }))
    : [
        {
          name: label ?? "Quality Score",
          value: score,
          fill: mainColor,
        },
      ];

  const innerR = hasBreakdown ? "30%" : "55%";
  const outerR = hasBreakdown ? "90%" : "75%";

  return (
    <div className="flex flex-col items-center">
      <div style={{ width: "100%", height: size }} className="relative">
        <ResponsiveContainer width="100%" height="100%">
          <RadialBarChart
            cx="50%"
            cy="50%"
            innerRadius={innerR}
            outerRadius={outerR}
            startAngle={90}
            endAngle={-270}
            data={radialData}
            barSize={hasBreakdown ? 10 : 18}
          >
            {/* Full-circle background track */}
            <PolarAngleAxis type="number" domain={[0, 100]} tick={false} />
            <RadialBar
              background={{ fill: "#f1f5f9" }}
              dataKey="value"
              cornerRadius={8}
            />
            <Tooltip content={<GaugeTooltip />} />
          </RadialBarChart>
        </ResponsiveContainer>

        {/* Centre overlay — score + grade */}
        <div className="absolute inset-0 flex flex-col items-center justify-center pointer-events-none">
          <span
            className="text-4xl font-extrabold leading-none tabular-nums"
            style={{ color: mainColor }}
          >
            {Math.round(score)}
          </span>
          <span
            className="mt-1 text-sm font-bold px-2 py-0.5 rounded-full"
            style={{ background: `${mainColor}18`, color: mainColor }}
          >
            Grade {grade}
          </span>
          {label && !hasBreakdown && (
            <span className="mt-1 text-[11px] text-slate-500">{label}</span>
          )}
        </div>
      </div>

      {/* Breakdown legend */}
      {hasBreakdown && showLegend && (
        <div className="mt-3 w-full grid grid-cols-1 gap-1 px-2">
          {breakdown!.map((d, i) => {
            const name = (d.dimension ?? d.label ?? `Dim ${i + 1}`).replace(/_/g, " ");
            const color = DIM_COLORS[i % DIM_COLORS.length];
            return (
              <div key={name} className="flex items-center justify-between text-xs">
                <div className="flex items-center gap-1.5">
                  <span className="w-2.5 h-2.5 rounded-full flex-shrink-0" style={{ background: color }} />
                  <span className="text-slate-600 capitalize">{name}</span>
                </div>
                <span className="font-semibold tabular-nums" style={{ color }}>
                  {d.score.toFixed(0)}
                </span>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
