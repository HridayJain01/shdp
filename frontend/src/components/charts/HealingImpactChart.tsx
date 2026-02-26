/**
 * HealingImpactChart
 * Bar chart visualising the healing pipeline's impact:
 *  - If a TransformationLog is provided: corrections made per strategy
 *  - If HealingStep[] is provided: estimated impact per step / column
 *  - Supports a "before vs after" overlay mode using QualityDelta pillars
 */
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  Cell,
  LabelList,
  ReferenceLine,
  type TooltipProps,
} from "recharts";
import type { TransformationLog, HealingStep } from "../../types";

// ── colour helpers ────────────────────────────────────────────────────────────
const STRATEGY_COLORS = [
  "#6366f1", "#22c55e", "#f59e0b", "#a855f7",
  "#06b6d4", "#f97316", "#ec4899", "#3b82f6",
];
function stratColor(i: number) { return STRATEGY_COLORS[i % STRATEGY_COLORS.length]; }

// ── tooltip ───────────────────────────────────────────────────────────────────
function ImpactTooltip({ active, payload, label }: TooltipProps<number, string>) {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-slate-800 text-slate-100 text-xs px-3 py-2 rounded-lg shadow-lg space-y-0.5">
      <p className="font-semibold mb-1">{label}</p>
      {payload.map((p) => (
        <div key={p.name} className="flex justify-between gap-4">
          <span style={{ color: p.color }}>{p.name}</span>
          <span className="font-mono tabular-nums font-semibold">{p.value}</span>
        </div>
      ))}
    </div>
  );
}

// ── mode: TransformationLog (corrections per strategy) ────────────────────────
interface TransformationDatum {
  strategy: string;
  corrections: number;
  ai: number;
}

function transformationData(log: TransformationLog): TransformationDatum[] {
  if (!log.entries?.length) return [];
  const map: Record<string, TransformationDatum> = {};
  for (const e of log.entries) {
    const key = e.strategy_name ?? "unknown";
    if (!map[key]) map[key] = { strategy: key, corrections: 0, ai: 0 };
    map[key].corrections += e.corrections ?? 0;
    if (e.source === "ai") map[key].ai += e.corrections ?? 0;
  }
  return Object.values(map).sort((a, b) => b.corrections - a.corrections);
}

// ── mode: HealingStep[] (estimated impact per step) ───────────────────────────
interface StepImpactDatum {
  label: string;
  impact: number;
  priority: number;
}

function stepImpactData(steps: HealingStep[]): StepImpactDatum[] {
  return steps
    .filter((s) => (s.estimated_impact ?? 0) > 0)
    .map((s) => ({
      label: s.column_name ?? s.strategy_name ?? s.title ?? "Unnamed",
      impact: s.estimated_impact ?? 0,
      priority: s.priority ?? 99,
    }))
    .sort((a, b) => b.impact - a.impact)
    .slice(0, 15);
}

// ── main component ────────────────────────────────────────────────────────────
export interface HealingImpactChartProps {
  /** Provide either a log (actual results) or steps (planned estimates) */
  transformationLog?: TransformationLog;
  healingSteps?: HealingStep[];
  /** Chart height in px (default 280) */
  height?: number;
  /** Show AI vs rule-based breakdown (only when log is provided) */
  showAiSplit?: boolean;
}

export default function HealingImpactChart({
  transformationLog,
  healingSteps,
  height = 280,
  showAiSplit = true,
}: HealingImpactChartProps) {
  // ── derive chart data ──────────────────────────────────────────────────────
  if (transformationLog && (transformationLog.entries?.length ?? 0) > 0) {
    const data = transformationData(transformationLog);
    if (data.length === 0) return <Empty />;

    const totalCorrections = data.reduce((a, d) => a + d.corrections, 0);
    const aiCorrections = data.reduce((a, d) => a + d.ai, 0);

    return (
      <div className="space-y-3">
        {/* Totals row */}
        <div className="flex flex-wrap gap-3 text-xs">
          <div className="bg-brand-50 border border-brand-200 rounded-lg px-3 py-1.5">
            <span className="text-slate-500">Total corrections </span>
            <span className="font-bold text-brand-700">{totalCorrections.toLocaleString()}</span>
          </div>
          {aiCorrections > 0 && (
            <div className="bg-purple-50 border border-purple-200 rounded-lg px-3 py-1.5">
              <span className="text-slate-500">AI-assisted </span>
              <span className="font-bold text-purple-700">{aiCorrections.toLocaleString()}</span>
            </div>
          )}
          <div className="bg-slate-50 border border-slate-200 rounded-lg px-3 py-1.5">
            <span className="text-slate-500">Strategies applied </span>
            <span className="font-bold text-slate-700">{data.length}</span>
          </div>
        </div>

        <ResponsiveContainer width="100%" height={height}>
          <BarChart data={data} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
            <XAxis
              dataKey="strategy"
              tick={{ fontSize: 10, fill: "#475569" }}
              axisLine={false}
              tickLine={false}
              interval={0}
              angle={data.length > 5 ? -35 : 0}
              textAnchor={data.length > 5 ? "end" : "middle"}
              height={data.length > 5 ? 52 : 24}
            />
            <YAxis
              tick={{ fontSize: 10, fill: "#94a3b8" }}
              axisLine={false}
              tickLine={false}
              width={40}
              tickFormatter={(v: number) =>
                v >= 1000 ? `${(v / 1000).toFixed(1)}k` : String(v)
              }
            />
            <Tooltip content={<ImpactTooltip />} cursor={{ fill: "#f8fafc" }} />
            {showAiSplit && aiCorrections > 0 ? (
              <>
                <Legend iconSize={10} wrapperStyle={{ fontSize: 11 }} />
                <Bar dataKey="corrections" name="Rule-based" stackId="a" fill="#6366f1" radius={[0, 0, 0, 0]}>
                  {data.map((d) => (
                    <Cell key={d.strategy} fill="#6366f1" fillOpacity={0.85} />
                  ))}
                </Bar>
                <Bar dataKey="ai" name="AI-assisted" stackId="a" fill="#a855f7" radius={[4, 4, 0, 0]}>
                  {data.map((d) => (
                    <Cell key={d.strategy} fill="#a855f7" />
                  ))}
                </Bar>
              </>
            ) : (
              <Bar dataKey="corrections" name="Corrections" radius={[4, 4, 0, 0]} maxBarSize={48}>
                {data.map((d, i) => (
                  <Cell key={d.strategy} fill={stratColor(i)} />
                ))}
                <LabelList
                  dataKey="corrections"
                  position="top"
                  formatter={(v: number) => (v >= 1000 ? `${(v / 1000).toFixed(1)}k` : v)}
                  style={{ fontSize: 9, fill: "#64748b" }}
                />
              </Bar>
            )}
          </BarChart>
        </ResponsiveContainer>
      </div>
    );
  }

  // ── Plan mode: estimated impact per step ───────────────────────────────────
  if (healingSteps && healingSteps.length > 0) {
    const data = stepImpactData(healingSteps);
    if (data.length === 0) return <Empty label="No impact estimates in AI plan" />;

    const avgImpact = data.reduce((a, d) => a + d.impact, 0) / data.length;

    return (
      <ResponsiveContainer width="100%" height={height}>
        <BarChart data={data} margin={{ top: 4, right: 40, left: 0, bottom: 4 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
          <XAxis
            dataKey="label"
            tick={{ fontSize: 10, fill: "#475569" }}
            axisLine={false}
            tickLine={false}
            interval={0}
            angle={data.length > 5 ? -35 : 0}
            textAnchor={data.length > 5 ? "end" : "middle"}
            height={data.length > 5 ? 52 : 24}
          />
          <YAxis
            tick={{ fontSize: 10, fill: "#94a3b8" }}
            axisLine={false}
            tickLine={false}
            tickFormatter={(v: number) => `${v.toFixed(0)}%`}
            domain={[0, 100]}
            width={36}
          />
          <Tooltip content={<ImpactTooltip />} cursor={{ fill: "#f8fafc" }} />
          <ReferenceLine
            y={avgImpact}
            stroke="#94a3b8"
            strokeDasharray="4 3"
            label={{ value: "avg", position: "right", fontSize: 10, fill: "#94a3b8" }}
          />
          <Bar dataKey="impact" name="Est. Impact %" radius={[4, 4, 0, 0]} maxBarSize={48}>
            {data.map((d, i) => (
              <Cell key={d.label} fill={stratColor(i)} />
            ))}
            <LabelList
              dataKey="impact"
              position="top"
              formatter={(v: number) => `${v.toFixed(0)}%`}
              style={{ fontSize: 9, fill: "#64748b" }}
            />
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    );
  }

  return <Empty />;
}

function Empty({ label = "No healing data available" }: { label?: string }) {
  return (
    <div className="flex flex-col items-center justify-center py-10 text-slate-400 text-sm">
      <span className="text-3xl mb-2">🔧</span>
      <p>{label}</p>
    </div>
  );
}
