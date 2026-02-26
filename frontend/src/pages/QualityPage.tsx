import { useAppStore } from "../store/appStore";
import EmptyState from "../components/ui/EmptyState";
import Card from "../components/ui/Card";
import QualityGauge from "../components/charts/QualityGauge";

function ScoreBar({ score, max = 100 }: { score: number; max?: number }) {
  const pct = (score / max) * 100;
  const color =
    pct >= 90 ? "bg-emerald-500" :
    pct >= 75 ? "bg-blue-500" :
    pct >= 60 ? "bg-amber-400" :
    pct >= 45 ? "bg-orange-400" :
    "bg-red-500";

  return (
    <div className="w-full h-2 bg-slate-100 rounded-full overflow-hidden">
      <div className={`h-full ${color} rounded-full transition-all duration-500`} style={{ width: `${pct}%` }} />
    </div>
  );
}

export default function QualityPage() {
  const result = useAppStore((s) => s.result);

  if (!result?.quality_score) {
    return (
      <EmptyState
        icon="🎯"
        title="No quality data"
        description="Upload and process a dataset to view quality scores."
      />
    );
  }

  const qs = result.quality_score;
  const breakdown = qs.breakdown ?? [];
  const delta = qs.delta;
  const suggestions = qs.improvement_suggestions ?? [];
  const deltaVal = delta?.delta ?? null;

  return (
    <div className="max-w-5xl mx-auto space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-900">Quality Score</h1>
        <p className="text-sm text-slate-500 mt-1">Composite quality assessment across 5 dimensions</p>
      </div>

      {/* Gauges */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-6">
        <Card title="Before Healing">
          <QualityGauge
            score={delta?.before ?? qs.overall_score ?? 0}
            label="Before"
            size={220}
          />
        </Card>
        <Card
          title="After Healing"
          action={
            deltaVal != null ? (
              <span className={`text-sm font-bold ${deltaVal >= 0 ? "text-emerald-600" : "text-red-500"}`}>
                {deltaVal >= 0 ? "+" : ""}{deltaVal.toFixed(1)} pts
              </span>
            ) : undefined
          }
        >
          <QualityGauge
            score={delta?.after ?? qs.overall_score ?? 0}
            label="After"
            size={220}
            breakdown={breakdown.length > 0 ? breakdown : undefined}
          />
        </Card>
      </div>

      {/* Dimension breakdown */}
      {breakdown.length > 0 && (
        <Card title="Dimension Breakdown" subtitle="5-pillar quality score breakdown">
          <div className="overflow-x-auto -mx-6 px-6">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-100">
                  {["Dimension", "Score", "Weight", "Issues", "Affected Cols"].map((h) => (
                    <th key={h} className="text-left py-2.5 pr-4 text-xs font-semibold uppercase tracking-wider text-slate-500">
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-50">
                {breakdown.map((dim) => (
                  <tr key={dim.dimension} className="hover:bg-slate-50 transition-colors">
                    <td className="py-3 pr-4 font-medium text-slate-800 capitalize">
                      {(dim.dimension ?? dim.label ?? "").replace(/_/g, " ")}
                    </td>
                    <td className="py-3 pr-4 w-40">
                      <div className="flex items-center gap-2">
                        <ScoreBar score={dim.score} />
                        <span className="text-xs text-slate-600 w-8 flex-shrink-0">{dim.score.toFixed(0)}</span>
                      </div>
                    </td>
                    <td className="py-3 pr-4 text-slate-600">{(dim.weight * 100).toFixed(0)}%</td>
                    <td className="py-3 pr-4 text-slate-600">{dim.issue_count ?? "—"}</td>
                    <td className="py-3 text-slate-500 text-xs">{dim.affected_columns?.join(", ") ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      )}

      {/* Suggestions */}
      {suggestions.length > 0 && (
        <Card title="Improvement Suggestions">
          <ul className="space-y-3">
            {suggestions.map((s, i) => (
              <li key={i} className="flex items-start gap-3">
                <span className="mt-0.5 flex-shrink-0 w-5 h-5 rounded-full bg-brand-100 text-brand-700 text-xs font-bold flex items-center justify-center">
                  {i + 1}
                </span>
                <div>
                  <p className="text-sm font-medium text-slate-800">{s.title ?? `Suggestion ${i + 1}`}</p>
                  {s.description && <p className="text-xs text-slate-500 mt-0.5">{s.description}</p>}
                </div>
              </li>
            ))}
          </ul>
        </Card>
      )}
    </div>
  );
}
