import { useAppStore } from "../store/appStore";
import EmptyState from "../components/ui/EmptyState";
import Badge from "../components/ui/Badge";
import Card from "../components/ui/Card";
import HealingImpactChart from "../components/charts/HealingImpactChart";

const PRIORITY_COLORS = ["bg-red-500", "bg-orange-400", "bg-amber-400", "bg-sky-400", "bg-slate-300"];

function priorityColor(p: number): string {
  return PRIORITY_COLORS[Math.min(p - 1, PRIORITY_COLORS.length - 1)] ?? "bg-slate-300";
}

export default function AIPlanPage() {
  const result = useAppStore((s) => s.result);

  if (!result?.healing_plan) {
    return (
      <EmptyState
        icon="🤖"
        title="No AI plan available"
        description="Upload and process a dataset to generate an AI healing plan."
      />
    );
  }

  const plan = result.healing_plan;
  const steps = plan.steps ?? [];
  const model = (plan as any).llm_model as string | undefined;
  const rationale = plan.overall_rationale ?? "";
  const confidence = (plan as any).confidence_score as number | undefined;

  return (
    <div className="max-w-4xl mx-auto space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-900">AI Healing Plan</h1>
        <p className="text-sm text-slate-500 mt-1">{steps.length} transformation steps planned</p>
      </div>

      {/* Meta card */}
      <div className="bg-gradient-to-r from-slate-900 to-slate-800 rounded-xl p-5 text-white">
        <div className="flex items-start justify-between gap-4">
          <div>
            <p className="text-slate-400 text-xs uppercase tracking-wider mb-1">AI Model</p>
            <p className="font-semibold">{model ?? "OpenRouter / Default"}</p>
            {rationale && (
              <p className="text-slate-300 text-sm mt-2 max-w-xl leading-relaxed">{rationale}</p>
            )}
          </div>
          {confidence != null && (
            <div className="text-right flex-shrink-0">
              <p className="text-slate-400 text-xs uppercase tracking-wider mb-1">Confidence</p>
              <p className="text-2xl font-bold">{(confidence * 100).toFixed(0)}%</p>
            </div>
          )}
        </div>
      </div>

      {/* Execution order pills */}
      {steps.length > 0 && (
        <div>
          <p className="text-xs text-slate-500 uppercase tracking-wider mb-2">Transformation Order</p>
          <div className="flex flex-wrap gap-2">
            {steps.map((s, i) => (
              <div key={i} className="flex items-center gap-1.5 bg-white border border-slate-200 rounded-full px-3 py-1 text-xs text-slate-700 shadow-sm">
                <span className={`w-4 h-4 rounded-full ${priorityColor(i + 1)} flex-shrink-0`} />
                <span className="font-medium">{i + 1}.</span>
                <span>{s.column_name ?? s.strategy_name ?? `Step ${i + 1}`}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Estimated impact chart */}
      {steps.length > 0 && (
        <Card title="Estimated Healing Impact" subtitle="Per-column improvement estimates from AI plan">
          <HealingImpactChart healingSteps={steps} height={240} />
        </Card>
      )}

      {/* Step cards */}
      <div className="space-y-4">
        {steps.map((step, i) => {
          const actions = step.actions ?? [];
          return (
            <Card key={i}>
              <div className="flex items-start gap-4">
                {/* Priority circle */}
                <div className={`w-9 h-9 rounded-full ${priorityColor(i + 1)} flex-shrink-0 flex items-center justify-center text-white font-bold text-sm`}>
                  {i + 1}
                </div>
                <div className="flex-1 min-w-0">
                  <div className="flex flex-wrap items-center gap-2 mb-1">
                    <h3 className="font-semibold text-slate-900">{step.column_name ?? `Step ${i + 1}`}</h3>
                    {step.strategy_name && (
                      <Badge variant="default">{step.strategy_name}</Badge>
                    )}
                    {(step as any).priority != null && (
                      <span className="text-xs text-slate-500">priority {(step as any).priority}</span>
                    )}
                  </div>
                  {step.rationale && (
                    <p className="text-sm text-slate-600 mb-3">{step.rationale}</p>
                  )}
                  {actions.length > 0 && (
                    <div className="space-y-2">
                      {actions.map((action, j) => (
                        <div key={j} className="flex items-start gap-3 bg-slate-50 rounded-lg px-3 py-2.5">
                          <span className="text-slate-400 text-xs mt-0.5">{j + 1}.</span>
                          <div className="flex-1 min-w-0">
                            <p className="text-xs font-semibold text-slate-700 font-mono">{action.action_type}</p>
                            {action.description && (
                              <p className="text-xs text-slate-500 mt-0.5">{action.description}</p>
                            )}
                          </div>
                          {action.estimated_impact_pct != null && (
                            <span className="text-xs text-emerald-600 font-medium flex-shrink-0">
                              +{action.estimated_impact_pct.toFixed(0)}%
                            </span>
                          )}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            </Card>
          );
        })}
      </div>
    </div>
  );
}
