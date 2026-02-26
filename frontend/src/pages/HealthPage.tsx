import { useEffect, useState } from "react";
import { getHealthMetrics } from "../services/api";
import type { HealthMetrics } from "../types";
import Spinner from "../components/ui/Spinner";
import StatCard from "../components/ui/StatCard";
import Badge from "../components/ui/Badge";
import Card from "../components/ui/Card";

export default function HealthPage() {
  const [metrics, setMetrics] = useState<HealthMetrics | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  async function load() {
    try {
      const data = await getHealthMetrics();
      setMetrics(data);
    } catch {
      setError("Failed to load health metrics.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
    const id = setInterval(load, 30_000);
    return () => clearInterval(id);
  }, []);

  if (loading) {    return (
      <div className="flex items-center justify-center py-32">
        <Spinner size={32} className="text-brand-600" />
      </div>
    );
  }

  if (error || !metrics) {
    return (
      <div className="p-6 bg-red-50 border border-red-200 rounded-xl text-red-700 text-sm">
        {error ?? "No health data available."}
      </div>
    );
  }

  const workerCount = metrics.worker_count ?? metrics.active_workers;

  // Normalize services to Record<string, ServiceStatus>
  const servicesRecord: Record<string, import("../types").ServiceStatus> = (() => {
    if (!metrics.services) return {};
    if (Array.isArray(metrics.services)) {
      const r: Record<string, import("../types").ServiceStatus> = {};
      metrics.services.forEach((s) => { r[s.name ?? "service"] = s; });
      return r;
    }
    return metrics.services as Record<string, import("../types").ServiceStatus>;
  })();

  const overallOk = metrics.status === "ok";

  return (
    <div className="max-w-4xl mx-auto space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Pipeline Health</h1>
          <p className="text-sm text-slate-500 mt-1">Service status and system metrics (auto-refreshes every 30s)</p>
        </div>
        <button
          onClick={() => { setLoading(true); load(); }}
          className="text-xs text-brand-600 hover:underline"
        >
          Refresh
        </button>
      </div>

      {/* Overall banner */}
      <div className={`flex items-center gap-3 px-5 py-4 rounded-xl border ${
        overallOk
          ? "bg-emerald-50 border-emerald-200 text-emerald-800"
          : "bg-red-50 border-red-200 text-red-800"
      }`}>
        <span className="text-2xl">{overallOk ? "✅" : "🔴"}</span>
        <div>
          <p className="font-semibold">{overallOk ? "All systems operational" : "Degraded — check services below"}</p>
          {metrics.version && <p className="text-xs opacity-70">Version {metrics.version}</p>}
        </div>
      </div>

      {/* System stats */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        {metrics.uptime_seconds != null && (
          <StatCard
            label="Uptime"
            icon="⏱️"
            value={`${Math.floor(metrics.uptime_seconds / 60)} min`}
          />
        )}
        {workerCount != null && (
          <StatCard label="Workers" icon="⚙️" value={workerCount} />
        )}
        {metrics.cpu_count != null && (
          <StatCard label="CPU Cores" icon="🖥️" value={metrics.cpu_count} />
        )}
        {metrics.version && (
          <StatCard label="Version" icon="🏷️" value={metrics.version} />
        )}
      </div>

      {/* Services */}
      {Object.keys(servicesRecord).length > 0 && (
        <Card title="Services" subtitle="Individual service status">
          <div className="divide-y divide-slate-100">
            {Object.entries(servicesRecord).map(([name, svc]) => (
              <div key={name} className="flex items-center justify-between py-3 first:pt-0 last:pb-0">
                <div className="flex items-center gap-3">
                  <span
                    className={`w-2.5 h-2.5 rounded-full flex-shrink-0 ${
                      svc.status === "ok"
                        ? "bg-emerald-400"
                        : svc.status === "degraded"
                          ? "bg-amber-400"
                          : "bg-red-400"
                    }`}
                  />
                  <span className="text-sm font-medium text-slate-800 capitalize">{name}</span>
                </div>
                <div className="flex items-center gap-3">
                  {svc.latency_ms != null && (
                    <span className="text-xs text-slate-500">{svc.latency_ms.toFixed(1)} ms</span>
                  )}
                  <Badge variant={svc.status as any}>{svc.status}</Badge>
                </div>
              </div>
            ))}
          </div>
        </Card>
      )}

      {/* Raw debug */}
      <details className="group">
        <summary className="cursor-pointer text-xs text-slate-400 hover:text-slate-600">Show raw metrics</summary>
        <pre className="mt-2 p-4 bg-slate-800 text-slate-200 text-[11px] rounded-xl overflow-auto max-h-72">
          {JSON.stringify(metrics, null, 2)}
        </pre>
      </details>
    </div>
  );
}
