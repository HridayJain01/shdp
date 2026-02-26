interface StatCardProps {
  label: string;
  value: string | number;
  sub?: string;
  icon?: string;
  trend?: "up" | "down" | "neutral";
  trendValue?: string;
  className?: string;
}

export default function StatCard({ label, value, sub, icon, trend, trendValue, className = "" }: StatCardProps) {
  const trendColor =
    trend === "up" ? "text-emerald-600" : trend === "down" ? "text-red-500" : "text-slate-500";
  const trendArrow = trend === "up" ? "↑" : trend === "down" ? "↓" : "→";

  return (
    <div className={`bg-white rounded-xl shadow-card border border-slate-100 p-5 ${className}`}>
      <div className="flex items-center justify-between mb-2">
        <span className="text-xs font-medium uppercase tracking-wider text-slate-500">{label}</span>
        {icon && <span className="text-lg">{icon}</span>}
      </div>
      <p className="text-2xl font-bold text-slate-900 leading-none">{value}</p>
      {(sub || trendValue) && (
        <p className={`text-xs mt-1.5 ${trendValue ? trendColor : "text-slate-500"}`}>
          {trendValue ? `${trendArrow} ${trendValue}` : sub}
        </p>
      )}
    </div>
  );
}
