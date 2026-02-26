type Variant = "critical" | "high" | "medium" | "low" | "ok" | "degraded" | "down" | "default";

const VARIANT_CLASSES: Record<Variant, string> = {
  critical: "bg-red-100 text-red-700 border border-red-200",
  high:     "bg-orange-100 text-orange-700 border border-orange-200",
  medium:   "bg-amber-100 text-amber-700 border border-amber-200",
  low:      "bg-green-100 text-green-700 border border-green-200",
  ok:       "bg-emerald-100 text-emerald-700 border border-emerald-200",
  degraded: "bg-amber-100 text-amber-700 border border-amber-200",
  down:     "bg-red-100 text-red-700 border border-red-200",
  default:  "bg-slate-100 text-slate-600 border border-slate-200",
};

interface BadgeProps {
  variant?: Variant;
  children: React.ReactNode;
  className?: string;
}

export default function Badge({ variant = "default", children, className = "" }: BadgeProps) {
  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-semibold ${VARIANT_CLASSES[variant]} ${className}`}
    >
      {children}
    </span>
  );
}
