interface GaugeArcProps {
  score: number; // 0–100
  size?: number;
  strokeWidth?: number;
}

function gradeColor(score: number): string {
  if (score >= 90) return "#10b981"; // emerald
  if (score >= 75) return "#3b82f6"; // blue
  if (score >= 60) return "#f59e0b"; // amber
  if (score >= 45) return "#f97316"; // orange
  return "#ef4444"; // red
}

function gradeLabel(score: number): string {
  if (score >= 90) return "A";
  if (score >= 75) return "B";
  if (score >= 60) return "C";
  if (score >= 45) return "D";
  return "F";
}

function arcPoint(cx: number, cy: number, r: number, fraction: number): { x: number; y: number } {
  // fraction 0 = left end, 1 = right end (semicircle over top)
  const angle = Math.PI * (1 - fraction); // 0 frac → angle=π (left), 1 frac → angle=0 (right)
  return {
    x: cx + r * Math.cos(angle),
    y: cy - r * Math.sin(angle),
  };
}

export default function GaugeArc({ score, size = 200, strokeWidth = 14 }: GaugeArcProps) {
  const cx = size / 2;
  const cy = size / 2;
  const r = (size - strokeWidth * 2 - 8) / 2;

  const fraction = Math.max(0.005, Math.min(0.995, score / 100));
  const color = gradeColor(score);
  const grade = gradeLabel(score);

  // Background arc: full semicircle (left to right, top)
  const bgStart = arcPoint(cx, cy, r, 0);
  const bgEnd = arcPoint(cx, cy, r, 1);
  const bgPath = `M ${bgStart.x} ${bgStart.y} A ${r} ${r} 0 0 1 ${bgEnd.x} ${bgEnd.y}`;

  // Score arc
  const scStart = arcPoint(cx, cy, r, 0);
  const scEnd = arcPoint(cx, cy, r, fraction);
  const largeArc = fraction > 0.5 ? 1 : 0;
  const scPath = `M ${scStart.x} ${scStart.y} A ${r} ${r} 0 ${largeArc} 1 ${scEnd.x} ${scEnd.y}`;

  return (
    <svg
      viewBox={`0 0 ${size} ${size}`}
      width={size}
      height={size}
      className="select-none"
    >
      {/* Track */}
      <path
        d={bgPath}
        fill="none"
        stroke="#e2e8f0"
        strokeWidth={strokeWidth}
        strokeLinecap="round"
      />
      {/* Score arc */}
      <path
        d={scPath}
        fill="none"
        stroke={color}
        strokeWidth={strokeWidth}
        strokeLinecap="round"
        style={{ transition: "all 0.6s ease" }}
      />
      {/* Center text */}
      <text
        x={cx}
        y={cy + 4}
        textAnchor="middle"
        dominantBaseline="middle"
        className="font-bold"
        style={{ fontFamily: "Inter, sans-serif", fontWeight: 700, fontSize: size * 0.16, fill: "#0f172a" }}
      >
        {Math.round(score)}
      </text>
      <text
        x={cx}
        y={cy + size * 0.13}
        textAnchor="middle"
        style={{ fontFamily: "Inter, sans-serif", fontWeight: 600, fontSize: size * 0.1, fill: color }}
      >
        Grade {grade}
      </text>
    </svg>
  );
}
