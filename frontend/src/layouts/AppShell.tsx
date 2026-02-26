import { useState } from "react";
import { NavLink, Outlet, useNavigate } from "react-router-dom";
import { useAppStore } from "../store/appStore";

const NAV_ITEMS = [
  { path: "/upload",     icon: "⬆️",  label: "Upload" },
  { path: "/preview",    icon: "📋",  label: "Preview" },
  { path: "/profile",    icon: "📊",  label: "Profiling" },
  { path: "/anomalies",  icon: "⚠️",  label: "Anomalies" },
  { path: "/ai-plan",    icon: "🤖",  label: "AI Plan" },
  { path: "/quality",    icon: "🎯",  label: "Quality Score" },
  { path: "/comparison", icon: "🔀",  label: "Comparison" },
  { path: "/health",     icon: "🩺",  label: "Health" },
];

const STEP_LABELS: Record<string, string> = {
  idle:              "Idle",
  parsing:           "Parsing…",
  profiling:         "Profiling…",
  anomaly_detection: "Detecting anomalies…",
  planning:          "AI planning…",
  healing:           "Healing…",
  scoring:           "Scoring…",
  reporting:         "Reporting…",
  complete:          "Complete ✓",
  error:             "Error",
};

export default function AppShell() {
  const { step, filename, reset } = useAppStore();
  const navigate = useNavigate();
  const [open, setOpen] = useState(false);

  const isRunning = !["idle", "complete", "error"].includes(step);

  function handleReset() {
    reset();
    navigate("/upload");
    setOpen(false);
  }

  const sidebar = (
    <aside className="flex flex-col h-full bg-slate-900 text-slate-300 select-none">
      {/* Logo */}
      <div className="flex items-center gap-2 px-5 py-5 border-b border-slate-800">
        <span className="text-xl">✨</span>
        <div>
          <p className="text-white font-semibold text-sm leading-tight">SHDP</p>
          <p className="text-slate-500 text-[10px]">Self-Healing Data Platform</p>
        </div>
      </div>

      {/* Dataset badge */}
      {filename && (
        <div className="mx-3 mt-3 px-3 py-2 bg-slate-800 rounded-lg text-xs text-slate-400 truncate">
          <span className="block text-[10px] uppercase tracking-wider text-slate-500 mb-0.5">Dataset</span>
          <span className="text-slate-200 font-medium truncate block">{filename}</span>
        </div>
      )}

      {/* Nav */}
      <nav className="flex-1 overflow-y-auto py-3 space-y-0.5 px-2">
        {NAV_ITEMS.map(({ path, icon, label }) => (
          <NavLink
            key={path}
            to={path}
            onClick={() => setOpen(false)}
            className={({ isActive }) =>
              `flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors ${
                isActive
                  ? "bg-brand-600 text-white"
                  : "text-slate-400 hover:bg-slate-800 hover:text-slate-100"
              }`
            }
          >
            <span className="text-base w-5 text-center">{icon}</span>
            {label}
          </NavLink>
        ))}
      </nav>

      {/* Pipeline step indicator */}
      <div className="mx-3 mb-3 px-3 py-2 bg-slate-800 rounded-lg text-xs">
        <span className="block text-[10px] uppercase tracking-wider text-slate-500 mb-1">Pipeline</span>
        <div className="flex items-center gap-2">
          {isRunning && (
            <span className="inline-block w-2 h-2 rounded-full bg-brand-500 animate-pulse" />
          )}
          <span className={isRunning ? "text-brand-400" : step === "complete" ? "text-emerald-400" : step === "error" ? "text-red-400" : "text-slate-500"}>
            {STEP_LABELS[step] ?? step}
          </span>
        </div>
      </div>

      {/* Reset */}
      <div className="px-3 pb-4">
        <button
          onClick={handleReset}
          className="w-full text-center text-xs text-slate-500 hover:text-slate-300 py-2 rounded-lg hover:bg-slate-800 transition-colors"
        >
          ↩ New upload
        </button>
      </div>
    </aside>
  );

  return (
    <div className="flex h-screen overflow-hidden bg-surface font-sans">
      {/* Desktop sidebar */}
      <div className="hidden lg:flex lg:flex-shrink-0 lg:w-56 flex-col">
        {sidebar}
      </div>

      {/* Mobile sidebar overlay */}
      {open && (
        <div className="fixed inset-0 z-40 flex lg:hidden">
          <div className="fixed inset-0 bg-black/60" onClick={() => setOpen(false)} />
          <div className="relative z-50 flex flex-col w-56">
            {sidebar}
          </div>
        </div>
      )}

      {/* Main content */}
      <div className="flex flex-col flex-1 overflow-hidden">
        {/* Mobile top bar */}
        <div className="lg:hidden flex items-center gap-3 px-4 py-3 bg-slate-900 border-b border-slate-800">
          <button
            onClick={() => setOpen(true)}
            className="text-slate-400 hover:text-white p-1"
            aria-label="Open menu"
          >
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
            </svg>
          </button>
          <span className="text-white text-sm font-semibold">✨ SHDP</span>
        </div>

        {/* Page content */}
        <main className="flex-1 overflow-y-auto p-6">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
