/**
 * BeforeAfterDiffTable
 * Scrollable table showing cell-level before → after changes produced
 * by the healing pipeline. Supports column filtering and compact mode.
 */
import { useMemo, useState } from "react";
import type { ChangedCell } from "../../types";

// ── value renderer ─────────────────────────────────────────────────────────────
function Chip({
  value,
  variant,
}: {
  value: unknown;
  variant: "before" | "after" | "unchanged";
}) {
  const str = value == null ? "null" : String(value);
  if (variant === "before") {
    return (
      <span className="inline-block bg-red-50 text-red-700 border border-red-200 px-1.5 py-0.5 rounded font-mono text-[11px] max-w-[160px] truncate" title={str}>
        {str}
      </span>
    );
  }
  if (variant === "after") {
    return (
      <span className="inline-block bg-emerald-50 text-emerald-700 border border-emerald-200 px-1.5 py-0.5 rounded font-mono text-[11px] max-w-[160px] truncate" title={str}>
        {str}
      </span>
    );
  }
  return (
    <span className="inline-block text-slate-500 font-mono text-[11px]">{str}</span>
  );
}

// ── diff arrow ────────────────────────────────────────────────────────────────
function DiffArrow() {
  return <span className="text-slate-400 text-xs mx-1 select-none">→</span>;
}

// ── main component ────────────────────────────────────────────────────────────
export interface BeforeAfterDiffTableProps {
  changedCells: ChangedCell[];
  /** Maximum rows to display in one page (default 200) */
  maxRows?: number;
  /** Compact row height (default false) */
  compact?: boolean;
  /** Fixed table height (default 340px) */
  tableHeight?: number;
}

export default function BeforeAfterDiffTable({
  changedCells,
  maxRows = 200,
  compact = false,
  tableHeight = 340,
}: BeforeAfterDiffTableProps) {
  const [colFilter, setColFilter] = useState("");
  const [page, setPage] = useState(0);
  const PAGE_SIZE = 50;

  // Normalise field names (backend may use `col` or `column`, `before` or `before_value`…)
  const normalised = useMemo(
    () =>
      changedCells.slice(0, maxRows).map((c) => ({
        row: c.row_index ?? c.row ?? "—",
        column: c.column ?? c.col ?? "—",
        before: c.before_value !== undefined ? c.before_value : c.before,
        after: c.after_value !== undefined ? c.after_value : c.after,
      })),
    [changedCells, maxRows]
  );

  const filtered = useMemo(() => {
    if (!colFilter.trim()) return normalised;
    const q = colFilter.toLowerCase();
    return normalised.filter((r) => String(r.column).toLowerCase().includes(q));
  }, [normalised, colFilter]);

  const pageCount = Math.ceil(filtered.length / PAGE_SIZE);
  const visible = filtered.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  // Unique column names for filter badge hints
  const uniqueCols = useMemo(
    () => [...new Set(normalised.map((r) => String(r.column)))].sort(),
    [normalised]
  );

  if (changedCells.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-10 text-slate-400 text-sm">
        <span className="text-3xl mb-2">✅</span>
        <p>No cell modifications recorded</p>
      </div>
    );
  }

  const rowPy = compact ? "py-1.5" : "py-2.5";

  return (
    <div className="space-y-3">
      {/* Controls */}
      <div className="flex flex-wrap items-center gap-2">
        <input
          type="text"
          placeholder="Filter by column…"
          value={colFilter}
          onChange={(e) => { setColFilter(e.target.value); setPage(0); }}
          className="text-xs border border-slate-200 rounded-lg px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-brand-400 w-48"
        />
        <span className="text-xs text-slate-500">
          {filtered.length.toLocaleString()} change{filtered.length !== 1 ? "s" : ""}
          {colFilter && ` in "${colFilter}"`}
          {changedCells.length > maxRows && (
            <span className="ml-1 text-amber-600">(showing first {maxRows.toLocaleString()})</span>
          )}
        </span>
        {/* Column quick-filter chips (max 6) */}
        {uniqueCols.slice(0, 6).map((col) => (
          <button
            key={col}
            onClick={() => { setColFilter(col === colFilter ? "" : col); setPage(0); }}
            className={`text-[10px] px-2 py-0.5 rounded-full border transition-colors ${
              colFilter === col
                ? "bg-brand-600 text-white border-brand-600"
                : "bg-slate-50 text-slate-600 border-slate-200 hover:border-brand-300"
            }`}
          >
            {col}
          </button>
        ))}
      </div>

      {/* Table */}
      <div
        className="overflow-auto rounded-lg border border-slate-100"
        style={{ maxHeight: tableHeight }}
      >
        <table className="w-full text-xs min-w-[480px]">
          <thead className="sticky top-0 bg-slate-50 z-10">
            <tr className="border-b border-slate-200">
              <th className="text-left py-2 px-3 text-[10px] font-semibold uppercase tracking-wider text-slate-500 w-16">Row</th>
              <th className="text-left py-2 px-3 text-[10px] font-semibold uppercase tracking-wider text-slate-500 w-36">Column</th>
              <th className="text-left py-2 px-3 text-[10px] font-semibold uppercase tracking-wider text-slate-500">Before → After</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-50 bg-white">
            {visible.map((r, i) => (
              <tr key={i} className="hover:bg-slate-50 transition-colors">
                <td className={`px-3 ${rowPy} font-mono text-slate-500`}>{String(r.row)}</td>
                <td className={`px-3 ${rowPy} font-medium text-slate-700`}>{String(r.column)}</td>
                <td className={`px-3 ${rowPy}`}>
                  <div className="flex items-center gap-0.5">
                    <Chip value={r.before} variant="before" />
                    <DiffArrow />
                    <Chip value={r.after} variant="after" />
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {pageCount > 1 && (
        <div className="flex items-center gap-2 justify-end">
          <button
            disabled={page === 0}
            onClick={() => setPage((p) => p - 1)}
            className="px-2.5 py-1 text-xs rounded border border-slate-200 disabled:opacity-40 hover:bg-slate-50"
          >
            ← Prev
          </button>
          <span className="text-xs text-slate-500">
            Page {page + 1} / {pageCount}
          </span>
          <button
            disabled={page >= pageCount - 1}
            onClick={() => setPage((p) => p + 1)}
            className="px-2.5 py-1 text-xs rounded border border-slate-200 disabled:opacity-40 hover:bg-slate-50"
          >
            Next →
          </button>
        </div>
      )}
    </div>
  );
}
