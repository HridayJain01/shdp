import { ReactNode } from "react";

interface CardProps {
  children: ReactNode;
  className?: string;
  title?: string;
  subtitle?: string;
  action?: ReactNode;
}

export default function Card({ children, className = "", title, subtitle, action }: CardProps) {
  return (
    <div className={`bg-white rounded-xl shadow-card border border-slate-100 ${className}`}>
      {(title || action) && (
        <div className="flex items-start justify-between px-6 pt-5 pb-0">
          <div>
            {title && <h3 className="text-sm font-semibold text-slate-800">{title}</h3>}
            {subtitle && <p className="text-xs text-slate-500 mt-0.5">{subtitle}</p>}
          </div>
          {action && <div className="flex-shrink-0 ml-4">{action}</div>}
        </div>
      )}
      <div className={title ? "p-6 pt-4" : "p-6"}>{children}</div>
    </div>
  );
}
