import { useNavigate } from "react-router-dom";

interface EmptyStateProps {
  icon?: string;
  title: string;
  description?: string;
  showUploadCTA?: boolean;
}

export default function EmptyState({
  icon = "📂",
  title,
  description,
  showUploadCTA = true,
}: EmptyStateProps) {
  const navigate = useNavigate();

  return (
    <div className="flex flex-col items-center justify-center py-24 text-center">
      <span className="text-5xl mb-4">{icon}</span>
      <h3 className="text-base font-semibold text-slate-700 mb-1">{title}</h3>
      {description && <p className="text-sm text-slate-500 max-w-xs mb-6">{description}</p>}
      {showUploadCTA && (
        <button
          onClick={() => navigate("/upload")}
          className="px-4 py-2 bg-brand-600 text-white text-sm font-medium rounded-lg hover:bg-brand-700 transition-colors"
        >
          Upload a dataset
        </button>
      )}
    </div>
  );
}
