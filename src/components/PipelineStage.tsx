import { LucideIcon } from 'lucide-react';

interface PipelineStageProps {
  icon: LucideIcon;
  title: string;
  status: 'pending' | 'active' | 'completed' | 'error';
  description?: string;
  logs?: Array<{ message: string; status: string }>;
}

export function PipelineStage({ icon: Icon, title, status, description, logs }: PipelineStageProps) {
  const getStatusStyles = () => {
    switch (status) {
      case 'active':
        return 'border-blue-400 bg-blue-500/10 shadow-lg shadow-blue-500/20 scale-105';
      case 'completed':
        return 'border-green-400 bg-green-500/10';
      case 'error':
        return 'border-red-400 bg-red-500/10';
      default:
        return 'border-gray-600 bg-gray-800/50';
    }
  };

  const getIconStyles = () => {
    switch (status) {
      case 'active':
        return 'text-blue-400 animate-pulse';
      case 'completed':
        return 'text-green-400';
      case 'error':
        return 'text-red-400';
      default:
        return 'text-gray-500';
    }
  };

  return (
    <div className={`relative border-2 rounded-xl p-6 transition-all duration-300 ${getStatusStyles()}`}>
      <div className="flex items-start gap-4">
        <div className={`p-3 rounded-lg bg-gray-900/50 ${getIconStyles()}`}>
          <Icon className="w-6 h-6" />
        </div>

        <div className="flex-1">
          <h3 className="text-lg font-semibold text-white mb-1">{title}</h3>
          {description && (
            <p className="text-sm text-gray-400 mb-3">{description}</p>
          )}

          {logs && logs.length > 0 && (
            <div className="space-y-1 mt-3">
              {logs.map((log, idx) => (
                <div
                  key={idx}
                  className={`text-xs px-3 py-1.5 rounded-md ${
                    log.status === 'success'
                      ? 'bg-green-500/10 text-green-400 border border-green-500/20'
                      : log.status === 'warning'
                      ? 'bg-yellow-500/10 text-yellow-400 border border-yellow-500/20'
                      : log.status === 'error'
                      ? 'bg-red-500/10 text-red-400 border border-red-500/20'
                      : 'bg-gray-700/50 text-gray-300 border border-gray-600/20'
                  }`}
                >
                  {log.message}
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {status === 'active' && (
        <div className="absolute -inset-0.5 bg-gradient-to-r from-blue-500 to-cyan-500 rounded-xl opacity-20 blur animate-pulse" />
      )}
    </div>
  );
}
