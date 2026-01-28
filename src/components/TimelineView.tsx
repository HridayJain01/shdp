import { Clock } from 'lucide-react';

interface Log {
  id: number;
  timestamp: string;
  stage: string;
  message: string;
  status: string;
}

interface TimelineViewProps {
  logs: Log[];
}

export function TimelineView({ logs }: TimelineViewProps) {
  const getStatusColor = (status: string) => {
    switch (status) {
      case 'success':
        return 'bg-green-500';
      case 'warning':
        return 'bg-yellow-500';
      case 'error':
        return 'bg-red-500';
      default:
        return 'bg-blue-500';
    }
  };

  const getStageLabel = (stage: string) => {
    const labels: Record<string, string> = {
      'ingestion': 'Ingestion',
      'detection': 'Detection',
      'rules': 'Local Rules',
      'ai-healing': 'AI Healing',
      'rulebook': 'Rulebook',
      'output': 'Output',
    };
    return labels[stage] || stage;
  };

  return (
    <div className="bg-gray-800/50 border-2 border-gray-700 rounded-xl p-6 backdrop-blur-sm">
      <div className="flex items-center gap-3 mb-6">
        <div className="p-2 rounded-lg bg-gradient-to-br from-blue-500 to-cyan-500">
          <Clock className="w-5 h-5 text-white" />
        </div>
        <div>
          <h2 className="text-xl font-bold text-white">Processing Timeline</h2>
          <p className="text-sm text-gray-400">Complete audit trail of data transformations</p>
        </div>
      </div>

      <div className="relative">
        <div className="absolute left-5 top-0 bottom-0 w-0.5 bg-gradient-to-b from-blue-500/50 via-cyan-500/50 to-green-500/50" />

        <div className="space-y-4">
          {logs.map((log, index) => (
            <div key={log.id} className="relative pl-12 pb-4">
              <div className={`absolute left-3 w-4 h-4 rounded-full ${getStatusColor(log.status)} ring-4 ring-gray-800 shadow-lg`} />

              <div className="bg-gray-900/50 border border-gray-700 rounded-lg p-4 hover:border-gray-600 transition-all">
                <div className="flex items-start justify-between gap-4 mb-2">
                  <div className="flex-1">
                    <div className="flex items-center gap-2 mb-1">
                      <span className="px-2 py-0.5 rounded text-xs font-medium bg-blue-500/10 text-blue-400 border border-blue-500/20">
                        {getStageLabel(log.stage)}
                      </span>
                      <span className="text-xs text-gray-500">
                        {new Date(log.timestamp).toLocaleTimeString()}
                      </span>
                    </div>
                    <p className="text-sm text-white">{log.message}</p>
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
