import { Database, Search, Wrench, Sparkles, BookOpen, CheckCircle2 } from 'lucide-react';
import { PipelineStage } from './PipelineStage';

interface Log {
  stage: string;
  message: string;
  status: string;
}

interface PipelineFlowProps {
  currentStage: string;
  logs: Log[];
}

export function PipelineFlow({ currentStage, logs }: PipelineFlowProps) {
  const stages = [
    { id: 'ingestion', icon: Database, title: 'Ingestion', description: 'Raw data received' },
    { id: 'detection', icon: Search, title: 'Detection', description: 'Analyzing data quality' },
    { id: 'rules', icon: Wrench, title: 'Local Rules', description: 'Applying rulebook' },
    { id: 'ai-healing', icon: Sparkles, title: 'AI Healing', description: 'Gemini Pro analysis' },
    { id: 'rulebook', icon: BookOpen, title: 'Learning', description: 'Updating rulebook' },
    { id: 'output', icon: CheckCircle2, title: 'Output', description: 'Clean data ready' },
  ];

  const getStageStatus = (stageId: string) => {
    const stageIndex = stages.findIndex(s => s.id === stageId);
    const currentIndex = stages.findIndex(s => s.id === currentStage);

    if (currentIndex === -1) return 'pending';
    if (stageIndex < currentIndex) return 'completed';
    if (stageIndex === currentIndex) return 'active';
    return 'pending';
  };

  const getStageLogs = (stageId: string) => {
    return logs
      .filter(log => log.stage === stageId)
      .map(log => ({ message: log.message, status: log.status }));
  };

  return (
    <div className="space-y-6">
      <div className="text-center mb-8">
        <h2 className="text-2xl font-bold text-white mb-2">Self-Healing Pipeline</h2>
        <p className="text-gray-400">Watch your data flow through detection, healing, and learning stages</p>
      </div>

      <div className="grid gap-6 lg:grid-cols-3 md:grid-cols-2">
        {stages.map((stage) => (
          <PipelineStage
            key={stage.id}
            icon={stage.icon}
            title={stage.title}
            status={getStageStatus(stage.id)}
            description={stage.description}
            logs={getStageLogs(stage.id)}
          />
        ))}
      </div>

      <div className="relative mt-8">
        <div className="absolute top-1/2 left-0 right-0 h-0.5 bg-gradient-to-r from-blue-500/20 via-cyan-500/20 to-green-500/20" />
        <div className="relative flex justify-between items-center">
          {stages.map((stage, index) => (
            <div
              key={stage.id}
              className={`w-4 h-4 rounded-full border-2 transition-all duration-300 ${
                getStageStatus(stage.id) === 'completed'
                  ? 'bg-green-500 border-green-400 shadow-lg shadow-green-500/50'
                  : getStageStatus(stage.id) === 'active'
                  ? 'bg-blue-500 border-blue-400 shadow-lg shadow-blue-500/50 animate-pulse scale-125'
                  : 'bg-gray-800 border-gray-600'
              }`}
            />
          ))}
        </div>
      </div>
    </div>
  );
}
