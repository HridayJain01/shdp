import { useState } from 'react';
import { Activity, RefreshCw } from 'lucide-react';
import { DataForm, FormData } from './components/DataForm';
import { PipelineFlow } from './components/PipelineFlow';
import { ComparisonView } from './components/ComparisonView';
import { RulebookPanel } from './components/RulebookPanel';
import { TimelineView } from './components/TimelineView';

interface ProcessingResult {
  original: Record<string, unknown>;
  cleaned: Record<string, unknown>;
  dirtyFields: string[];
  appliedRules: Array<{ id: number; name: string; source: string }>;
  isClean: boolean;
  logs: Array<{ id: number; timestamp: string; stage: string; message: string; status: string }>;
  rulebook: Array<{ id: number; name: string; description: string; source: string; active: boolean; createdAt?: string }>;
}

function App() {
  const [isProcessing, setIsProcessing] = useState(false);
  const [currentStage, setCurrentStage] = useState('');
  const [result, setResult] = useState<ProcessingResult | null>(null);

  const handleSubmit = async (formData: FormData) => {
    setIsProcessing(true);
    setResult(null);
    setCurrentStage('');

    const stages = ['ingestion', 'detection', 'rules', 'ai-healing', 'rulebook', 'output'];

    for (let i = 0; i < stages.length; i++) {
      setCurrentStage(stages[i]);
      await new Promise(resolve => setTimeout(resolve, 600));
    }

    try {
      const response = await fetch('http://localhost:3001/api/ingest', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(formData),
      });

      const data = await response.json();

      if (data.success) {
        setResult({
          original: data.original,
          cleaned: data.cleaned,
          dirtyFields: data.dirtyFields,
          appliedRules: data.appliedRules,
          isClean: data.isClean,
          logs: data.logs,
          rulebook: data.rulebook,
        });
      }
    } catch (error) {
      console.error('Error processing data:', error);
      alert('Failed to connect to backend. Make sure the server is running on port 3001.');
    } finally {
      setIsProcessing(false);
    }
  };

  const handleReset = () => {
    setResult(null);
    setCurrentStage('');
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-900 via-gray-800 to-gray-900">
      <div className="absolute inset-0 bg-[url('data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iNjAiIGhlaWdodD0iNjAiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyI+PGRlZnM+PHBhdHRlcm4gaWQ9ImdyaWQiIHdpZHRoPSI2MCIgaGVpZ2h0PSI2MCIgcGF0dGVyblVuaXRzPSJ1c2VyU3BhY2VPblVzZSI+PHBhdGggZD0iTSAxMCAwIEwgMCAwIDAgMTAiIGZpbGw9Im5vbmUiIHN0cm9rZT0icmdiYSgyNTUsMjU1LDI1NSwwLjAzKSIgc3Ryb2tlLXdpZHRoPSIxIi8+PC9wYXR0ZXJuPjwvZGVmcz48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBmaWxsPSJ1cmwoI2dyaWQpIi8+PC9zdmc+')] opacity-50" />

      <div className="relative">
        <header className="border-b border-gray-700/50 bg-gray-900/50 backdrop-blur-md">
          <div className="max-w-7xl mx-auto px-6 py-6">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-4">
                <div className="p-3 rounded-xl bg-gradient-to-br from-blue-500 to-cyan-500 shadow-lg shadow-blue-500/20">
                  <Activity className="w-8 h-8 text-white" />
                </div>
                <div>
                  <h1 className="text-3xl font-bold text-white">Self-Healing Data Pipeline</h1>
                  <p className="text-gray-400 mt-1">Intelligent data quality with AI-powered learning</p>
                </div>
              </div>
              {result && (
                <button
                  onClick={handleReset}
                  className="flex items-center gap-2 px-4 py-2 rounded-lg bg-gray-700 hover:bg-gray-600 text-white transition-colors"
                >
                  <RefreshCw className="w-4 h-4" />
                  Reset
                </button>
              )}
            </div>
          </div>
        </header>

        <main className="max-w-7xl mx-auto px-6 py-8">
          <div className="grid gap-8">
            {!result && (
              <div className="max-w-2xl mx-auto w-full">
                <DataForm onSubmit={handleSubmit} isProcessing={isProcessing} />
              </div>
            )}

            {(isProcessing || result) && (
              <div>
                <PipelineFlow
                  currentStage={currentStage}
                  logs={result?.logs || []}
                />
              </div>
            )}

            {result && (
              <>
                <div className="grid lg:grid-cols-2 gap-8">
                  <ComparisonView
                    original={result.original}
                    cleaned={result.cleaned}
                    dirtyFields={result.dirtyFields}
                  />
                  <RulebookPanel
                    rules={result.rulebook}
                    appliedRules={result.appliedRules}
                  />
                </div>

                <TimelineView logs={result.logs} />
              </>
            )}
          </div>
        </main>

        <footer className="border-t border-gray-700/50 mt-12 py-6">
          <div className="max-w-7xl mx-auto px-6 text-center text-gray-500 text-sm">
            <p>Enterprise Data Observability Platform - Demo Version</p>
            <p className="mt-1">Powered by Gemini AI (Simulated)</p>
          </div>
        </footer>
      </div>
    </div>
  );
}

export default App;
