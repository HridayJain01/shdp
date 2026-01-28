import { BookOpen, User, Sparkles } from 'lucide-react';

interface Rule {
  id: number;
  name: string;
  description: string;
  source: string;
  active: boolean;
  createdAt?: string;
}

interface RulebookPanelProps {
  rules: Rule[];
  appliedRules?: Array<{ id: number; name: string; source: string }>;
}

export function RulebookPanel({ rules, appliedRules = [] }: RulebookPanelProps) {
  const humanRules = rules.filter(r => r.source === 'Human');
  const aiRules = rules.filter(r => r.source === 'AI Generated');

  const isRuleApplied = (ruleId: number) => {
    return appliedRules.some(r => r.id === ruleId);
  };

  return (
    <div className="bg-gray-800/50 border-2 border-gray-700 rounded-xl p-6 backdrop-blur-sm">
      <div className="flex items-center gap-3 mb-6">
        <div className="p-2 rounded-lg bg-gradient-to-br from-blue-500 to-cyan-500">
          <BookOpen className="w-5 h-5 text-white" />
        </div>
        <div>
          <h2 className="text-xl font-bold text-white">Rulebook</h2>
          <p className="text-sm text-gray-400">{rules.length} total rules ({aiRules.length} learned from AI)</p>
        </div>
      </div>

      <div className="space-y-6">
        <div>
          <div className="flex items-center gap-2 mb-3">
            <User className="w-4 h-4 text-blue-400" />
            <h3 className="text-sm font-semibold text-blue-400">Human-Defined Rules</h3>
            <span className="px-2 py-0.5 rounded-full text-xs bg-blue-500/10 text-blue-400 border border-blue-500/20">
              {humanRules.length}
            </span>
          </div>
          <div className="space-y-2">
            {humanRules.map(rule => (
              <div
                key={rule.id}
                className={`p-3 rounded-lg border transition-all ${
                  isRuleApplied(rule.id)
                    ? 'bg-blue-500/10 border-blue-500/30 ring-1 ring-blue-500/50'
                    : 'bg-gray-900/50 border-gray-700'
                }`}
              >
                <div className="flex items-start justify-between gap-2">
                  <div className="flex-1">
                    <div className="font-medium text-white text-sm">{rule.name}</div>
                    <div className="text-xs text-gray-400 mt-1">{rule.description}</div>
                  </div>
                  {isRuleApplied(rule.id) && (
                    <span className="px-2 py-0.5 rounded-full text-xs bg-blue-500/20 text-blue-300 border border-blue-500/30 whitespace-nowrap">
                      Applied
                    </span>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>

        {aiRules.length > 0 && (
          <div>
            <div className="flex items-center gap-2 mb-3">
              <Sparkles className="w-4 h-4 text-cyan-400" />
              <h3 className="text-sm font-semibold text-cyan-400">AI-Generated Rules</h3>
              <span className="px-2 py-0.5 rounded-full text-xs bg-cyan-500/10 text-cyan-400 border border-cyan-500/20">
                {aiRules.length}
              </span>
            </div>
            <div className="space-y-2">
              {aiRules.map(rule => (
                <div
                  key={rule.id}
                  className={`p-3 rounded-lg border transition-all ${
                    isRuleApplied(rule.id)
                      ? 'bg-cyan-500/10 border-cyan-500/30 ring-1 ring-cyan-500/50'
                      : 'bg-gray-900/50 border-gray-700'
                  }`}
                >
                  <div className="flex items-start justify-between gap-2">
                    <div className="flex-1">
                      <div className="flex items-center gap-2">
                        <div className="font-medium text-white text-sm">{rule.name}</div>
                        <span className="px-1.5 py-0.5 rounded text-xs bg-gradient-to-r from-cyan-500/20 to-blue-500/20 text-cyan-300 border border-cyan-500/20">
                          NEW
                        </span>
                      </div>
                      <div className="text-xs text-gray-400 mt-1">{rule.description}</div>
                      {rule.createdAt && (
                        <div className="text-xs text-gray-500 mt-1">
                          Learned: {new Date(rule.createdAt).toLocaleTimeString()}
                        </div>
                      )}
                    </div>
                    {isRuleApplied(rule.id) && (
                      <span className="px-2 py-0.5 rounded-full text-xs bg-cyan-500/20 text-cyan-300 border border-cyan-500/30 whitespace-nowrap">
                        Applied
                      </span>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
