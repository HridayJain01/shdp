import { AlertCircle, CheckCircle2 } from 'lucide-react';

interface ComparisonViewProps {
  original: Record<string, unknown>;
  cleaned: Record<string, unknown>;
  dirtyFields: string[];
}

export function ComparisonView({ original, cleaned, dirtyFields }: ComparisonViewProps) {
  const hasChanges = JSON.stringify(original) !== JSON.stringify(cleaned);

  const getFieldStatus = (key: string, originalValue: unknown, cleanedValue: unknown) => {
    const isDirty = dirtyFields.some(field => field.toLowerCase().includes(key.toLowerCase()));
    const hasChanged = JSON.stringify(originalValue) !== JSON.stringify(cleanedValue);

    if (hasChanged && isDirty) return 'healed';
    if (isDirty) return 'dirty';
    return 'clean';
  };

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'healed':
        return (
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-green-500/10 text-green-400 border border-green-500/20">
            <CheckCircle2 className="w-3 h-3" />
            Healed
          </span>
        );
      case 'dirty':
        return (
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-red-500/10 text-red-400 border border-red-500/20">
            <AlertCircle className="w-3 h-3" />
            Dirty
          </span>
        );
      default:
        return (
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-gray-500/10 text-gray-400 border border-gray-500/20">
            <CheckCircle2 className="w-3 h-3" />
            Clean
          </span>
        );
    }
  };

  return (
    <div className="bg-gray-800/50 border-2 border-gray-700 rounded-xl p-6 backdrop-blur-sm">
      <div className="flex items-center justify-between mb-6">
        <h2 className="text-xl font-bold text-white">Data Comparison</h2>
        {hasChanges ? (
          <span className="px-3 py-1 rounded-full text-sm font-medium bg-blue-500/10 text-blue-400 border border-blue-500/20">
            Transformations Applied
          </span>
        ) : (
          <span className="px-3 py-1 rounded-full text-sm font-medium bg-green-500/10 text-green-400 border border-green-500/20">
            No Changes Needed
          </span>
        )}
      </div>

      <div className="grid md:grid-cols-2 gap-6">
        <div>
          <h3 className="text-sm font-semibold text-gray-400 mb-3 flex items-center gap-2">
            <AlertCircle className="w-4 h-4" />
            Original Data
          </h3>
          <div className="space-y-3">
            {Object.entries(original).map(([key, value]) => {
              const status = getFieldStatus(key, value, cleaned[key]);
              return (
                <div
                  key={key}
                  className={`p-3 rounded-lg border transition-all ${
                    status === 'healed'
                      ? 'bg-orange-500/5 border-orange-500/20'
                      : status === 'dirty'
                      ? 'bg-red-500/5 border-red-500/20'
                      : 'bg-gray-900/50 border-gray-700'
                  }`}
                >
                  <div className="flex items-center justify-between mb-1">
                    <span className="text-xs font-medium text-gray-400 uppercase">{key}</span>
                    {getStatusBadge(status)}
                  </div>
                  <div className="text-white font-mono text-sm break-all">
                    {value === '' ? (
                      <span className="text-gray-500 italic">(empty)</span>
                    ) : (
                      String(value)
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        <div>
          <h3 className="text-sm font-semibold text-gray-400 mb-3 flex items-center gap-2">
            <CheckCircle2 className="w-4 h-4" />
            Cleaned Data
          </h3>
          <div className="space-y-3">
            {Object.entries(cleaned).map(([key, value]) => {
              const status = getFieldStatus(key, original[key], value);
              return (
                <div
                  key={key}
                  className={`p-3 rounded-lg border transition-all ${
                    status === 'healed'
                      ? 'bg-green-500/5 border-green-500/20'
                      : 'bg-gray-900/50 border-gray-700'
                  }`}
                >
                  <div className="flex items-center justify-between mb-1">
                    <span className="text-xs font-medium text-gray-400 uppercase">{key}</span>
                    {status === 'healed' && (
                      <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-green-500/10 text-green-400 border border-green-500/20">
                        <CheckCircle2 className="w-3 h-3" />
                        Fixed
                      </span>
                    )}
                  </div>
                  <div className="text-white font-mono text-sm break-all">
                    {value === '' ? (
                      <span className="text-gray-500 italic">(empty)</span>
                    ) : (
                      String(value)
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}
