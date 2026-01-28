import { useState } from 'react';
import { Send, Loader2 } from 'lucide-react';

interface DataFormProps {
  onSubmit: (data: FormData) => void;
  isProcessing: boolean;
}

export interface FormData {
  name: string;
  email: string;
  age: string;
  salary: string;
  country: string;
}

export function DataForm({ onSubmit, isProcessing }: DataFormProps) {
  const [formData, setFormData] = useState<FormData>({
    name: '',
    email: '',
    age: '',
    salary: '',
    country: '',
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onSubmit(formData);
  };

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setFormData(prev => ({
      ...prev,
      [e.target.name]: e.target.value,
    }));
  };

  const loadDirtyExample = () => {
    setFormData({
      name: '  John Doe  ',
      email: 'JOHN.DOE@Example.COM',
      age: '-5',
      salary: '$75,000 USD',
      country: '',
    });
  };

  const loadCleanExample = () => {
    setFormData({
      name: 'Jane Smith',
      email: 'jane.smith@example.com',
      age: '28',
      salary: '65000',
      country: 'USA',
    });
  };

  return (
    <div className="bg-gray-800/50 border-2 border-gray-700 rounded-xl p-6 backdrop-blur-sm">
      <div className="flex justify-between items-center mb-6">
        <h2 className="text-xl font-bold text-white">Data Ingestion</h2>
        <div className="flex gap-2">
          <button
            type="button"
            onClick={loadDirtyExample}
            className="text-xs px-3 py-1.5 rounded-md bg-orange-500/10 text-orange-400 border border-orange-500/20 hover:bg-orange-500/20 transition-colors"
          >
            Load Dirty Data
          </button>
          <button
            type="button"
            onClick={loadCleanExample}
            className="text-xs px-3 py-1.5 rounded-md bg-green-500/10 text-green-400 border border-green-500/20 hover:bg-green-500/20 transition-colors"
          >
            Load Clean Data
          </button>
        </div>
      </div>

      <form onSubmit={handleSubmit} className="space-y-4">
        <div>
          <label htmlFor="name" className="block text-sm font-medium text-gray-300 mb-1.5">
            Name
          </label>
          <input
            type="text"
            id="name"
            name="name"
            value={formData.name}
            onChange={handleChange}
            className="w-full px-4 py-2.5 bg-gray-900/50 border border-gray-600 rounded-lg text-white placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all"
            placeholder="Enter name"
          />
        </div>

        <div>
          <label htmlFor="email" className="block text-sm font-medium text-gray-300 mb-1.5">
            Email
          </label>
          <input
            type="text"
            id="email"
            name="email"
            value={formData.email}
            onChange={handleChange}
            className="w-full px-4 py-2.5 bg-gray-900/50 border border-gray-600 rounded-lg text-white placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all"
            placeholder="Enter email"
          />
        </div>

        <div className="grid grid-cols-2 gap-4">
          <div>
            <label htmlFor="age" className="block text-sm font-medium text-gray-300 mb-1.5">
              Age
            </label>
            <input
              type="text"
              id="age"
              name="age"
              value={formData.age}
              onChange={handleChange}
              className="w-full px-4 py-2.5 bg-gray-900/50 border border-gray-600 rounded-lg text-white placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all"
              placeholder="Enter age"
            />
          </div>

          <div>
            <label htmlFor="salary" className="block text-sm font-medium text-gray-300 mb-1.5">
              Salary
            </label>
            <input
              type="text"
              id="salary"
              name="salary"
              value={formData.salary}
              onChange={handleChange}
              className="w-full px-4 py-2.5 bg-gray-900/50 border border-gray-600 rounded-lg text-white placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all"
              placeholder="Enter salary"
            />
          </div>
        </div>

        <div>
          <label htmlFor="country" className="block text-sm font-medium text-gray-300 mb-1.5">
            Country
          </label>
          <input
            type="text"
            id="country"
            name="country"
            value={formData.country}
            onChange={handleChange}
            className="w-full px-4 py-2.5 bg-gray-900/50 border border-gray-600 rounded-lg text-white placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all"
            placeholder="Enter country"
          />
        </div>

        <button
          type="submit"
          disabled={isProcessing}
          className="w-full px-6 py-3 bg-gradient-to-r from-blue-500 to-cyan-500 text-white font-semibold rounded-lg hover:from-blue-600 hover:to-cyan-600 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 focus:ring-offset-gray-900 transition-all disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2"
        >
          {isProcessing ? (
            <>
              <Loader2 className="w-5 h-5 animate-spin" />
              Processing...
            </>
          ) : (
            <>
              <Send className="w-5 h-5" />
              Process Data
            </>
          )}
        </button>
      </form>
    </div>
  );
}
