import React from 'react';

function ResultsTable({ results }) {
  // Helper function to determine if the result contains records
  const hasRecords = (result) => {
    return result && result.records && Array.isArray(result.records);
  };

  // Helper function to format values for display
  const formatValue = (value) => {
    if (value === null || value === undefined) return 'NULL';
    if (typeof value === 'object') return JSON.stringify(value);
    if (typeof value === 'boolean') return value.toString();
    // Convert Unix timestamp to date if it looks like a timestamp
    if (typeof value === 'number' && value > 1000000000 && value < 10000000000) {
      return new Date(value * 1000).toISOString().split('T')[0];
    }
    return value.toString();
  };

  // For SELECT queries
  if (results.records) {
    return (
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              {results.columns?.map((column, i) => (
                <th
                  key={i}
                  className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                >
                  {column.name}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {results.records.map((record, i) => (
              <tr key={i} className={i % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                {record.map((value, j) => (
                  <td key={j} className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 font-mono">
                    {formatValue(value)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  }

  // For non-SELECT queries (CREATE, INSERT, etc.)
  return (
    <div className="rounded-md bg-green-50 p-4">
      <div className="flex">
        <div className="flex-shrink-0">
          <svg
            className="h-5 w-5 text-green-400"
            xmlns="http://www.w3.org/2000/svg"
            viewBox="0 0 20 20"
            fill="currentColor"
          >
            <path
              fillRule="evenodd"
              d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z"
              clipRule="evenodd"
            />
          </svg>
        </div>
        <div className="ml-3">
          <h3 className="text-sm font-medium text-green-800">Query executed successfully</h3>
          <div className="mt-2 text-sm text-green-700">
            <pre className="whitespace-pre-wrap font-mono">
              {JSON.stringify(results, null, 2)}
            </pre>
          </div>
        </div>
      </div>
    </div>
  );
}

export default ResultsTable; 