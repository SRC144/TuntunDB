import React, { useState } from 'react';
import { ArrowPathIcon, PlayIcon } from '@heroicons/react/24/solid';
import QueryEditor from './components/QueryEditor';
import ResultsTable from './components/ResultsTable';

const EXAMPLE_QUERIES = [
  `CREATE TABLE Restaurantes (
    id INT KEY INDEX BPlusTree,
    nombre VARCHAR[20] INDEX BPlusTree,
    fechaRegistro DATE,
    ubicacion ARRAY[FLOAT]
  );`,
  `INSERT INTO Restaurantes VALUES (1, "El Buen Sabor", "2024-01-01", "10.5,20.3");`,
  `INSERT INTO Restaurantes VALUES (2, "La Pizzeria", "2024-01-02", "15.7,25.1");`,
  `SELECT * FROM Restaurantes WHERE nombre BETWEEN "A" AND "M";`
];

const API_URL = 'http://localhost:5000/api';

function App() {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const executeQuery = async () => {
    try {
      setLoading(true);
      setError(null);
      setResults(null);

      const response = await fetch(`${API_URL}/query`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ query }),
      });

      if (!response.ok) {
        // Try to get error message from response
        try {
          const errorData = await response.json();
          throw new Error(errorData.error || `HTTP error! status: ${response.status}`);
        } catch (e) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
      }

      const data = await response.json();
      setResults(data);
    } catch (err) {
      let errorMessage = 'An error occurred while executing the query. ';
      
      if (err.message === 'Failed to fetch') {
        errorMessage += 'Could not connect to the server. Please make sure the backend is running on port 5000.';
      } else {
        errorMessage += err.message;
      }
      
      setError(errorMessage);
      setResults(null);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 py-8">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="bg-white rounded-lg shadow">
          {/* Header */}
          <div className="px-6 py-4 border-b border-gray-200">
            <h1 className="text-2xl font-semibold text-gray-900">DB Query Interface</h1>
            <p className="mt-1 text-sm text-gray-500">
              Make sure the backend API is running on port 5000 before executing queries.
            </p>
          </div>

          {/* Query Section */}
          <div className="p-6">
            <QueryEditor
              value={query}
              onChange={setQuery}
              onExecute={executeQuery}
              loading={loading}
            />

            {/* Example Queries */}
            <div className="mt-4">
              <h3 className="text-sm font-medium text-gray-700 mb-2">Example Queries:</h3>
              <div className="space-y-2">
                {EXAMPLE_QUERIES.map((exampleQuery, index) => (
                  <button
                    key={index}
                    onClick={() => setQuery(exampleQuery)}
                    className="block text-left text-sm text-blue-600 hover:text-blue-800 font-mono"
                  >
                    {exampleQuery.split('\n')[0]}...
                  </button>
                ))}
              </div>
            </div>

            {/* Results Section */}
            <div className="mt-6">
              {error && (
                <div className="bg-red-50 border border-red-200 rounded-md p-4">
                  <div className="flex">
                    <div className="flex-shrink-0">
                      <svg className="h-5 w-5 text-red-400" viewBox="0 0 20 20" fill="currentColor">
                        <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
                      </svg>
                    </div>
                    <div className="ml-3">
                      <h3 className="text-sm font-medium text-red-800">Error</h3>
                      <div className="mt-2 text-sm text-red-700">{error}</div>
                    </div>
                  </div>
                </div>
              )}
              {results && <ResultsTable results={results} />}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App; 