import { useState } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Area, AreaChart } from 'recharts';
import { TrendingUp, DollarSign, Activity, LogOut, Search, BarChart3, AlertCircle } from 'lucide-react';

const API_BASE = 'http://localhost:8000';

const Dashboard = ({ onLogout }) => {
  const [symbol, setSymbol] = useState('');
  const [stockData, setStockData] = useState(null);
  const [prediction, setPrediction] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const username = localStorage.getItem('username');

  const apiCall = async (endpoint, options = {}) => {
    const token = localStorage.getItem('token');
    const headers = {
      'Content-Type': 'application/json',
      ...(token && { 'Authorization': `Bearer ${token}` }),
      ...options.headers,
    };
    
    const response = await fetch(`${API_BASE}${endpoint}`, {
      ...options,
      headers,
    });
    
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || 'Request failed');
    }
    
    return response.json();
  };

  const handleSearch = async (e) => {
    e.preventDefault();
    if (!symbol.trim()) return;

    setLoading(true);
    setError('');
    setPrediction(null);

    try {
      const data = await apiCall(`/api/stocks/${symbol.toUpperCase()}`);
      setStockData(data);
    } catch (err) {
      setError(err.message);
      setStockData(null);
    } finally {
      setLoading(false);
    }
  };

  const handlePredict = async () => {
    if (!symbol.trim()) return;

    setLoading(true);
    setError('');

    try {
      const data = await apiCall('/api/predict', {
        method: 'POST',
        body: JSON.stringify({ symbol: symbol.toUpperCase() }),
      });
      setPrediction(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const chartData = stockData?.prices
    ?.slice(0, 30)
    .reverse()
    .map((p) => ({
      date: new Date(p.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
      price: p.close,
    })) || [];

  const latestPrice = stockData?.prices?.[0]?.close || 0;
  const priceChange = prediction ? (prediction.predicted_next_day_close - latestPrice) : 0;
  const priceChangePercent = prediction ? ((priceChange / latestPrice) * 100) : 0;

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-white shadow-sm border-b">
        <div className="max-w-7xl mx-auto px-4 py-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 bg-indigo-600 rounded-lg flex items-center justify-center">
              <TrendingUp className="text-white" size={24} />
            </div>
            <div>
              <h1 className="text-xl font-bold text-gray-800">Stock Predictor</h1>
              <p className="text-xs text-gray-500">Welcome, {username}</p>
            </div>
          </div>
          <button
            onClick={onLogout}
            className="flex items-center gap-2 px-4 py-2 text-gray-600 hover:text-red-600 hover:bg-red-50 rounded-lg transition"
          >
            <LogOut size={20} />
            <span className="font-medium">Logout</span>
          </button>
        </div>
      </header>

      <div className="max-w-7xl mx-auto px-4 py-8">
        <div className="bg-white rounded-xl shadow-sm p-6 mb-6">
          <form onSubmit={handleSearch} className="flex gap-3">
            <div className="flex-1 relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" size={20} />
              <input
                type="text"
                value={symbol}
                onChange={(e) => setSymbol(e.target.value.toUpperCase())}
                placeholder="Enter stock symbol (e.g., AAPL, MSFT, TSLA)"
                className="w-full pl-10 pr-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-transparent outline-none"
              />
            </div>
            <button
              type="submit"
              disabled={loading}
              className="px-6 py-3 bg-indigo-600 text-white rounded-lg font-semibold hover:bg-indigo-700 transition disabled:opacity-50"
            >
              {loading ? 'Loading...' : 'Search'}
            </button>
          </form>
        </div>

        {error && (
          <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-lg mb-6 flex items-center gap-2">
            <AlertCircle size={20} />
            <span>{error}</span>
          </div>
        )}

        {stockData && (
          <>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-6">
              <div className="bg-white rounded-xl shadow-sm p-6">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-gray-600 text-sm font-medium">Current Price</span>
                  <DollarSign className="text-indigo-600" size={20} />
                </div>
                <div className="text-3xl font-bold text-gray-800">${latestPrice.toFixed(2)}</div>
                <div className="text-xs text-gray-500 mt-1">{stockData.symbol}</div>
              </div>

              <div className="bg-white rounded-xl shadow-sm p-6">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-gray-600 text-sm font-medium">Predicted Price</span>
                  <Activity className="text-green-600" size={20} />
                </div>
                <div className="text-3xl font-bold text-gray-800">
                  {prediction ? `$${prediction.predicted_next_day_close.toFixed(2)}` : '--'}
                </div>
                {prediction && (
                  <div className={`text-sm mt-1 font-semibold ${priceChange >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                    {priceChange >= 0 ? '+' : ''}{priceChange.toFixed(2)} ({priceChangePercent.toFixed(2)}%)
                  </div>
                )}
              </div>

              <div className="bg-white rounded-xl shadow-sm p-6">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-gray-600 text-sm font-medium">Data Points</span>
                  <BarChart3 className="text-purple-600" size={20} />
                </div>
                <div className="text-3xl font-bold text-gray-800">{stockData.prices.length}</div>
                <div className="text-xs text-gray-500 mt-1">Historical records</div>
              </div>
            </div>

            {!prediction && (
              <div className="mb-6">
                <button
                  onClick={handlePredict}
                  disabled={loading}
                  className="w-full bg-gradient-to-r from-indigo-600 to-purple-600 text-white py-4 rounded-xl font-semibold text-lg hover:from-indigo-700 hover:to-purple-700 transition shadow-lg disabled:opacity-50"
                >
                  {loading ? 'Analyzing...' : '🔮 Predict Tomorrow\'s Price'}
                </button>
              </div>
            )}

            <div className="bg-white rounded-xl shadow-sm p-6">
              <h2 className="text-xl font-bold text-gray-800 mb-4">30-Day Price History</h2>
              <ResponsiveContainer width="100%" height={300}>
                <AreaChart data={chartData}>
                  <defs>
                    <linearGradient id="colorPrice" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#4f46e5" stopOpacity={0.3}/>
                      <stop offset="95%" stopColor="#4f46e5" stopOpacity={0}/>
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                  <XAxis dataKey="date" stroke="#6b7280" style={{ fontSize: '12px' }} />
                  <YAxis stroke="#6b7280" style={{ fontSize: '12px' }} />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: '#fff',
                      border: '1px solid #e5e7eb',
                      borderRadius: '8px',
                      boxShadow: '0 4px 6px rgba(0,0,0,0.1)'
                    }}
                  />
                  <Area
                    type="monotone"
                    dataKey="price"
                    stroke="#4f46e5"
                    strokeWidth={2}
                    fill="url(#colorPrice)"
                  />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </>
        )}

        {!stockData && !error && !loading && (
          <div className="text-center py-20">
            <div className="inline-flex items-center justify-center w-20 h-20 bg-indigo-100 rounded-full mb-4">
              <Search className="text-indigo-600" size={40} />
            </div>
            <h3 className="text-xl font-semibold text-gray-800 mb-2">Search for a Stock</h3>
            <p className="text-gray-600">Enter a stock symbol above to view historical data and predictions</p>
          </div>
        )}
      </div>
    </div>
  );
};

export default Dashboard;