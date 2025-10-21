import { useState, useEffect } from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { TrendingUp, DollarSign, Activity, LogOut, Search, BarChart3, AlertCircle, Loader, Building, Star } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

const API_BASE = import.meta.env.VITE_API_BASE || 'https://stock-predictor-ujiu.onrender.com';

// A list of popular stocks for the watchlist feature
const WATCHLIST_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA"];

// --- NEW: Upgraded Logo Component ---
// This component now tries multiple sources to find a logo, making it far more reliable.
const StockLogo = ({ symbol, className }) => {
  const [logoUrl, setLogoUrl] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const findLogo = async () => {
      setLoading(true);
      setLogoUrl(null);

      if (!symbol) {
        setLoading(false);
        return;
      }

      // --- STRATEGY 1: Try the direct symbol-based logo provider (fastest) ---
      const primaryUrl = `https://eodhistoricaldata.com/img/logos/US/${symbol.toUpperCase()}.png`;
      
      // --- STRATEGY 2: Use a lookup service to get company info, then use Clearbit ---
      // This is our powerful fallback.
      try {
        // This free API gives us the company's website domain from its stock symbol.
        const response = await fetch(`https://company.bigpicture.io/v1/companies/find?companyName=${symbol}`);
        if (response.ok) {
          const data = await response.json();
          if (data && data.length > 0 && data[0].domain) {
            // Now use the domain with the Clearbit logo API.
            const clearbitUrl = `https://logo.clearbit.com/${data[0].domain}`;
            setLogoUrl(clearbitUrl);
            setLoading(false);
            return; // Found a logo, we're done.
          }
        }
      } catch (error) {
        console.warn(`Could not find domain for ${symbol}, falling back.`);
      }

      // If the second strategy fails, fall back to the first one.
      setLogoUrl(primaryUrl);
      setLoading(false);
    };

    findLogo();
  }, [symbol]);

  const handleError = () => {
    // This is the final fallback if all strategies fail.
    setLogoUrl(null);
    setLoading(false);
  };

  if (loading) {
    return <div className={`flex items-center justify-center bg-gray-700 rounded-full ${className}`} />;
  }

  if (!logoUrl) {
    return (
      <div className={`flex items-center justify-center bg-gray-700 rounded-full ${className}`}>
        <Building size="60%" />
      </div>
    );
  }

  return <img src={logoUrl} alt={`${symbol} logo`} className={className} onError={handleError} />;
};


const Dashboard = ({ onLogout }) => {
  const [symbol, setSymbol] = useState('');
  const [activeSymbol, setActiveSymbol] = useState('');
  const [stockData, setStockData] = useState(null);
  const [prediction, setPrediction] = useState(null);
  const [loading, setLoading] = useState(true); // Start loading on initial load
  const [error, setError] = useState('');
  const username = localStorage.getItem('username');

  const apiCall = async (endpoint, options = {}) => {
    const token = localStorage.getItem('token');
    const headers = { 'Content-Type': 'application/json', ...(token && { 'Authorization': `Bearer ${token}` }), ...options.headers };
    const response = await fetch(`${API_BASE}${endpoint}`, { ...options, headers });
    if (!response.ok) {
      const errorData = await response.json();
      throw new Error(errorData.detail || 'Request failed');
    }
    return response.json();
  };

  const executeSearch = async (searchSymbol) => {
    if (!searchSymbol.trim()) return;
    setLoading(true);
    setError('');
    setPrediction(null);
    setActiveSymbol(searchSymbol.toUpperCase());

    try {
      const data = await apiCall(`/api/stocks/${searchSymbol.toUpperCase()}`);
      setStockData(data);
    } catch (err) {
      setError(err.message);
      setStockData(null);
    } finally {
      setLoading(false);
    }
  };

  // Automatically load data for a default stock when the component mounts
  useEffect(() => {
    executeSearch('MSFT'); // Load Microsoft data by default
  }, []);

  const handlePredict = async () => {
    if (!activeSymbol) return;
    setLoading(true);
    setError('');
    try {
      const data = await apiCall('/api/predict', { method: 'POST', body: JSON.stringify({ symbol: activeSymbol }) });
      setPrediction(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const chartData = stockData?.prices?.slice(0, 30).reverse().map((p) => ({
    date: new Date(p.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
    price: p.close,
  })) || [];

  const latestPrice = stockData?.prices?.[0]?.close || 0;
  const priceChange = prediction ? (prediction.predicted_next_day_close - latestPrice) : 0;
  const priceChangePercent = prediction && latestPrice ? ((priceChange / latestPrice) * 100) : 0;

  return (
    <div className="min-h-screen bg-gray-900 text-white">
      <header className="bg-gray-800/50 backdrop-blur-xl border-b border-gray-700 sticky top-0 z-20">
        <div className="max-w-screen-2xl mx-auto px-4 sm:px-6 lg:px-8 py-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 bg-indigo-600 rounded-lg flex items-center justify-center shadow-lg shadow-indigo-600/30">
              <TrendingUp size={24} />
            </div>
            <h1 className="text-xl font-bold">Stock Predictor</h1>
          </div>
          <div className="flex items-center gap-4">
            <span className="text-sm text-gray-400 hidden sm:block">Welcome, {username}</span>
            <button onClick={onLogout} className="flex items-center gap-2 px-3 py-2 text-gray-400 hover:text-red-400 hover:bg-gray-700/50 rounded-lg transition">
              <LogOut size={20} />
            </button>
          </div>
        </div>
      </header>

      <div className="max-w-screen-2xl mx-auto px-4 sm:px-6 lg:px-8 py-8 grid grid-cols-1 lg:grid-cols-4 gap-8">
        {/* --- Left Sidebar --- */}
        <aside className="lg:col-span-1 space-y-6">
          <form onSubmit={(e) => { e.preventDefault(); executeSearch(symbol); }} className="bg-gray-800/50 border border-gray-700 rounded-xl p-4">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-500" size={20} />
              <input
                type="text" value={symbol} onChange={(e) => setSymbol(e.target.value)}
                placeholder="Search symbol..."
                className="w-full pl-10 pr-4 py-2 bg-gray-900/50 border border-gray-600 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-transparent outline-none transition"
              />
            </div>
          </form>

          <div className="bg-gray-800/50 border border-gray-700 rounded-xl p-4">
            <h2 className="text-lg font-semibold mb-4 flex items-center gap-2"><Star size={20} className="text-yellow-400" /> Market Movers</h2>
            <div className="flex flex-wrap gap-2">
              {WATCHLIST_SYMBOLS.map(s => (
                <button key={s} onClick={() => executeSearch(s)} className={`px-3 py-1 text-sm font-medium rounded-full transition ${activeSymbol === s ? 'bg-indigo-600 text-white' : 'bg-gray-700/50 hover:bg-gray-600'}`}>
                  {s}
                </button>
              ))}
            </div>
          </div>
        </aside>

        {/* --- Main Content --- */}
        <main className="lg:col-span-3">
          <AnimatePresence>
            {loading && (
              <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} className="absolute inset-0 bg-gray-900/50 flex items-center justify-center z-10">
                <Loader className="animate-spin text-indigo-500" size={48} />
              </motion.div>
            )}
          </AnimatePresence>
          
          <AnimatePresence>
            {error && (
              <motion.div initial={{ opacity: 0, y: -10 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }} className="bg-red-900/50 border border-red-700 text-red-300 px-4 py-3 rounded-lg mb-6 flex items-center gap-2">
                <AlertCircle size={20} /> <span>{error}</span>
              </motion.div>
            )}
          </AnimatePresence>

          {stockData ? (
            <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="space-y-8">
              <div className="flex items-baseline gap-4">
                <Building size={32} className="text-gray-500" />
                <h1 className="text-4xl font-bold">{stockData.symbol}</h1>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                {[
                  { title: 'Current Price', value: `$${latestPrice.toFixed(2)}`, icon: DollarSign, color: 'indigo' },
                  { title: 'Predicted Price', value: prediction ? `$${prediction.predicted_next_day_close.toFixed(2)}` : '--', icon: Activity, color: 'green', change: prediction ? { value: priceChange, percent: priceChangePercent } : null },
                  { title: 'Data Points', value: stockData.prices.length, icon: BarChart3, color: 'purple', sub: 'records' }
                ].map(item => (
                  <div key={item.title} className="bg-gray-800/50 border border-gray-700 rounded-xl shadow-lg p-6">
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-gray-400 text-sm font-medium">{item.title}</span>
                      <item.icon className={`text-${item.color}-400`} size={20} />
                    </div>
                    <div className="text-3xl font-bold">{item.value}</div>
                    {item.change ? (
                      <div className={`text-sm mt-1 font-semibold ${item.change.value >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                        {item.change.value >= 0 ? '▲' : '▼'} {Math.abs(item.change.value).toFixed(2)} ({item.change.percent.toFixed(2)}%)
                      </div>
                    ) : item.sub && <div className="text-xs text-gray-500 mt-1">{item.sub}</div>}
                  </div>
                ))}
              </div>

              {!prediction && (
                <motion.div whileHover={{ scale: 1.01 }} whileTap={{ scale: 0.99 }}>