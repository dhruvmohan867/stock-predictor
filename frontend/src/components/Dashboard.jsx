import { useState, useEffect } from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { TrendingUp, DollarSign, Activity, LogOut, Search, BarChart3, AlertCircle, Loader, Building, Star , ArrowUp, ArrowDown, Briefcase  } from 'lucide-react'
import { motion, AnimatePresence } from 'framer-motion';

const API_BASE = import.meta.env.VITE_API_BASE || 'https://stock-predictor-ujiu.onrender.com';

// A list of popular stocks for the watchlist feature
const WATCHLIST_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "RELIANCE.NS"];

// --- NEW: Upgraded Logo Component ---
const SYMBOL_DOMAIN = {
  AAPL: 'apple.com',
  MSFT: 'microsoft.com',
  GOOGL: 'abc.xyz',
  GOOG: 'abc.xyz',
  AMZN: 'amazon.com',
  TSLA: 'tesla.com',
  NVDA: 'nvidia.com',
  META: 'meta.com',
  JNJ: 'jnj.com',
  MA: 'mastercard.com',
  F: 'ford.com',
  'RELIANCE.NS': 'ril.com',
  RELIANCE: 'ril.com',
};

const StockLogo = ({ symbol, className }) => {
  const [logoUrl, setLogoUrl] = useState(null);
  const [loading, setLoading] = useState(true);
  const [triedFallback, setTriedFallback] = useState(false);

  useEffect(() => {
    setLoading(true);
    setTriedFallback(false);
    if (!symbol) {
      setLogoUrl(null);
      setLoading(false);
      return;
    }
    const primaryUrl = `https://eodhistoricaldata.com/img/logos/US/${symbol.toUpperCase()}.png`;
    setLogoUrl(primaryUrl);
    setLoading(false);
  }, [symbol]);

  const handleError = () => {
    if (triedFallback) {
      setLogoUrl(null);
      return;
    }
    const domain = SYMBOL_DOMAIN[symbol?.toUpperCase()];
    if (domain) {
      setLogoUrl(`https://logo.clearbit.com/${domain}`);
    } else {
      setLogoUrl(null);
    }
    setTriedFallback(true);
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


const Dashboard = () => {
  const [symbol, setSymbol] = useState('');
  const [activeSymbol, setActiveSymbol] = useState('');
  const [stockData, setStockData] = useState(null);
  const [prediction, setPrediction] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const apiCall = async (endpoint, options = {}) => {
    const headers = { 'Content-Type': 'application/json', ...options.headers };
    const res = await fetch(`${API_BASE}${endpoint}`, { ...options, headers });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.detail || 'Request failed');
    }
    return res.json();
  };

  const executeSearch = async (searchSymbol) => {
    if (!searchSymbol.trim()) return;
    setLoading(true);
    setError('');
    setPrediction(null);
    setActiveSymbol(searchSymbol.toUpperCase());
    try {
      const data = await apiCall(`/api/stocks/${searchSymbol.toUpperCase()}?refresh=1`);
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

  // 🔁 Auto-refresh live data every 60 seconds
  useEffect(() => {
    if (!activeSymbol) return;

    const interval = setInterval(async () => {
      try {
        const res = await fetch(`${API_BASE}/api/live/${activeSymbol}`);
        if (res.ok) {
          const json = await res.json();
          setStockData((prev) => ({
            ...prev,
            live_info: json.live_info,
          }));
        }
      } catch (err) {
        console.warn("Live refresh failed:", err);
      }
    }, 60000); // every 60 seconds

    return () => clearInterval(interval);
  }, [activeSymbol]);

  const handlePredict = async () => {
    if (!activeSymbol) return;
    setLoading(true);
    setError('');
    try {
      const data = await apiCall('/api/predict', { method: 'POST', body: JSON.stringify({ symbol: activeSymbol }) });
      setPrediction(data);
      if (data.live_info) {
        setStockData((prev) => ({
          ...prev,
          live_info: data.live_info,
        }));
      }
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

  const latestPrice = stockData?.live_info?.currentPrice ?? stockData?.prices?.[0]?.close ?? 0;

  const formatLargeNumber = (num) => {
    if (!num && num !== 0) return '--';
    if (num >= 1_000_000_000_000) return `${(num / 1_000_000_000_000).toFixed(2)}T`;
    if (num >= 1_000_000_000) return `${(num / 1_000_000_000).toFixed(2)}B`;
    if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(2)}M`;
    return `${num}`;
  };

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
        </div>
      </header>
    
      <div className="max-w-screen-2xl mx-auto px-4 sm:px-6 lg:px-8 py-8 grid grid-cols-1 lg:grid-cols-4 gap-8">
        {/* Sidebar */}
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
                <button key={s} onClick={() => executeSearch(s)} className={`flex items-center gap-2 px-3 py-1 text-sm font-medium rounded-full transition ${activeSymbol === s ? 'bg-indigo-600 text-white' : 'bg-gray-700/50 hover:bg-gray-600'}`}>
                  <StockLogo symbol={s} className="w-4 h-4 rounded-full" />
                  {s}
                </button>
              ))}
            </div>
          </div>
        </aside>

        {/* --- Main --- */}
        <main className="lg:col-span-3 relative">
          <AnimatePresence>
            {loading && (
              <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} className="absolute inset-0 bg-gray-900/50 flex items-center justify-center z-10 rounded-xl">
                <Loader className="animate-spin text-indigo-500" size={48} />
              </motion.div>
            )}
          </AnimatePresence>
          
          <AnimatePresence>
            {error && !loading && (
              <motion.div initial={{ opacity: 0, y: -10 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }} className="bg-red-900/50 border border-red-700 text-red-300 px-4 py-3 rounded-lg mb-6 flex items-center gap-2">
                <AlertCircle size={20} /> <span>{error}</span>
              </motion.div>
            )}
          </AnimatePresence>

          {stockData ? (
            <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="space-y-8">
              <div className="flex items-center gap-4">
                <StockLogo symbol={stockData.symbol} className="w-10 h-10 rounded-full bg-gray-700" />
                <div>
                  <h1 className="text-4xl font-bold">{stockData.symbol}</h1>
                  {stockData.company_name && <p className="text-gray-400">{stockData.company_name}</p>}
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                <InfoCard title="Current Price" value={latestPrice ? `$${Number(latestPrice).toFixed(2)}` : '--'} icon={DollarSign} color="indigo" />
                <InfoCard title="Market Cap" value={formatLargeNumber(stockData?.live_info?.marketCap)} icon={Briefcase} color="blue" />
                <InfoCard title="Day's High" value={stockData?.live_info?.dayHigh ? `$${Number(stockData.live_info.dayHigh).toFixed(2)}` : '--'} icon={ArrowUp} color="green" />
                <InfoCard title="Day's Low" value={stockData?.live_info?.dayLow ? `$${Number(stockData.live_info.dayLow).toFixed(2)}` : '--'} icon={ArrowDown} color="red" />
              </div>

              {prediction && (
                <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                  <InfoCard title="Predicted Next Close" value={`$${Number(prediction.predicted_next_day_close).toFixed(2)}`} icon={Activity} color="yellow" sub={`vs current $${Number(latestPrice).toFixed(2)}`} />
                  <InfoCard title="Change" value={`${(prediction.predicted_next_day_close - latestPrice >= 0 ? '▲' : '▼')} $${Math.abs(prediction.predicted_next_day_close - latestPrice).toFixed(2)}`} icon={TrendingUp} color={prediction.predicted_next_day_close - latestPrice >= 0 ? 'green' : 'red'} />
                  <button onClick={() => setPrediction(null)} className="bg-gray-800/50 border border-gray-700 rounded-xl p-6 text-left font-semibold hover:bg-gray-700 transition">
                    Clear Prediction
                  </button>
                </div>
              )}

              {!prediction && (
                <motion.div whileHover={{ scale: 1.01 }} whileTap={{ scale: 0.99 }}>
                  <button onClick={handlePredict} disabled={loading} className="w-full bg-green-600 text-white py-3 rounded-lg font-semibold hover:bg-green-700 transition disabled:opacity-50 flex items-center justify-center gap-2">
                    <Activity size={20} /> Predict Next Day
                  </button>
                </motion.div>
              )}

              <div className="bg-gray-800/50 border border-gray-700 rounded-xl p-6 h-96">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={chartData} margin={{ top: 5, right: 20, left: -10, bottom: 5 }}>
                    <defs>
                      <linearGradient id="colorPrice" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#667EEA" stopOpacity={0.8}/>
                        <stop offset="95%" stopColor="#667EEA" stopOpacity={0}/>
                      </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="#4A5568" />
                    <XAxis dataKey="date" stroke="#A0AEC0" fontSize={12} />
                    <YAxis stroke="#A0AEC0" fontSize={12} domain={['dataMin - 5', 'dataMax + 5']} />
                    <Tooltip contentStyle={{ backgroundColor: '#1A202C', border: '1px solid #4A5568', color: '#E2E8F0' }} itemStyle={{ color: '#E2E8F0' }} labelStyle={{ color: '#A0AEC0' }} />
                    <Area type="monotone" dataKey="price" stroke="#818CF8" fillOpacity={1} fill="url(#colorPrice)" />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            </motion.div>
          ) : (
            !loading && (
              <div className="flex flex-col items-center justify-center h-full text-center text-gray-500">
                <BarChart3 size={48} className="mb-4" />
                <h2 className="text-xl font-semibold">No Data Available</h2>
                <p>Could not load data for the selected stock. Please try another symbol.</p>
              </div>
            )
          )}
        </main>
      </div>
    </div>
  );
};

// --- Reusable InfoCard ---
const InfoCard = ({ title, value, icon: Icon, color, change, sub }) => (
  <div className="bg-gray-800/50 border border-gray-700 rounded-xl shadow-lg p-6">
    <div className="flex items-center justify-between mb-2">
      <span className="text-gray-400 text-sm font-medium">{title}</span>
      <Icon className={`text-${color}-400`} size={20} />
    </div>
    <div className="text-3xl font-bold">{value}</div>
    {change ? (
      <div className={`text-sm mt-1 font-semibold ${change.value >= 0 ? 'text-green-400' : 'text-red-400'}`}>
        {change.value >= 0 ? '▲' : '▼'} {Math.abs(change.value).toFixed(2)} ({change.percent.toFixed(2)}%)
      </div>
    ) : sub && <div className="text-xs text-gray-500 mt-1">{sub}</div>}
  </div>
);

export default Dashboard;
