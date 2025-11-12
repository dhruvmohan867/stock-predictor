import { useState, useEffect, useMemo } from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts';
import { TrendingUp, DollarSign, Search, BarChart3, AlertCircle, Loader, Building, Star, ArrowUp, ArrowDown, Briefcase, Activity, BrainCircuit } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

const API_BASE = import.meta.env.VITE_API_BASE || 'https://stock-predictor-ujiu.onrender.com';

const WATCHLIST_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "RELIANCE.NS", "TCS.NS", "TSLA", "^NSEI", "^GSPC"];

const StockLogo = ({ symbol, className }) => {
  const [logoUrl, setLogoUrl] = useState(null);
  const [error, setError] = useState(false);

  useEffect(() => {
    setError(false);
    if (!symbol || symbol.startsWith('^')) {
      setLogoUrl(null);
      return;
    }
    // Use a more reliable logo provider
    const sanitizedSymbol = symbol.split('.')[0];
    setLogoUrl(`https://api.twelvedata.com/logo/${sanitizedSymbol}.png`);
  }, [symbol]);

  if (error || !logoUrl) {
    return (
      <div className={`flex items-center justify-center bg-gray-700 rounded-full ${className}`}>
        <Building size="60%" />
      </div>
    );
  }
  return <img src={logoUrl} alt={`${symbol} logo`} className={className} onError={() => setError(true)} />;
};

const Dashboard = () => {
  const [symbol, setSymbol] = useState('');
  const [activeSymbol, setActiveSymbol] = useState('MSFT');
  const [stockData, setStockData] = useState(null);
  const [prediction, setPrediction] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const apiCall = async (endpoint, options = {}) => {
    const res = await fetch(`${API_BASE}${endpoint}`, { headers: { 'Content-Type': 'application/json' }, ...options });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.detail || `Request failed with status ${res.status}`);
    }
    return res.json();
  };

  const executeSearch = async (searchSymbol) => {
    if (!searchSymbol?.trim()) return;
    setLoading(true);
    setError('');
    setPrediction(null);
    const upperSymbol = searchSymbol.toUpperCase();
    setActiveSymbol(upperSymbol);
    try {
      const data = await apiCall(`/api/stocks/${upperSymbol}`);
      setStockData(data);
    } catch (err) {
      setError(err.message);
      setStockData(null);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    executeSearch(activeSymbol);
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

  const chartData = useMemo(() => {
    const data = stockData?.prices?.slice(0, 90).reverse().map(p => ({
      date: new Date(p.date),
      price: p.close,
    })) || [];
    
    if (prediction && data.length > 0) {
      const lastDate = data[data.length - 1].date;
      const nextDay = new Date(lastDate);
      nextDay.setDate(lastDate.getDate() + 1);
      return [...data, { date: nextDay, prediction: prediction.predicted_next_day_close }];
    }
    return data;
  }, [stockData, prediction]);

  const lastDaily = stockData?.prices?.[0];
  const latestPrice = lastDaily?.close ?? 0;
  const priceChange = lastDaily ? (lastDaily.close - (stockData.prices[1]?.close ?? 0)) : 0;
  const priceChangePercent = latestPrice ? (priceChange / (stockData.prices[1]?.close ?? 1)) * 100 : 0;

  return (
    <div className="w-full min-h-screen bg-gray-900 text-gray-200 font-sans flex">
      {/* Sidebar */}
      <aside className="w-72 bg-gray-950/50 border-r border-gray-800 p-6 flex flex-col">
        <div className="flex items-center gap-3 mb-8">
          <div className="w-10 h-10 bg-indigo-600 rounded-lg flex items-center justify-center shadow-lg shadow-indigo-600/30">
            <TrendingUp size={24} />
          </div>
          <h1 className="text-xl font-bold">AlphaPredict</h1>
        </div>
        <form onSubmit={(e) => { e.preventDefault(); executeSearch(symbol); }} className="relative mb-6">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-500" size={20} />
          <input
            type="text" value={symbol} onChange={(e) => setSymbol(e.target.value)}
            placeholder="Search symbol (e.g. AAPL)"
            className="w-full pl-10 pr-4 py-2 bg-gray-800 border border-gray-700 rounded-lg focus:ring-2 focus:ring-indigo-500 outline-none transition"
          />
        </form>
        <h2 className="text-sm font-semibold text-gray-400 mb-3 px-2">Watchlist</h2>
        <div className="flex flex-col gap-2">
          {WATCHLIST_SYMBOLS.map(s => (
            <button key={s} onClick={() => executeSearch(s)} className={`flex items-center gap-3 px-3 py-2 text-sm font-medium rounded-lg transition ${activeSymbol === s ? 'bg-indigo-600/20 text-indigo-300' : 'hover:bg-gray-800'}`}>
              <StockLogo symbol={s} className="w-6 h-6 rounded-full bg-gray-700" />
              <span className="flex-grow text-left">{s}</span>
            </button>
          ))}
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 p-8 overflow-y-auto">
        <AnimatePresence>
          {loading && (
            <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} className="absolute inset-0 bg-gray-900/50 flex items-center justify-center z-20">
              <Loader className="animate-spin text-indigo-500" size={48} />
            </motion.div>
          )}
        </AnimatePresence>
        
        {error && !loading && (
          <motion.div initial={{ opacity: 0, y: -10 }} animate={{ opacity: 1, y: 0 }} className="bg-red-900/30 border border-red-700 text-red-300 px-4 py-3 rounded-lg mb-6 flex items-center gap-3">
            <AlertCircle size={20} /> <span>{error}</span>
          </motion.div>
        )}

        {stockData ? (
          <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="space-y-8">
            <header className="flex items-center gap-4">
              <StockLogo symbol={stockData.symbol} className="w-12 h-12 rounded-full bg-gray-700" />
              <div>
                <h1 className="text-4xl font-bold">{stockData.symbol}</h1>
                <p className="text-gray-400">{stockData.company_name}</p>
              </div>
            </header>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
              <InfoCard title="Last Close Price" value={latestPrice ? `$${Number(latestPrice).toFixed(2)}` : '--'} icon={DollarSign} change={priceChange} changePercent={priceChangePercent} />
              <InfoCard title="Day's High" value={lastDaily?.high ? `$${Number(lastDaily.high).toFixed(2)}` : '--'} icon={ArrowUp} color="green" />
              <InfoCard title="Day's Low" value={lastDaily?.low ? `$${Number(lastDaily.low).toFixed(2)}` : '--'} icon={ArrowDown} color="red" />
              <InfoCard title="Volume" value={lastDaily?.volume ? (lastDaily.volume / 1e6).toFixed(2) + 'M' : '--'} icon={Briefcase} color="blue" />
            </div>

            <div className="bg-gray-950/50 border border-gray-800 rounded-xl p-6 h-[400px]">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={chartData} margin={{ top: 5, right: 20, left: -10, bottom: 5 }}>
                  <defs>
                    <linearGradient id="colorPrice" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#818CF8" stopOpacity={0.4}/>
                      <stop offset="95%" stopColor="#818CF8" stopOpacity={0}/>
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
                  <XAxis dataKey="date" stroke="#9CA3AF" fontSize={12} tickFormatter={(d) => new Date(d).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })} />
                  <YAxis stroke="#9CA3AF" fontSize={12} domain={['dataMin - (dataMax-dataMin)*0.1', 'dataMax + (dataMax-dataMin)*0.1']} tickFormatter={(v) => `$${v.toFixed(0)}`} />
                  <Tooltip content={<CustomTooltip />} />
                  <Area type="monotone" dataKey="price" stroke="#818CF8" fill="url(#colorPrice)" strokeWidth={2} />
                  {prediction && <Area type="monotone" dataKey="prediction" stroke="#34D399" strokeDasharray="5 5" fill="none" />}
                </AreaChart>
              </ResponsiveContainer>
            </div>

            {!prediction ? (
              <motion.div whileHover={{ scale: 1.01 }} whileTap={{ scale: 0.99 }}>
                <button onClick={handlePredict} disabled={loading} className="w-full bg-indigo-600 text-white py-3 rounded-lg font-semibold hover:bg-indigo-700 transition disabled:opacity-50 flex items-center justify-center gap-2">
                  <BrainCircuit size={20} /> Predict Next Day's Close
                </button>
              </motion.div>
            ) : (
              <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} className="bg-gray-950/50 border border-gray-800 rounded-xl p-6 flex items-center justify-between">
                <div className="flex items-center gap-4">
                  <Activity className="text-green-400" size={28} />
                  <div>
                    <h3 className="font-semibold text-lg">Predicted Next Close: ${Number(prediction.predicted_next_day_close).toFixed(2)}</h3>
                    <p className="text-sm text-gray-400">Based on the latest available daily data.</p>
                  </div>
                </div>
                <button onClick={() => setPrediction(null)} className="bg-gray-700 hover:bg-gray-600 text-sm font-semibold px-4 py-2 rounded-lg transition">Clear</button>
              </motion.div>
            )}

          </motion.div>
        ) : (
          !loading && (
            <div className="flex flex-col items-center justify-center h-full text-center text-gray-500">
              <BarChart3 size={48} className="mb-4" />
              <h2 className="text-xl font-semibold">No Data Available</h2>
              <p>Could not load data. Please try another symbol or run the data pipeline.</p>
            </div>
          )
        )}
      </main>
    </div>
  );
};

const InfoCard = ({ title, value, icon: Icon, color, change, changePercent }) => (
  <div className="bg-gray-950/50 border border-gray-800 rounded-xl p-5">
    <div className="flex items-center justify-between mb-2">
      <span className="text-gray-400 text-sm font-medium">{title}</span>
      <Icon className={`text-${color}-400`} size={20} />
    </div>
    <div className="text-3xl font-bold">{value}</div>
    {change != null && (
      <div className={`text-sm mt-1 font-semibold flex items-center gap-1 ${change >= 0 ? 'text-green-400' : 'text-red-400'}`}>
        {change >= 0 ? <ArrowUp size={14} /> : <ArrowDown size={14} />}
        {Math.abs(change).toFixed(2)} ({changePercent.toFixed(2)}%)
      </div>
    )}
  </div>
);

const CustomTooltip = ({ active, payload, label }) => {
  if (active && payload && payload.length) {
    const data = payload[0].payload;
    return (
      <div className="bg-gray-950/80 backdrop-blur-sm border border-gray-700 rounded-lg p-3 text-sm">
        <p className="label text-gray-400">{new Date(label).toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' })}</p>
        {data.price != null && <p className="intro text-indigo-300">{`Price: $${data.price.toFixed(2)}`}</p>}
        {data.prediction != null && <p className="intro text-green-400">{`Predicted: $${data.prediction.toFixed(2)}`}</p>}
      </div>
    );
  }
  return null;
};

export default Dashboard;
