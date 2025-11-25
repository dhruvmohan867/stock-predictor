import { useState, useEffect, useMemo } from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { TrendingUp, DollarSign, Search, BarChart3, AlertCircle, Loader, Building, ArrowUp, ArrowDown, Briefcase, Activity, BrainCircuit, Sun, Moon, BookOpen } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

const API_BASE = import.meta.env.VITE_API_BASE;

const WATCHLIST_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "RELIANCE.NS", "TCS.NS", "TSLA", "^NSEI", "^GSPC"];

// --- NEW: Professional Theme Hook using View Transitions API ---
const useTheme = () => {
  const [theme, setTheme] = useState(() => localStorage.getItem('theme') || 'dark');

  useEffect(() => {
    document.documentElement.classList.remove('light', 'dark');
    document.documentElement.classList.add(theme);
    localStorage.setItem('theme', theme);
  }, [theme]);

  const toggleTheme = () => {
    const newTheme = theme === 'dark' ? 'light' : 'dark';

    // Use the browser's new View Transitions API if available
    if (document.startViewTransition) {
      document.startViewTransition(() => {
        setTheme(newTheme);
      });
    } else {
      // Fallback for browsers that don't support it
      setTheme(newTheme);
    }
  };

  return [theme, toggleTheme];
};


const StockLogo = ({ symbol, className }) => {
  const [logoUrl, setLogoUrl] = useState(null);
  const [error, setError] = useState(false);

  useEffect(() => {
    setError(false);
    if (!symbol || symbol.startsWith('^')) {
      setLogoUrl(null);
      return;
    }
    const sanitizedSymbol = symbol.split('.')[0];
    setLogoUrl(`https://api.twelvedata.com/logo/${sanitizedSymbol}.png`);
  }, [symbol]);

  if (error || !logoUrl) {
    return (
      <div className={`flex items-center justify-center bg-slate-200 dark:bg-gray-700 rounded-full ${className}`}>
        <Building className="text-slate-500 dark:text-gray-400" size="60%" />
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
  const [theme, toggleTheme] = useTheme(); // <-- MODIFIED: Simpler return value
  const [showKnowledge, setShowKnowledge] = useState(false);

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
    <div className="w-full min-h-screen bg-slate-100 dark:bg-gray-900 text-slate-800 dark:text-gray-200 font-sans flex transition-colors duration-300">
      {/* REMOVED: The transition overlay is no longer needed */}

      {/* Sidebar */}
      <aside className="w-72 bg-white dark:bg-gray-950/50 border-r border-slate-200 dark:border-gray-800 p-6 flex flex-col shrink-0">
        <div className="flex items-center gap-3 mb-8">
          <div className="w-10 h-10 bg-indigo-600 rounded-lg flex items-center justify-center shadow-lg shadow-indigo-600/30">
            <TrendingUp className="text-white" size={24} />
          </div>
          <h1 className="text-xl font-bold">AlphaPredict</h1>
        </div>
        <form onSubmit={(e) => { e.preventDefault(); executeSearch(symbol); }} className="relative mb-6">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 dark:text-gray-500" size={20} />
          <input
            type="text" value={symbol} onChange={(e) => setSymbol(e.target.value)}
            placeholder="Search symbol (e.g. AAPL)"
            className="w-full pl-10 pr-4 py-2 bg-slate-100 dark:bg-gray-800 border border-slate-300 dark:border-gray-700 rounded-lg focus:ring-2 focus:ring-indigo-500 outline-none transition"
          />
        </form>
        <h2 className="text-sm font-semibold text-slate-500 dark:text-gray-400 mb-3 px-2">Watchlist</h2>
        <div className="flex flex-col gap-2">
          {WATCHLIST_SYMBOLS.map(s => (
            <button key={s} onClick={() => executeSearch(s)} className={`flex items-center gap-3 px-3 py-2 text-sm font-medium rounded-lg transition ${activeSymbol === s ? 'bg-indigo-600/10 text-indigo-600 dark:bg-indigo-600/20 dark:text-indigo-300' : 'hover:bg-slate-100 dark:hover:bg-gray-800'}`}>
              <StockLogo symbol={s} className="w-6 h-6 rounded-full" />
              <span className="flex-grow text-left">{s}</span>
            </button>
          ))}
        </div>
        <div className="mt-auto space-y-2">
          <button onClick={() => setShowKnowledge(!showKnowledge)} className="w-full flex items-center justify-center gap-2 px-3 py-2 text-sm font-medium rounded-lg transition hover:bg-slate-100 dark:hover:bg-gray-800">
            <BookOpen size={16} /> Knowledge Base
          </button>
          <button onClick={toggleTheme} className="w-full flex items-center justify-center gap-2 px-3 py-2 text-sm font-medium rounded-lg transition hover:bg-slate-100 dark:hover:bg-gray-800">
            {theme === 'dark' ? <Sun size={16} /> : <Moon size={16} />}
            <span>{theme === 'dark' ? 'Light' : 'Dark'} Mode</span>
          </button>
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 p-8 overflow-y-auto relative">
        <AnimatePresence>
          {loading && (
            <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} className="absolute inset-0 bg-slate-100/50 dark:bg-gray-900/50 flex items-center justify-center z-20 backdrop-blur-sm">
              <Loader className="animate-spin text-indigo-500" size={48} />
            </motion.div>
          )}
        </AnimatePresence>
        
        {showKnowledge ? (
          <KnowledgeBase onClose={() => setShowKnowledge(false)} />
        ) : (
          <>
            {error && !loading && (
              <motion.div initial={{ opacity: 0, y: -10 }} animate={{ opacity: 1, y: 0 }} className="bg-red-100 dark:bg-red-900/30 border border-red-300 dark:border-red-700 text-red-700 dark:text-red-300 px-4 py-3 rounded-lg mb-6 flex items-center gap-3">
                <AlertCircle size={20} /> <span>{error}</span>
              </motion.div>
            )}

            {stockData ? (
              <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="space-y-8">
                <header className="flex items-center justify-between">
                  <div className="flex items-center gap-4">
                    <StockLogo symbol={stockData.symbol} className="w-12 h-12 rounded-full" />
                    <div>
                      <h1 className="text-4xl font-bold">{stockData.symbol}</h1>
                      <p className="text-slate-500 dark:text-gray-400">{stockData.company_name}</p>
                    </div>
                  </div>
                  {lastDaily?.date && (
                    <div className="text-right">
                      <span className="text-xs text-slate-500 dark:text-gray-500 font-medium">Last Updated</span>
                      <p className="font-semibold text-slate-700 dark:text-gray-300">
                        {new Date(lastDaily.date).toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' })}
                      </p>
                    </div>
                  )}
                </header>

                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                  <InfoCard title="Last Close Price" value={latestPrice ? `$${Number(latestPrice).toFixed(2)}` : '--'} icon={DollarSign} change={priceChange} changePercent={priceChangePercent} />
                  <InfoCard title="Day's High" value={lastDaily?.high ? `$${Number(lastDaily.high).toFixed(2)}` : '--'} icon={ArrowUp} color="green" />
                  <InfoCard title="Day's Low" value={lastDaily?.low ? `$${Number(lastDaily.low).toFixed(2)}` : '--'} icon={ArrowDown} color="red" />
                  <InfoCard title="Volume" value={lastDaily?.volume ? (lastDaily.volume / 1e6).toFixed(2) + 'M' : '--'} icon={Briefcase} color="blue" />
                </div>

                <div className="bg-white dark:bg-gray-950/50 border border-slate-200 dark:border-gray-800 rounded-xl p-6 h-[400px]">
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={chartData} margin={{ top: 5, right: 20, left: -10, bottom: 5 }}>
                      <defs>
                        <linearGradient id="colorPrice" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor="#818CF8" stopOpacity={0.4}/>
                          <stop offset="95%" stopColor="#818CF8" stopOpacity={0}/>
                        </linearGradient>
                      </defs>
                      <CartesianGrid strokeDasharray="3 3" stroke="rgba(128,128,128,0.1)" />
                      <XAxis dataKey="date" stroke="#64748B" fontSize={12} tickFormatter={(d) => new Date(d).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })} />
                      <YAxis stroke="#64748B" fontSize={12} domain={['dataMin - (dataMax-dataMin)*0.1', 'dataMax + (dataMax-dataMin)*0.1']} tickFormatter={(v) => `$${v.toFixed(0)}`} />
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
                  <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} className="bg-white dark:bg-gray-950/50 border border-slate-200 dark:border-gray-800 rounded-xl p-6 flex items-center justify-between">
                    <div className="flex items-center gap-4">
                      <Activity className="text-green-500 dark:text-green-400" size={28} />
                      <div>
                        <h3 className="font-semibold text-lg">Predicted Next Close: ${Number(prediction.predicted_next_day_close).toFixed(2)}</h3>
                        <p className="text-sm text-slate-500 dark:text-gray-400">Based on the latest available daily data.</p>
                      </div>
                    </div>
                    <button onClick={() => setPrediction(null)} className="bg-slate-200 dark:bg-gray-700 hover:bg-slate-300 dark:hover:bg-gray-600 text-sm font-semibold px-4 py-2 rounded-lg transition">Clear</button>
                  </motion.div>
                )}

              </motion.div>
            ) : (
              !loading && (
                <div className="flex flex-col items-center justify-center h-full text-center text-slate-500 dark:text-gray-500">
                  <BarChart3 size={48} className="mb-4" />
                  <h2 className="text-xl font-semibold">No Data Available</h2>
                  <p>Could not load data. Please try another symbol or run the data pipeline.</p>
                </div>
              )
            )}
          </>
        )}
      </main>
    </div>
  );
};

const InfoCard = ({ title, value, icon: Icon, color, change, changePercent }) => (
  <div className="bg-white dark:bg-gray-950/50 border border-slate-200 dark:border-gray-800 rounded-xl p-5">
    <div className="flex items-center justify-between mb-2">
      <span className="text-slate-500 dark:text-gray-400 text-sm font-medium">{title}</span>
      <Icon className={`text-${color}-500 dark:text-${color}-400`} size={20} />
    </div>
    <div className="text-3xl font-bold">{value}</div>
    {change != null && (
      <div className={`text-sm mt-1 font-semibold flex items-center gap-1 ${change >= 0 ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'}`}>
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
      <div className="bg-white/80 dark:bg-gray-950/80 backdrop-blur-sm border border-slate-300 dark:border-gray-700 rounded-lg p-3 text-sm shadow-lg">
        <p className="label text-slate-500 dark:text-gray-400">{new Date(label).toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' })}</p>
        {data.price != null && <p className="intro text-indigo-600 dark:text-indigo-300 font-semibold">{`Price: $${data.price.toFixed(2)}`}</p>}
        {data.prediction != null && <p className="intro text-green-600 dark:text-green-400 font-semibold">{`Predicted: $${data.prediction.toFixed(2)}`}</p>}
      </div>
    );
  }
  return null;
};

const KnowledgeBase = ({ onClose }) => (
  <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="prose prose-slate dark:prose-invert max-w-none">
    <button onClick={onClose} className="mb-4 font-semibold text-indigo-600 dark:text-indigo-400">&larr; Back to Dashboard</button>
    <h1>Stock Market Knowledge Base</h1>
    
    <h2>About the S&P 500 (^GSPC)</h2>
    <p>The S&P 500 is a stock market index that represents the performance of 500 of the largest publicly-traded companies in the United States. It is one of the most commonly followed equity indexes and is considered a primary benchmark for the overall health of the U.S. stock market. The companies are selected by the Standard & Poor's Index Committee based on criteria such as market size, liquidity, and sector representation.</p>
    
    <h2>About the NIFTY 50 (^NSEI)</h2>
    <p>The NIFTY 50 is the flagship benchmark index for the Indian stock market. It represents the weighted average of 50 of the largest and most liquid Indian companies listed on the National Stock Exchange (NSE). The NIFTY 50 is used for a variety of purposes, such as benchmarking fund portfolios, index-based derivatives, and general market sentiment analysis for India.</p>
    
    <h2>How This App Works</h2>
    <p>This application follows a daily cycle to provide predictions:</p>
    <ol>
      <li><strong>Data Fetching:</strong> Twice a day, a script runs to fetch the latest end-of-day (EOD) stock prices from Yahoo Finance for over 1000 companies across the US (S&P 500) and India (NIFTY 500).</li>
      <li><strong>Database Storage:</strong> This data is stored in a robust PostgreSQL database, creating a comprehensive historical record.</li>
      <li><strong>Prediction:</strong> When you click "Predict," the app sends the most recent day's data (Open, High, Low, Close, Volume) to a machine learning model. The model, trained on all historical data, analyzes these inputs to predict the closing price for the next trading day.</li>
    </ol>
    <p>This daily, EOD approach is a standard and reliable method for financial analysis and modeling.</p>
  </motion.div>
);

export default Dashboard;

