import os
import sys
import psycopg
import yfinance as yf
import pandas as pd
from fastapi import FastAPI, HTTPException, Depends, Response, Query
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import joblib
from datetime import datetime, timezone
from psycopg_pool import ConnectionPool
import math
from fastapi_utils.tasks import repeat_every

load_dotenv()

# --------------------------------------------------------------------
# 🧠 Database Pool
# --------------------------------------------------------------------
pool = None

def _normalize_dsn(dsn: str) -> str:
    if "supabase.co" in dsn and "sslmode=" not in dsn:
        return f"{dsn}{'?' if '?' not in dsn else '&'}sslmode=require"
    return dsn

def get_pool():
    global pool
    if pool is None:
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            raise RuntimeError("DATABASE_URL missing.")
        dsn = _normalize_dsn(dsn)
        pool = ConnectionPool(conninfo=dsn, min_size=1, max_size=10, kwargs={'prepare_threshold': None})
    return pool

def get_db_connection():
    db_pool = get_pool()
    with db_pool.connection() as conn:
        yield conn

# --------------------------------------------------------------------
# 🌍 FastAPI Setup
# --------------------------------------------------------------------
app = FastAPI()
FRONTEND_ORIGINS = [
    "http://localhost:5173",
    "https://stock-predictor-five-opal.vercel.app",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=FRONTEND_ORIGINS,
    allow_origin_regex=r"https://.*\.vercel\.app$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --------------------------------------------------------------------
# 📦 Simple In-Memory Cache for Live Data
# --------------------------------------------------------------------
live_cache = {}  # {symbol: {"data": ..., "timestamp": datetime}}

def get_cached(symbol, max_age_seconds=300):
    cached = live_cache.get(symbol)
    if cached and (datetime.now() - cached["timestamp"]).total_seconds() < max_age_seconds:
        return cached["data"]
    return None

def set_cached(symbol, data):
    live_cache[symbol] = {"data": data, "timestamp": datetime.now()}

# --------------------------------------------------------------------
# 🧠 Model Loading
# --------------------------------------------------------------------
MODEL_PATH = os.path.join(os.path.dirname(__file__), 'ml_model', 'stock_predictor.joblib')
model = None
try:
    model = joblib.load(MODEL_PATH)
    print("✓ ML model loaded")
except Exception as e:
    print(f"⚠️ Could not load model: {e}")

# --------------------------------------------------------------------
# 🧩 Helper functions
# --------------------------------------------------------------------
def query_stock_data(search_term, conn: psycopg.Connection):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, symbol, company_name
            FROM stocks WHERE symbol ILIKE %s OR company_name ILIKE %s
        """, (f"{search_term.upper()}", f"%{search_term}%"))
        stock = cur.fetchone()
        if not stock: return None
        stock_id, symbol, name = stock
        cur.execute("""
            SELECT date, open, high, low, close, volume
            FROM stock_prices WHERE stock_id=%s ORDER BY date DESC LIMIT 365
        """, (stock_id,))
        rows = cur.fetchall()
        return {"symbol": symbol, "company_name": name,
                "prices": [{"date": r[0].isoformat(),"open": float(r[1]),"high": float(r[2]),"low": float(r[3]),"close": float(r[4]),"volume": int(r[5])} for r in rows]}

def get_live_info(symbol: str):
    cached = get_cached(symbol)
    if cached: return cached
    try:
        t = yf.Ticker(symbol)
        m1 = t.history(period="1d", interval="1m", prepost=True)
        if not m1.empty:
            last = m1.iloc[-1]
            data = {
                "currentPrice": float(last["Close"]),
                "dayHigh": float(m1["High"].max()),
                "dayLow": float(m1["Low"].min()),
                "marketCap": getattr(t.fast_info, "market_cap", None),
            }
            set_cached(symbol, data)
            return data
    except Exception:
        pass
    return None

# --------------------------------------------------------------------
# 🧠 Background Daily Refresh (auto runs every 24h)
# --------------------------------------------------------------------
@app.on_event("startup")
@repeat_every(seconds=86400)  # every 24h
def background_refresh():
    print("🔄 Daily background refresh started...")
    os.system("python fetch.py")  # Runs your fetch.py daily
    print("✅ Background refresh complete.")

# --------------------------------------------------------------------
# 🌐 ROUTES
# --------------------------------------------------------------------
@app.get("/")
def root(): return {"message": "Stock Prediction API Running"}

@app.get("/api/stocks/{term}")
def get_stock(term: str, refresh: int = Query(0), conn: psycopg.Connection = Depends(get_db_connection)):
    data = query_stock_data(term, conn)
    if not data:
        raise HTTPException(status_code=404, detail="Stock not found")
    if refresh:
    # Just update this symbol’s latest price instead of entire fetch.py
     new_live = get_live_info(term)
    if new_live:
        data["live_info"] = new_live
    else:
      live = get_live_info(term)
    if live:
        data["live_info"] = live
    return data

    new_data = query_stock_data(term, conn)
    if new_data:
        new_data["live_info"] = get_live_info(term)
    return new_data

@app.post("/api/predict")
def predict(req: dict, conn: psycopg.Connection = Depends(get_db_connection)):
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    data = query_stock_data(req["symbol"], conn)
    if not data or not data["prices"]:
        raise HTTPException(status_code=404, detail="No data available")
    latest = data["prices"][0]
    df = pd.DataFrame([latest])
    prediction = model.predict(df[["open","high","low","close","volume"]])[0]
    live = get_live_info(req["symbol"])
    return {
      "symbol": req["symbol"],
      "predicted_next_day_close": float(prediction),
      "live_info": live,
    }


@app.get("/api/live/{symbol}")
def live(symbol: str):
    info = get_live_info(symbol.upper())
    if not info:
        raise HTTPException(status_code=404, detail="No live data")
    return {"symbol": symbol.upper(), "live_info": info}

@app.get("/health/db")
def health(conn: psycopg.Connection = Depends(get_db_connection)):
    with conn.cursor() as c: c.execute("SELECT 1")
    return {"ok": True}
