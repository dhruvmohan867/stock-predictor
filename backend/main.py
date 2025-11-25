import os
import sys
import psycopg
import yfinance as yf
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, HTTPException, Depends, Response, Query, BackgroundTasks
from fastapi.responses import StreamingResponse # <-- ADD THIS
import asyncio 
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import joblib
from psycopg_pool import ConnectionPool
import pandas as pd
import numpy as np # <-- Make sure numpy is imported
import math
import time
import threading
import requests
import random

load_dotenv()

# --------------------------------------------------------------------
# 🧠 Database Pool
# --------------------------------------------------------------------
pool = None

def _normalize_dsn(dsn: str) -> str:
    if ("supabase.co" in dsn or "supabase.com" in dsn) and "sslmode=" not in dsn:
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
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --------------------------------------------------------------------
# 🕒 Yahoo Finance - Multiple Sessions with Rotation
# --------------------------------------------------------------------
RATE_LIMIT_SEC = float(os.getenv("YF_RATE_LIMIT_SEC", "1.0"))  # Increased to 1 second
_YF_LOCK = threading.Lock()
_last_call_ts = 0.0

# ✅ NEW: Create multiple sessions to rotate through
_YF_SESSIONS = []
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
]

def _get_session():
    """Get or create a random session from the pool."""
    global _YF_SESSIONS
    if not _YF_SESSIONS:
        for ua in USER_AGENTS:
            s = requests.Session()
            s.headers.update({"User-Agent": ua})
            _YF_SESSIONS.append(s)
    return random.choice(_YF_SESSIONS)

def _rate_limit_wait():
    """Ensure at least RATE_LIMIT_SEC spacing between Yahoo calls."""
    global _last_call_ts
    with _YF_LOCK:
        now = time.time()
        delay = _last_call_ts + RATE_LIMIT_SEC - now
        if delay > 0:
            time.sleep(delay)
        _last_call_ts = time.time()

def _with_backoff(fn, retries=4, base=1.0):
    """Run fn with exponential backoff on exception."""
    for i in range(retries):
        try:
            _rate_limit_wait()
            return fn()
        except Exception as e:
            if i == retries - 1:
                print(f"⚠️ Final retry failed: {e}")
                return None
            wait = base * (2 ** i)
            print(f"🔄 Retry {i + 1}/{retries} after {wait}s: {e}")
            time.sleep(wait)

def _yf_ticker(sym: str):
    """Create a yfinance Ticker; let yfinance manage its own session (curl_cffi)."""
    return yf.Ticker(sym)

# --------------------------------------------------------------------
# 📦 Simple In-Memory Cache for Live Data
# --------------------------------------------------------------------
LIVE_TTL_SEC = int(os.getenv("LIVE_TTL_SEC", "60"))
_LIVE_CACHE = {}
_CACHE_LOCK = threading.Lock()

def _get_cached(symbol: str):
    now = time.time()
    with _CACHE_LOCK:
        entry = _LIVE_CACHE.get(symbol)
        if not entry:
            return None
        ts, data = entry
        if now - ts <= LIVE_TTL_SEC:
            return data
        _LIVE_CACHE.pop(symbol, None)
        return None

def _set_cached(symbol: str, data: dict):
    with _CACHE_LOCK:
        _LIVE_CACHE[symbol] = (time.time(), data)

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
        """, (f"%{search_term}%", f"%{search_term}%"))
        stock = cur.fetchone()
        if not stock: return None
        stock_id, symbol, name = stock
        
        # --- MODIFICATION: Filter out future dates ---
        # This query now explicitly ignores any data with a date after today,
        # preventing bad data (like the 2025 date) from ever reaching the frontend.
        today = datetime.now(timezone.utc).date()
        cur.execute("""
            SELECT date, open, high, low, close, volume
            FROM stock_prices 
            WHERE stock_id=%s AND date <= %s
            ORDER BY date DESC 
            LIMIT 365
        """, (stock_id, today))
        rows = cur.fetchall()
        
        if not rows:
            # Fallback if only future data exists for some reason
            return {"symbol": symbol, "company_name": name, "prices": []}

        return {"symbol": symbol, "company_name": name,
                "prices": [{"date": r[0].isoformat(),"open": float(r[1]),"high": float(r[2]),"low": float(r[3]),"close": float(r[4]),"volume": int(r[5])} for r in rows]}

def get_live_info(symbol: str, conn: psycopg.Connection):
    """
    MODIFIED: This function no longer calls live APIs.
    It fetches the most recent record from the database.
    """
    sym = symbol.upper()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT sp.close, sp.high, sp.low, s.company_name
            FROM stock_prices sp
            JOIN stocks s ON s.id = sp.stock_id
            WHERE s.symbol = %s
            ORDER BY sp.date DESC
            LIMIT 1
        """, (sym,))
        latest_record = cur.fetchone()

    if not latest_record:
        return None

    close, high, low, name = latest_record
    return {
        "currentPrice": float(close) if close else None,
        "dayHigh": float(high) if high else None,
        "dayLow": float(low) if low else None,
        "marketCap": None,  # Market cap is not in our daily data
        "previousClose": None, # This would require looking at the second-to-last record
        "source": "database" # Clearly indicate the data source
}

# --------------------------------------------------------------------
# 🌐 Public API Endpoints
# --------------------------------------------------------------------
@app.get("/", include_in_schema=False)
def root():
    return {"ok": True, "service": "stock-predictor"}

@app.get("/api/stocks/{term}")
def get_stock(term: str, conn: psycopg.Connection = Depends(get_db_connection)):
    data = query_stock_data(term, conn)
    if not data:
        raise HTTPException(status_code=404, detail="Stock not found")

    # Use the new database-driven get_live_info
    live = get_live_info(data["symbol"], conn)
    if live:
        data["live_info"] = live
    return data

@app.post("/api/predict")
def predict(req: dict, conn: psycopg.Connection = Depends(get_db_connection)):
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    data = query_stock_data(req["symbol"], conn)
    if not data or not data["prices"]:
        raise HTTPException(status_code=404, detail="No data available")
    
    # --- FIX START: Use numpy for prediction, not pandas ---
    latest = data["prices"][0]
    # The model was trained on features in this specific order
    feature_order = ['open', 'high', 'low', 'close', 'volume']
    # Create a numpy array from the latest data in the correct order
    features = np.array([[latest[key] for key in feature_order]])
    
    prediction = model.predict(features)[0]
    # --- FIX END ---

    # Use the new database-driven get_live_info
    live = get_live_info(req["symbol"], conn)
    return {
      "symbol": req["symbol"],
      "predicted_next_day_close": float(prediction),
      "live_info": live,
    }

@app.get("/api/live/{symbol}")
def live(symbol: str, conn: psycopg.Connection = Depends(get_db_connection)):
    # This endpoint now uses the reliable, database-backed function
    info = get_live_info(symbol.upper(), conn)
    if info:
        return {"symbol": symbol.upper(), "live_info": info}
    
    raise HTTPException(status_code=404, detail="No data for this symbol in the database.")

@app.get("/api/symbols")
def list_symbols(conn: psycopg.Connection = Depends(get_db_connection)):
    with conn.cursor() as cur:
        cur.execute("SELECT symbol FROM stocks ORDER BY symbol")
        return [r[0] for r in cur.fetchall()]

@app.get("/health/db")
def health(conn: psycopg.Connection = Depends(get_db_connection)):
    with conn.cursor() as c:
        c.execute("SELECT 1")
    return {"ok": True}