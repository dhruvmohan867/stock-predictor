import os
import sys
import psycopg
import yfinance as yf
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, HTTPException, Depends, Response, Query
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import joblib
from psycopg_pool import ConnectionPool
import pandas as pd
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
    allow_origin_regex=r"https://.*\.vercel\.app$",
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
    """Create a yfinance Ticker with a random session."""
    return yf.Ticker(sym, session=_get_session())

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
        cur.execute("""
            SELECT date, open, high, low, close, volume
            FROM stock_prices WHERE stock_id=%s ORDER BY date DESC LIMIT 365
        """, (stock_id,))
        rows = cur.fetchall()
        return {"symbol": symbol, "company_name": name,
                "prices": [{"date": r[0].isoformat(),"open": float(r[1]),"high": float(r[2]),"low": float(r[3]),"close": float(r[4]),"volume": int(r[5])} for r in rows]}

def get_live_info(symbol: str):
    """Intraday-first live metrics with safe fallbacks + short TTL cache."""
    sym = symbol.upper()
    cached = _get_cached(sym)
    if cached:
        return cached

    def clean(v):
        try:
            if v is None:
                return None
            f = float(v)
            return None if math.isnan(f) else f
        except Exception:
            return None

    current = day_high = day_low = market_cap = prev_close = None
    shares_out = None
    try:
        t = _yf_ticker(sym)

        # 1) Intraday 1m (best live read)
        try:
            m1 = _with_backoff(lambda: t.history(period="1d", interval="1m", auto_adjust=False, prepost=True))
            if m1 is not None and not m1.empty:
                last = m1.iloc[-1]
                current = clean(last.get("Close"))
                day_high = clean(m1["High"].max())
                day_low = clean(m1["Low"].min())
        except Exception:
            pass

        # 2) fast_info (no quoteSummary)
        try:
            fi = getattr(t, "fast_info", None)
            get = (fi.get if isinstance(fi, dict) else lambda k, d=None: getattr(fi, k, d)) if fi else None
            if get:
                market_cap = market_cap or clean(get("market_cap"))
                prev_close = prev_close or clean(get("previous_close"))
                current = current or clean(get("last_price"))
                day_high = day_high or clean(get("day_high"))
                day_low = day_low or clean(get("day_low"))
                shares_out = get("shares_outstanding")
        except Exception:
            pass

        # 3) Daily history fallback
        if any(v is None for v in (current, day_high, day_low, prev_close)):
            try:
                d1 = _with_backoff(lambda: t.history(period="5d", interval="1d", auto_adjust=False, prepost=True))
                if d1 is not None and not d1.empty:
                    last = d1.iloc[-1]
                    prev = d1.iloc[-2] if len(d1) > 1 else None
                    current = current or clean(last.get("Close"))
                    day_high = day_high or clean(last.get("High"))
                    day_low = day_low or clean(last.get("Low"))
                    prev_close = prev_close or clean((prev or last).get("Close"))
            except Exception:
                pass

        # 4) Compute market cap if still missing
        if market_cap is None and shares_out and current is not None:
            try:
                market_cap = float(shares_out) * float(current)
            except Exception:
                pass

        data = {
            "currentPrice": current,
            "dayHigh": day_high,
            "dayLow": day_low,
            "marketCap": market_cap,
            "previousClose": prev_close,
        }
        _set_cached(sym, data)
        return data
    except Exception as e:
        print(f"❌ get_live_info failed for {sym}: {e}")
        return None

# --------------------------------------------------------------------
# ✅ UPDATED: Incremental refresh helpers with multi-fallback
# --------------------------------------------------------------------
def _get_stock_id(symbol: str, conn: psycopg.Connection) -> int | None:
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM stocks WHERE symbol=%s", (symbol,))
        row = cur.fetchone()
        return row[0] if row else None

def _get_latest_date(symbol: str, conn: psycopg.Connection):
    stock_id = _get_stock_id(symbol, conn)
    if stock_id is None:
        return None
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(date) FROM stock_prices WHERE stock_id=%s", (stock_id,))
        r = cur.fetchone()
        return r[0]

def _fetch_history(symbol: str, start_date=None):
    """Fetch with multiple fallback strategies."""
    session = _get_session()
    t = yf.Ticker(symbol, session=session)
    
    # Strategy 1: Targeted date range (if start_date provided)
    if start_date:
        try:
            df = _with_backoff(lambda: t.history(start=start_date, end=datetime.now(timezone.utc), interval="1d", auto_adjust=False))
            if df is not None and not df.empty:
                print(f"✅ Fetched {len(df)} rows for {symbol} from {start_date}")
                return df
        except Exception as e:
            print(f"⚠️ Date range fetch failed for {symbol}: {e}")
    
    # Strategy 2: Recent 5 days (most reliable)
    try:
        df = _with_backoff(lambda: t.history(period="5d", interval="1d", auto_adjust=False))
        if df is not None and not df.empty:
            print(f"✅ Fetched {len(df)} rows for {symbol} (5d fallback)")
            return df
    except Exception as e:
        print(f"⚠️ 5d fetch failed for {symbol}: {e}")
    
    # Strategy 3: 1 month (if 5d fails)
    try:
        df = _with_backoff(lambda: t.history(period="1mo", interval="1d", auto_adjust=False))
        if df is not None and not df.empty:
            print(f"✅ Fetched {len(df)} rows for {symbol} (1mo fallback)")
            return df
    except Exception as e:
        print(f"⚠️ 1mo fetch failed for {symbol}: {e}")
    
    # Strategy 4: yf.download (different API endpoint)
    try:
        df = yf.download(symbol, period="5d", interval="1d", progress=False, session=session)
        if df is not None and not df.empty:
            print(f"✅ Fetched {len(df)} rows for {symbol} (download method)")
            return df
    except Exception as e:
        print(f"⚠️ Download method failed for {symbol}: {e}")
    
    print(f"❌ All fetch strategies failed for {symbol}")
    return None

def _store_history(symbol: str, company_name: str, df: pd.DataFrame, conn: psycopg.Connection):
    if df is None or df.empty:
        return
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO stocks (symbol, company_name)
            VALUES (%s, %s)
            ON CONFLICT (symbol) DO UPDATE SET company_name=EXCLUDED.company_name
            RETURNING id
        """, (symbol, company_name))
        stock_id = cur.fetchone()[0]
        rows = []
        for date, row in df.iterrows():
            rows.append((
                stock_id,
                date.date(),
                float(row["Open"]),
                float(row["High"]),
                float(row["Low"]),
                float(row["Close"]),
                int(row["Volume"]) if not pd.isna(row["Volume"]) else 0
            ))
        cur.executemany("""
            INSERT INTO stock_prices (stock_id, date, open, high, low, close, volume)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (stock_id, date) DO NOTHING
        """, rows)
    conn.commit()

def refresh_symbol(symbol: str, conn: psycopg.Connection, max_retries=2):
    """Refresh with retry logic and detailed logging."""
    symbol = symbol.upper()
    latest_before = _get_latest_date(symbol, conn)
    start = None
    today = datetime.now(timezone.utc).date()
    
    if latest_before:
        start = latest_before + timedelta(days=1)
        if start > today:
            return {"updated": False, "reason": "up_to_date", "latest": str(latest_before)}
    
    # Retry logic
    df = None
    for attempt in range(max_retries):
        df = _fetch_history(symbol, start)
        if df is not None and not df.empty:
            break
        if attempt < max_retries - 1:
            wait = (attempt + 1) * 3  # 3s, 6s
            print(f"🔄 Retry {attempt + 1}/{max_retries} for {symbol} in {wait}s...")
            time.sleep(wait)
    
    if df is None or df.empty:
        print(f"❌ All retries exhausted for {symbol}")
        return {
            "updated": False,
            "reason": "fetch_failed_after_retries",
            "latest": str(latest_before) if latest_before else None
        }
    
    _store_history(symbol, symbol, df, conn)
    latest_after = _get_latest_date(symbol, conn)
    updated = bool(latest_after and (latest_before is None or latest_after > latest_before))
    
    return {
        "updated": updated,
        "reason": "ok" if updated else "no_new_rows",
        "latest": str(latest_after),
        "rows_added": len(df)
    }

# --------------------------------------------------------------------
# 🔒 Secure Internal Endpoints
# --------------------------------------------------------------------
REFRESH_SECRET = os.getenv("REFRESH_SECRET", "change_me")

@app.get("/", include_in_schema=False)
def root():
    return {"ok": True, "service": "stock-predictor"}

@app.post("/internal/refresh")
def internal_refresh(payload: dict, secret: str = Query(None), conn: psycopg.Connection = Depends(get_db_connection)):
    if secret != REFRESH_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    symbols = payload.get("symbols") or []
    if not isinstance(symbols, list) or not symbols:
        raise HTTPException(status_code=400, detail="symbols list required")
    
    results = {}
    updated = []
    failed = []
    
    for s in symbols:
        try:
            res = refresh_symbol(s, conn)
            results[s.upper()] = res
            if res.get("updated"):
                updated.append(s.upper())
            else:
                failed.append(s.upper())
        except Exception as e:
            print(f"❌ Exception refreshing {s}: {e}")
            results[s.upper()] = {"updated": False, "reason": f"error:{str(e)[:100]}"}
            failed.append(s.upper())
    
    return {
        "updated": updated,
        "failed": failed,
        "success_count": len(updated),
        "fail_count": len(failed),
        "results": results
    }

@app.get("/internal/stale")
def stale_symbols(secret: str = Query(None), conn: psycopg.Connection = Depends(get_db_connection)):
    if secret != REFRESH_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    today = datetime.now().date()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT s.symbol
            FROM stocks s
            LEFT JOIN LATERAL (
              SELECT MAX(date) AS max_date
              FROM stock_prices sp
              WHERE sp.stock_id = s.id
            ) m ON TRUE
            WHERE COALESCE(m.max_date, '1970-01-01') < %s
            ORDER BY s.symbol
            LIMIT 500
        """, (today,))
        rows = cur.fetchall()
    return [r[0] for r in rows]

# ✅ NEW: Status endpoint to monitor refresh health
@app.get("/internal/refresh-status")
def refresh_status(secret: str = Query(None), conn: psycopg.Connection = Depends(get_db_connection)):
    if secret != REFRESH_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    
    with conn.cursor() as cur:
        # Count stocks with today's data
        cur.execute("""
            SELECT COUNT(DISTINCT s.symbol)
            FROM stocks s
            JOIN stock_prices sp ON sp.stock_id = s.id
            WHERE sp.date = %s
        """, (today,))
        fresh_count = cur.fetchone()[0]
        
        # Count stocks with yesterday's data
        cur.execute("""
            SELECT COUNT(DISTINCT s.symbol)
            FROM stocks s
            JOIN stock_prices sp ON sp.stock_id = s.id
            WHERE sp.date = %s
        """, (yesterday,))
        yesterday_count = cur.fetchone()[0]
        
        # Total stocks
        cur.execute("SELECT COUNT(*) FROM stocks")
        total = cur.fetchone()[0]
        
        # Most recent dates
        cur.execute("""
            SELECT s.symbol, MAX(sp.date) as latest
            FROM stocks s
            JOIN stock_prices sp ON sp.stock_id = s.id
            GROUP BY s.symbol
            ORDER BY latest DESC
            LIMIT 10
        """)
        recent = [{"symbol": r[0], "latest": str(r[1])} for r in cur.fetchall()]
    
    return {
        "date": str(today),
        "fresh_today": fresh_count,
        "fresh_yesterday": yesterday_count,
        "total_stocks": total,
        "freshness_percent": round((fresh_count / total * 100) if total > 0 else 0, 2),
        "recent_updates": recent
    }

# --------------------------------------------------------------------
# 🌐 Public API Endpoints
# --------------------------------------------------------------------
@app.get("/api/stocks/{term}")
def get_stock(term: str, refresh: int = Query(0), conn: psycopg.Connection = Depends(get_db_connection)):
    data = query_stock_data(term, conn)
    if not data:
        raise HTTPException(status_code=404, detail="Stock not found")

    if refresh:
        refresh_symbol(data["symbol"], conn)
        data = query_stock_data(term, conn)

    live = get_live_info(data["symbol"])
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
def live(symbol: str, conn: psycopg.Connection = Depends(get_db_connection)):
    info = get_live_info(symbol.upper())
    if info:
        return {"symbol": symbol.upper(), "live_info": info}
    
    # Fallback to latest stored daily candle
    with conn.cursor() as cur:
        cur.execute("""
          SELECT sp.close, sp.high, sp.low
          FROM stock_prices sp
          JOIN stocks s ON s.id = sp.stock_id
          WHERE s.symbol=%s
          ORDER BY sp.date DESC
          LIMIT 1
        """, (symbol.upper(),))
        r = cur.fetchone()
    if r:
        fallback = {
            "currentPrice": float(r[0]),
            "dayHigh": float(r[1]),
            "dayLow": float(r[2]),
            "marketCap": None,
            "previousClose": None,
            "source": "db-fallback"
        }
        return {"symbol": symbol.upper(), "live_info": fallback}
    raise HTTPException(status_code=404, detail="No live or fallback data")

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