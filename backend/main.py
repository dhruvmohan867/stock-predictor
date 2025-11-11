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
# 📦 Simple In-Memory Cache for Live Data (make TTL configurable)
# --------------------------------------------------------------------
LIVE_TTL_SEC = int(os.getenv("LIVE_TTL_SEC", "60"))  # was 30

# --- Yahoo rate limit + backoff helpers ---
_YF_LOCK = threading.Lock()
_last_call_ts = 0.0
RATE_LIMIT_SEC = float(os.getenv("YF_RATE_LIMIT_SEC", "0.5"))  # min spacing per process

def _rate_limit_wait():
    global _last_call_ts
    with _YF_LOCK:
        now = time.time()
        delay = _last_call_ts + RATE_LIMIT_SEC - now
        if delay > 0:
            time.sleep(delay)
        _last_call_ts = time.time()

def _with_backoff(fn, retries=3, base=0.75):
    for i in range(retries):
        try:
            _rate_limit_wait()
            return fn()
        except Exception:
            if i == retries - 1:
                return None
            time.sleep(base * (2 ** i))

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
    try:
        t = yf.Ticker(sym)

        # 1) Intraday 1m
        try:
            m1 = _with_backoff(lambda: t.history(period="1d", interval="1m", auto_adjust=False, prepost=True))
            if m1 is not None and not m1.empty:
                last = m1.iloc[-1]
                current = clean(last.get("Close"))
                day_high = clean(m1["High"].max())
                day_low = clean(m1["Low"].min())
        except Exception:
            pass

        # 2) fast_info
        try:
            fi = getattr(t, "fast_info", None)
            if fi:
                market_cap = market_cap or clean(getattr(fi, "market_cap", None))
                prev_close = prev_close or clean(getattr(fi, "previous_close", None))
                current = current or clean(getattr(fi, "last_price", None))
                day_high = day_high or clean(getattr(fi, "day_high", None))
                day_low = day_low or clean(getattr(fi, "day_low", None))
        except Exception:
            pass

        # 3) info (call only if still missing)
        if any(v is None for v in (current, day_high, day_low, prev_close, market_cap)):
            try:
                info = _with_backoff(lambda: t.info) or {}
                shares_out = info.get("sharesOutstanding") or info.get("floatShares")
                market_cap = market_cap or clean(info.get("marketCap"))
                prev_close = prev_close or clean(info.get("previousClose"))
                current = current or clean(info.get("regularMarketPrice") or info.get("currentPrice"))
                day_high = day_high or clean(info.get("dayHigh"))
                day_low = day_low or clean(info.get("dayLow"))
            except Exception:
                shares_out = None
        else:
            shares_out = None

        # 4) daily fallback
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

        # Compute market cap if still missing
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

# --- NEW: incremental helpers (adapted from data_pipeline/fetch_data.py) ---

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
    t = yf.Ticker(symbol)
    try:
        if start_date:
            df = _with_backoff(lambda: t.history(start=start_date, end=datetime.now(timezone.utc), interval="1d"))
        else:
            df = _with_backoff(lambda: t.history(period="2y", interval="1d"))
    except Exception:
        df = None
    if df is None or df.empty:
        try:
            df = _with_backoff(lambda: t.history(period="1y", interval="1d"))
        except Exception:
            df = None
    return df

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

def refresh_symbol(symbol: str, conn: psycopg.Connection):
    symbol = symbol.upper()
    latest_before = _get_latest_date(symbol, conn)
    start = None
    today = datetime.now(timezone.utc).date()
    if latest_before:
        start = latest_before + timedelta(days=1)
        if start > today:
            return {"updated": False, "reason": "up_to_date", "latest": str(latest_before)}
    df = _fetch_history(symbol, start)
    if df is None or df.empty:
        # fallback try if partial fetch was empty
        df = _fetch_history(symbol, None)
        if df is None or df.empty:
            return {"updated": False, "reason": "no_data_from_yfinance", "latest": str(latest_before) if latest_before else None}
    _store_history(symbol, symbol, df, conn)
    latest_after = _get_latest_date(symbol, conn)
    updated = bool(latest_after and (latest_before is None or latest_after > latest_before))
    return {"updated": updated, "reason": "ok" if updated else "no_new_rows", "latest": str(latest_after)}

# --- REMOVE old background refresh ---
# Delete the @app.on_event("startup") block

# --- NEW: secure internal batch refresh endpoint ---
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
    for s in symbols:
        try:
            res = refresh_symbol(s, conn)
            results[s.upper()] = res
            if res.get("updated"):
                updated.append(s.upper())
        except Exception as e:
            results[s.upper()] = {"updated": False, "reason": f"error:{e}"}
    return {"updated": updated, "count": len(updated), "results": results}

@app.get("/internal/stale")
def stale_symbols(secret: str = Query(None), conn: psycopg.Connection = Depends(get_db_connection)):
    if secret != REFRESH_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    today = datetime.now().date()
    with conn.cursor() as cur:
        # Symbols whose latest stored date < today
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

# --- FIX refresh parameter logic in /api/stocks/{term} (indentation + flow) ---
@app.get("/api/stocks/{term}")
def get_stock(term: str, refresh: int = Query(0), conn: psycopg.Connection = Depends(get_db_connection)):
    data = query_stock_data(term, conn)
    if not data:
        raise HTTPException(status_code=404, detail="Stock not found")

    if refresh:
        # Refresh only this symbol (incremental)
        refresh_symbol(data["symbol"], conn)
        data = query_stock_data(term, conn)

    # Attach live info
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
def live(symbol: str):
    info = get_live_info(symbol.upper())
    if not info:
        raise HTTPException(status_code=404, detail="No live data")
    return {"symbol": symbol.upper(), "live_info": info}

@app.get("/api/symbols")
def list_symbols(conn: psycopg.Connection = Depends(get_db_connection)):
    with conn.cursor() as cur:
        cur.execute("SELECT symbol FROM stocks ORDER BY symbol")
        return [r[0] for r in cur.fetchall()]

@app.get("/health/db")
def health(conn: psycopg.Connection = Depends(get_db_connection)):
    with conn.cursor() as c: c.execute("SELECT 1")
    return {"ok": True}
