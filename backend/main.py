import os
import sys
import psycopg
import yfinance as yf
import pandas as pd
from fastapi import FastAPI, HTTPException, Depends, Response, Query
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import joblib
from pydantic import BaseModel
from datetime import datetime, timezone
from psycopg_pool import ConnectionPool
import math

ACCESS_TOKEN_EXPIRE_MINUTES = 30

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

load_dotenv()

# --- DATABASE CONNECTION POOL ---
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
            raise RuntimeError("DATABASE_URL not configured.")
        dsn = _normalize_dsn(dsn)
        # --- FIX: Add prepare_threshold=None to the connection pool ---
        # This prevents the "prepared statement already exists" error on the live server.
        pool = ConnectionPool(conninfo=dsn, min_size=1, max_size=10, kwargs={'prepare_threshold': None})
        print("✓ Database connection pool initialized.")
    return pool

def get_db_connection():
    db_pool = get_pool()
    with db_pool.connection() as conn:
        yield conn

# --- FASTAPI APP SETUP ---
app = FastAPI()

# CORS: allow your exact domain and any Vercel preview domains
FRONTEND_ORIGINS = [
    "http://localhost:5173",
    "https://stock-predictor-five-opal.vercel.app",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=FRONTEND_ORIGINS,
    allow_origin_regex=r"https://.*\.vercel\.app$",  # <-- allow all Vercel previews
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    max_age=86400,
)

@app.on_event("startup")
async def startup_event():
    get_pool()

@app.on_event("shutdown")
async def shutdown_event():
    global pool
    if pool:
        pool.close()

# --- add a universal preflight handler (helps some hosts/proxies) ---
@app.options("/{rest_of_path:path}")
def preflight_ok(rest_of_path: str) -> Response:
    return Response(status_code=204)

# --- LOAD MODEL ---
MODEL_PATH = os.path.join(os.path.dirname(__file__), 'ml_model', 'stock_predictor.joblib')
model = None
try:
    model = joblib.load(MODEL_PATH)
    print(f"✓ ML model loaded successfully from {MODEL_PATH}")
except Exception as e:
    print(f"⚠️ Error loading model: {e}")

# --- SCHEMAS ---
# removed: UserRegister, GoogleLoginRequest
class PredictionRequest(BaseModel):
    symbol: str

# --- HELPER: QUERY STOCK DATA ---
def query_stock_data(search_term: str, conn: psycopg.Connection): # <-- MODIFICATION: Use a generic search_term
    with conn.cursor() as cur:
        # --- MODIFICATION: Search by symbol OR company name ---
        # The ILIKE operator is case-insensitive.
        cur.execute("""
            SELECT id, symbol, company_name 
            FROM stocks 
            WHERE symbol ILIKE %s OR company_name ILIKE %s
        """, (f"{search_term.upper()}", f"%{search_term}%"))
        
        stock_record = cur.fetchone()
        if not stock_record:
            return None
            
        stock_id, symbol, company_name = stock_record
        # --- END MODIFICATION ---

        cur.execute("SELECT date, open, high, low, close, volume FROM stock_prices WHERE stock_id = %s ORDER BY date DESC", (stock_id,))
        prices = cur.fetchall()
        # --- MODIFICATION: Return the full company info ---
        return {"symbol": symbol, "company_name": company_name, "prices": [{"date": r[0].isoformat(), "open": float(r[1]), "high": float(r[2]), "low": float(r[3]), "close": float(r[4]), "volume": int(r[5])} for r in prices]}

# --- NEW: Add the exact same data fetching functions from your pipeline ---

def fetch_stock_data(symbol):
    """Fetches daily stock data and live info from Yahoo Finance."""
    try:
        print(f"🔄 Fetching data for {symbol} from Yahoo Finance...")
        ticker = yf.Ticker(symbol)

        # Be explicit to avoid provider defaults/caching quirks
        history_df = ticker.history(period="1y", interval="1d", auto_adjust=False)
        if history_df.empty:
            print(f"⚠️ No historical data found for {symbol}")
            return None, None

        # Live fields (fallback-safe)
        live_info = get_live_info(symbol)

        print(f"✅ Data and live info fetched successfully for {symbol}")
        return history_df, live_info
    except Exception as e:
        print(f"❌ Error fetching {symbol}: {e}")
        return None, None

# --- FIX: This function now accepts a 'conn' object ---
def store_stock_data(symbol, company_name, df, conn: psycopg.Connection): # <-- MODIFICATION: Accept company_name
    """Stores stock data from Yahoo Finance into the database."""
    if df is None or df.empty:
        return

    try:
        # It now uses the connection passed to it instead of creating a new one.
        with conn.cursor() as cur:
            # --- MODIFICATION: Update company name on conflict ---
            cur.execute("""
                INSERT INTO stocks (symbol, company_name) VALUES (%s, %s) 
                ON CONFLICT (symbol) DO UPDATE SET company_name = EXCLUDED.company_name
                RETURNING id
            """, (symbol, company_name))
            
            result = cur.fetchone()
            
            # --- FIX: Correctly fetch the stock_id if it already exists ---
            if result:
                stock_id = result[0]
            else:
                # 1. Execute the query first
                cur.execute("SELECT id FROM stocks WHERE symbol = %s", (symbol,))
                # 2. Then fetch the result
                stock_id_result = cur.fetchone()
                if stock_id_result:
                    stock_id = stock_id_result[0]
                else:
                    # This case should ideally not happen, but it's good to handle it.
                    print(f"❌ Critical error: Could not find or create stock_id for {symbol}")
                    return 
            # --- END FIX ---

            for date, row in df.iterrows():
                cur.execute("""
                    INSERT INTO stock_prices (stock_id, date, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (stock_id, date) DO NOTHING
                """, (stock_id, date.date(), float(row["Open"]), float(row["High"]), float(row["Low"]), float(row["Close"]), int(row["Volume"]) if not pd.isna(row["Volume"]) else 0))
            
            conn.commit()
    except Exception as e:
        print(f"❌ Database error for {symbol}: {e}")
        conn.rollback() # Rollback on error

# --- END of new functions ---


# --- ROUTES ---
@app.get("/")
def read_root():
    return {"message": "Stock Prediction API is running."}

@app.get("/api/stocks/{search_term}")
def get_stock_prices(
    search_term: str,
    refresh: int = Query(0, description="Force refresh from upstream when 1"),
    conn: psycopg.Connection = Depends(get_db_connection),
):
    data = query_stock_data(search_term, conn)

    force_refresh = bool(refresh)
    is_stale = False
    if data and data["prices"]:
        latest_db_date = datetime.fromisoformat(data["prices"][0]["date"]).date()
        today = datetime.now(timezone.utc).date()
        is_weekday = today.weekday() < 5
        if is_weekday and latest_db_date < today:
            is_stale = True
            print(f"⚠️ Stale history for {data['symbol']} (latest: {latest_db_date})")

    # If we have data, not forcing and not stale -> return with live info
    if data and not force_refresh and not is_stale:
        live_info = get_live_info(data["symbol"])
        if live_info:
            data["live_info"] = live_info
        return data

    # Otherwise fetch/refresh history, store, and return with live info
    symbol_to_fetch = data['symbol'] if data else search_term.upper()
    print(f"🔄 Refreshing history for {symbol_to_fetch} (force={force_refresh} stale={is_stale})")
    new_history_df, live_info = fetch_stock_data(symbol_to_fetch)
    if new_history_df is None:
        if data:
            # Fallback to whatever we have, but attach live if possible
            data["live_info"] = live_info or get_live_info(symbol_to_fetch)
            return data
        raise HTTPException(status_code=404, detail=f"Could not fetch data for '{search_term}'.")

    company_name_to_store = data['company_name'] if data and data.get('company_name') else symbol_to_fetch
    store_stock_data(symbol_to_fetch, company_name_to_store, new_history_df, conn)

    final_data = query_stock_data(symbol_to_fetch, conn)
    if final_data:
        final_data["live_info"] = live_info or get_live_info(symbol_to_fetch)
    return final_data

@app.post("/api/predict")
def predict_stock_price(
    request: PredictionRequest,
    conn: psycopg.Connection = Depends(get_db_connection),
):  # removed auth.get_current_user
    if model is None:
        raise HTTPException(status_code=503, detail="ML model not loaded on server")
    data = query_stock_data(request.symbol, conn)
    if not data or not data["prices"]:
        raise HTTPException(status_code=404, detail=f"Not enough historical data for {request.symbol}")
    try:
        latest = data["prices"][0]
        input_data = pd.DataFrame([{"open": latest["open"], "high": latest["high"], "low": latest["low"], "close": latest["close"], "volume": latest["volume"]}])
        prediction = model.predict(input_data)[0]
        return {"symbol": request.symbol, "predicted_next_day_close": float(prediction)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction failed: {e}")

@app.get("/health/db")
def health_db(conn: psycopg.Connection = Depends(get_db_connection)):
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        cur.fetchone()
    return {"ok": True}

def get_live_info(symbol: str):
    """Robust live metrics:
       1) Use 1d/1m intraday to compute current, dayHigh, dayLow (works for most tickers)
       2) Fill gaps from fast_info/info
       3) Final fallback: last 5d/1d candle
    """
    def _clean(v):
        try:
            if v is None:
                return None
            f = float(v)
            return None if math.isnan(f) else f
        except Exception:
            return None

    try:
        t = yf.Ticker(symbol)

        current = day_high = day_low = market_cap = None

        # 1) Intraday minute data for "today" (most reliable for day range)
        try:
            m1 = t.history(period="1d", interval="1m", auto_adjust=False, prepost=True)
            if not m1.empty:
                last = m1.iloc[-1]
                current = _clean(last.get("Close"))
                day_high = _clean(m1["High"].max())
                day_low  = _clean(m1["Low"].min())
        except Exception:
            pass

        # 2) fast_info/info for market cap and any missing fields
        try:
            fi = getattr(t, "fast_info", None)
            if fi:
                market_cap = market_cap or _clean(getattr(fi, "market_cap", None))
                # sometimes intraday fails; fill from fast_info if needed
                current = current or _clean(getattr(fi, "last_price", None))
                day_high = day_high or _clean(getattr(fi, "day_high", None))
                day_low  = day_low  or _clean(getattr(fi, "day_low", None))
        except Exception:
            pass

        if any(v is None for v in (current, day_high, day_low, market_cap)):
            try:
                info = t.info or {}
                market_cap = market_cap or _clean(info.get("marketCap"))
                current = current or _clean(info.get("regularMarketPrice") or info.get("currentPrice"))
                day_high = day_high or _clean(info.get("dayHigh"))
                day_low  = day_low  or _clean(info.get("dayLow"))
            except Exception:
                pass

        # 3) Final fallback from daily candle (e.g., off-hours or restricted tickers)
        if any(v is None for v in (current, day_high, day_low)):
            try:
                d1 = t.history(period="5d", interval="1d", auto_adjust=False, prepost=True)
                if not d1.empty:
                    last = d1.iloc[-1]
                    current = current or _clean(last.get("Close"))
                    day_high = day_high or _clean(last.get("High"))
                    day_low  = day_low  or _clean(last.get("Low"))
            except Exception:
                pass

        return {
            "currentPrice": current,
            "dayHigh": day_high,
            "dayLow": day_low,
            "marketCap": market_cap,
        }
    except Exception as e:
        print(f"❌ get_live_info failed for {symbol}: {e}")
        return None

# Optional: a live-only endpoint (no DB), handy for debugging UI
@app.get("/api/live/{symbol}")
def get_live(symbol: str):
    live = get_live_info(symbol.upper())
    if not live:
        raise HTTPException(status_code=404, detail="Live quote unavailable")
    return {"symbol": symbol.upper(), "live_info": live}
