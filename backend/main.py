import os
import sys
import psycopg
import yfinance as yf
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, HTTPException, Depends, Response, Query, BackgroundTasks
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import asyncio
import joblib
from psycopg_pool import ConnectionPool
import pandas as pd
import numpy as np
import math
import time
import threading
import requests
import random

load_dotenv()

# -------------------- DATABASE POOL --------------------
pool = None

def _normalize_dsn(dsn: str) -> str:
    if ("supabase.co" in dsn or "supabase.com" in dsn) and "sslmode=" not in dsn:
        return f"{dsn}{'?' if '?' not in dsn else '&'}sslmode=require"
    return dsn

def get_pool():
    global pool
    # --- THIS IS THE CRITICAL FIX ---
    if pool is None or pool.closed:
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            raise RuntimeError("DATABASE_URL missing.")
        dsn = _normalize_dsn(dsn)
        pool = ConnectionPool(conninfo=dsn, min_size=1, max_size=10, kwargs={'prepare_threshold': None})
        print("🔄 Database connection pool was closed. Recreated.")
    # --- END OF FIX ---
    return pool

def get_db_connection():
    db_pool = get_pool()
    with db_pool.connection() as conn:
        yield conn


# -------------------- FASTAPI APP & CORS --------------------
app = FastAPI()


FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:5173").rstrip("/")


ALLOWED_ORIGINS = [
    FRONTEND_URL,
    "https://stock-predictor-five-opal.vercel.app", 
    "http://localhost:5173",                       
    "http://localhost:3000",                       
]

# 3. Apply Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS, 
    allow_credentials=True,
    allow_methods=["*"],           
    allow_headers=["*"],           
)


# -------------------- Yahoo Finance Helper --------------------
RATE_LIMIT_SEC = float(os.getenv("YF_RATE_LIMIT_SEC", "1.0"))
_YF_LOCK = threading.Lock()
_last_call_ts = 0.0

_YF_SESSIONS = []
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
]

def _get_session():
    if not _YF_SESSIONS:
        for ua in USER_AGENTS:
            s = requests.Session()
            s.headers.update({"User-Agent": ua})
            _YF_SESSIONS.append(s)
    return random.choice(_YF_SESSIONS)


# -------------------- In-Memory Cache --------------------
LIVE_TTL_SEC = int(os.getenv("LIVE_TTL_SEC", "60"))
_LIVE_CACHE = {}
_CACHE_LOCK = threading.Lock()


# -------------------- ML Model --------------------
MODEL_PATH = os.path.join(os.path.dirname(__file__), 'ml_model', 'stock_predictor.joblib')
model = None

try:
    model = joblib.load(MODEL_PATH)
    print("✓ ML Model Loaded Successfully")
except Exception as e:
    print(f"⚠️ ML Model load failed: {e}")


# -------------------- Helper Queries --------------------
def query_stock_data(search_term, conn: psycopg.Connection):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, symbol, company_name
            FROM stocks WHERE symbol ILIKE %s OR company_name ILIKE %s
        """, (f"%{search_term}%", f"%{search_term}%"))
        
        stock = cur.fetchone()
        if not stock:
            return None
        
        stock_id, symbol, name = stock
        today = datetime.now(timezone.utc).date()

        cur.execute("""
            SELECT date, open, high, low, close, volume
            FROM stock_prices 
            WHERE stock_id=%s AND date <= %s
            ORDER BY date DESC 
            LIMIT 365
        """, (stock_id, today))

        rows = cur.fetchall()

        return {
            "symbol": symbol, 
            "company_name": name,
            "prices": [
                {"date": r[0].isoformat(), "open": float(r[1]), "high": float(r[2]),
                 "low": float(r[3]), "close": float(r[4]), "volume": int(r[5])}
                for r in rows
            ]
        }


def get_live_info(symbol: str, conn: psycopg.Connection):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT sp.close, sp.high, sp.low, s.company_name
            FROM stock_prices sp
            JOIN stocks s ON s.id = sp.stock_id
            WHERE s.symbol = %s
            ORDER BY sp.date DESC
            LIMIT 1
        """, (symbol.upper(),))
        
        latest = cur.fetchone()

    if not latest:
        return None

    close, high, low, name = latest
    return {
        "currentPrice": float(close),
        "dayHigh": float(high),
        "dayLow": float(low),
        "marketCap": None,
        "source": "database"
    }


# -------------------- API ROUTES --------------------
@app.get("/")
def home():
    return {"message": "Stock Predictor API Running"}


@app.get("/api/stocks/{term}")
def get_stock(term: str, conn: psycopg.Connection = Depends(get_db_connection)):
    data = query_stock_data(term, conn)
    if not data:
        raise HTTPException(status_code=404, detail="Stock not found")

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

    latest = data["prices"][0]
    features = np.array([[latest[key] for key in ['open', 'high', 'low', 'close', 'volume']]])
    
    prediction = float(model.predict(features)[0])
    live = get_live_info(req["symbol"], conn)

    return {
        "symbol": req["symbol"],
        "predicted_next_day_close": prediction,
        "live_info": live,
    }


@app.get("/api/live/{symbol}")
def live(symbol: str, conn: psycopg.Connection = Depends(get_db_connection)):
    info = get_live_info(symbol, conn)
    if info:
        return {"symbol": symbol.upper(), "live_info": info}

    raise HTTPException(status_code=404, detail="No data found")


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
