import os
import threading
import random
import time
from datetime import datetime, timezone

import psycopg
from psycopg_pool import ConnectionPool
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import joblib
import numpy as np

# --------------------------------------------------
# ENV
# --------------------------------------------------
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
FRONTEND_URL = os.getenv("FRONTEND_URL", "").rstrip("/")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

# --------------------------------------------------
# DATABASE POOL (RENDER SAFE)
# --------------------------------------------------
pool: ConnectionPool | None = None


def _normalize_dsn(dsn: str) -> str:
    if "sslmode=" not in dsn:
        return f"{dsn}{'?' if '?' not in dsn else '&'}sslmode=require"
    return dsn


def get_pool() -> ConnectionPool:
    global pool
    if pool is None or pool.closed:
        pool = ConnectionPool(
            conninfo=_normalize_dsn(DATABASE_URL),
            min_size=1,
            max_size=10,
            kwargs={"prepare_threshold": None},
        )
        print("✓ Database pool initialized")
    return pool


def get_db_connection():
    pool = get_pool()
    with pool.connection() as conn:
        yield conn


# --------------------------------------------------
# FASTAPI APP
# --------------------------------------------------
app = FastAPI(title="Stock Predictor API")

ALLOWED_ORIGINS = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:3000",
    "https://stock-predictor-five-opal.vercel.app",
]

if FRONTEND_URL and FRONTEND_URL not in ALLOWED_ORIGINS:
    ALLOWED_ORIGINS.append(FRONTEND_URL)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --------------------------------------------------
# ML MODEL
# --------------------------------------------------
MODEL_PATH = os.path.join(os.path.dirname(__file__), "ml_model", "stock_predictor.joblib")
model = None

try:
    model = joblib.load(MODEL_PATH)
    print("✓ ML model loaded")
except Exception as e:
    print("⚠️ ML model failed to load:", e)

# --------------------------------------------------
# DATA HELPERS
# --------------------------------------------------
def query_stock_data(term: str, conn: psycopg.Connection):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, symbol, company_name
            FROM stocks
            WHERE symbol ILIKE %s OR company_name ILIKE %s
            """,
            (f"%{term}%", f"%{term}%"),
        )
        row = cur.fetchone()
        if not row:
            return None

        stock_id, symbol, name = row

        cur.execute(
            """
            SELECT date, open, high, low, close, volume
            FROM stock_prices
            WHERE stock_id=%s
            ORDER BY date DESC
            LIMIT 365
            """,
            (stock_id,),
        )

        prices = cur.fetchall()

        return {
            "symbol": symbol,
            "company_name": name,
            "prices": [
                {
                    "date": r[0].isoformat(),
                    "open": float(r[1]),
                    "high": float(r[2]),
                    "low": float(r[3]),
                    "close": float(r[4]),
                    "volume": int(r[5]),
                }
                for r in prices
            ],
        }


def get_live_info(symbol: str, conn: psycopg.Connection):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT sp.close, sp.high, sp.low, s.company_name
            FROM stock_prices sp
            JOIN stocks s ON s.id = sp.stock_id
            WHERE s.symbol = %s
            ORDER BY sp.date DESC
            LIMIT 1
            """,
            (symbol.upper(),),
        )
        row = cur.fetchone()

    if not row:
        return None

    close, high, low, name = row
    return {
        "currentPrice": float(close),
        "dayHigh": float(high),
        "dayLow": float(low),
        "marketCap": None,
        "source": "database",
    }


# --------------------------------------------------
# ROUTES
# --------------------------------------------------
@app.get("/")
def root():
    return {"status": "ok", "service": "Stock Predictor API"}


@app.get("/api/stocks/{term}")
def get_stock(term: str, conn: psycopg.Connection = Depends(get_db_connection)):
    try:
        data = query_stock_data(term, conn)
        if not data:
            raise HTTPException(status_code=404, detail="Stock not found")

        live = get_live_info(data["symbol"], conn)
        if live:
            data["live_info"] = live

        return data
    except HTTPException:
        raise
    except Exception as e:
        print("ERROR /api/stocks:", e)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/api/predict")
def predict(payload: dict, conn: psycopg.Connection = Depends(get_db_connection)):
    if model is None:
        raise HTTPException(status_code=503, detail="Model not available")

    symbol = payload.get("symbol")
    if not symbol:
        raise HTTPException(status_code=400, detail="symbol is required")

    data = query_stock_data(symbol, conn)
    if not data or not data["prices"]:
        raise HTTPException(status_code=404, detail="No historical data")

    latest = data["prices"][0]
    features = np.array([[latest[k] for k in ("open", "high", "low", "close", "volume")]])

    try:
        prediction = float(model.predict(features)[0])
    except Exception as e:
        print("Prediction error:", e)
        raise HTTPException(status_code=500, detail="Prediction failed")

    live = get_live_info(symbol, conn)

    return {
        "symbol": symbol.upper(),
        "predicted_next_day_close": prediction,
        "live_info": live,
    }


@app.get("/api/live/{symbol}")
def live(symbol: str, conn: psycopg.Connection = Depends(get_db_connection)):
    info = get_live_info(symbol, conn)
    if not info:
        raise HTTPException(status_code=404, detail="No live data")
    return {"symbol": symbol.upper(), "live_info": info}


@app.get("/api/symbols")
def symbols(conn: psycopg.Connection = Depends(get_db_connection)):
    with conn.cursor() as cur:
        cur.execute("SELECT symbol FROM stocks ORDER BY symbol")
        return [r[0] for r in cur.fetchall()]


@app.get("/health/db")
def health_db(conn: psycopg.Connection = Depends(get_db_connection)):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return {"ok": True}
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": e.__class__.__name__},
        )
