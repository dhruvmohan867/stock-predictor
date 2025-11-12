import os
import psycopg
import yfinance as yf
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
from io import StringIO
import time
import requests
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed  # <-- add this

# -------------------------------------------------------------------------
# 🧩 Setup
# -------------------------------------------------------------------------
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# Logging (instead of print → better for production)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

RATE_LIMIT_SEC = float(os.getenv("YF_RATE_LIMIT_SEC", "0.5"))
PIPELINE_WORKERS = int(os.getenv("PIPELINE_WORKERS", "1"))

_YF_LOCK = threading.Lock()
_last_call = 0.0

def _rate_limit_wait():
    global _last_call
    with _YF_LOCK:
        now = time.time()
        delay = _last_call + RATE_LIMIT_SEC - now
        if delay > 0:
            time.sleep(delay)
        _last_call = time.time()

def _with_backoff(fn, retries=4, base=0.75):
    for i in range(retries):
        try:
            _rate_limit_wait()
            return fn()
        except Exception:
            if i == retries - 1:
                return None
            time.sleep(base * (2 ** i))

# -------------------------------------------------------------------------
# 🧱 Ensure database tables exist
# -------------------------------------------------------------------------
def create_tables_if_not_exist():
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS stocks (
                        id SERIAL PRIMARY KEY,
                        symbol VARCHAR(10) UNIQUE NOT NULL,
                        company_name VARCHAR(100),
                        sector VARCHAR(50)
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS stock_prices (
                        id SERIAL PRIMARY KEY,
                        stock_id INT REFERENCES stocks(id),
                        date DATE NOT NULL,
                        open NUMERIC,
                        high NUMERIC,
                        low NUMERIC,
                        close NUMERIC,
                        volume BIGINT,
                        UNIQUE(stock_id, date)
                    )
                """)
            conn.commit()
        logging.info("✅ Database tables ready")
        return True
    except Exception as e:
        logging.error(f"Error creating tables: {e}")
        return False

# -------------------------------------------------------------------------
# 🌐 Fetch S&P 500 company list
# -------------------------------------------------------------------------
def get_sp500_companies():
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        headers = {"User-Agent": "Mozilla/5.0"}
        html = requests.get(url, headers=headers).text
        df = pd.read_html(StringIO(html))[0]
        companies = [
            {"symbol": row["Symbol"].replace(".", "-"), "name": row["Security"]}
            for _, row in df.iterrows()
        ]
        logging.info(f"✅ Loaded {len(companies)} S&P 500 companies.")
        return companies
    except Exception as e:
        logging.warning(f"Could not fetch S&P 500 list: {e}")
        return [
            {"symbol": "AAPL", "name": "Apple"},
            {"symbol": "MSFT", "name": "Microsoft"},
            {"symbol": "TSLA", "name": "Tesla"},
            {"symbol": "GOOGL", "name": "Alphabet"},
        ]

# -------------------------------------------------------------------------
# 📅 Incremental fetching helper
# -------------------------------------------------------------------------
def get_latest_date(symbol):
    """Return the latest stored date for the given symbol."""
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM stocks WHERE symbol=%s", (symbol,))
                row = cur.fetchone()
                if not row:
                    return None
                stock_id = row[0]
                cur.execute("SELECT MAX(date) FROM stock_prices WHERE stock_id=%s", (stock_id,))
                date_row = cur.fetchone()
                return date_row[0]
    except Exception as e:
        logging.error(f"Error checking latest date for {symbol}: {e}")
        return None

# -------------------------------------------------------------------------
# 📈 Fetch data from Yahoo Finance
# -------------------------------------------------------------------------
_YF_SESSION = requests.Session()
_YF_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
})

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
ALPHA_HISTORY = os.getenv("ALPHA_HISTORY", "0") == "1"
AV_RATE_LIMIT_SEC = float(os.getenv("AV_RATE_LIMIT_SEC", "12"))

_AV_LOCK = threading.Lock()
_av_last = 0.0
def _av_wait():
    global _av_last
    with _AV_LOCK:
        now = time.time()
        delay = _av_last + AV_RATE_LIMIT_SEC - now
        if delay > 0:
            time.sleep(delay)
        _av_last = time.time()

def fetch_stock_data_alpha(symbol, start_date=None):
    if not ALPHA_HISTORY or not ALPHA_VANTAGE_API_KEY:
        return None
    try:
        _av_wait()
        r = requests.get(
            "https://www.alphavantage.co/query",
            params={"function": "TIME_SERIES_DAILY_ADJUSTED", "symbol": symbol, "apikey": ALPHA_VANTAGE_API_KEY},
            timeout=25,
        )
        j = r.json()
        ts = j.get("Time Series (Daily)")
        if not ts:
            logging.warning(f"Alpha returned no 'Time Series (Daily)' for {symbol}: "
                            f"Note={j.get('Note')} Error={j.get('Error Message')}")
            return None
        rows = []
        for d, row in ts.items():
            rows.append({
                "Date": datetime.strptime(d, "%Y-%m-%d").date(),
                "Open": float(row.get("1. open", 0)),
                "High": float(row.get("2. high", 0)),
                "Low":  float(row.get("3. low", 0)),
                "Close":float(row.get("4. close", 0)),
                "Volume": int(float(row.get("6. volume", 0))),
            })
        df = pd.DataFrame(rows).sort_values("Date")
        if start_date:
            df = df[df["Date"] >= start_date]
        df.set_index("Date", inplace=True)
        return df
    except Exception as e:
        logging.warning(f"Alpha history failed for {symbol}: {e}")
        return None

def fetch_stock_data(symbol, start_date=None):
    # Try Alpha first
    df = fetch_stock_data_alpha(symbol, start_date)
    if df is not None and not df.empty:
        return df
    # Yahoo fallback (no custom session)
    try:
        ticker = yf.Ticker(symbol)  # ← remove session arg
        if start_date:
            data = _with_backoff(lambda: ticker.history(start=start_date, end=datetime.today(), interval="1d"))
        else:
            data = _with_backoff(lambda: ticker.history(period="1y", interval="1d"))
        if data is None or data.empty:
            logging.warning(f"No data for {symbol}")
            return None
        return data
    except Exception as e:
        logging.error(f"Error fetching {symbol}: {e}")
        return None

# -------------------------------------------------------------------------
# 💾 Store data safely in PostgreSQL
# -------------------------------------------------------------------------
def store_stock_data(symbol, company_name, df):
    if df is None or df.empty:
        return
    try:
        with psycopg.connect(DATABASE_URL, prepare_threshold=None) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO stocks (symbol, company_name)
                    VALUES (%s, %s)
                    ON CONFLICT (symbol) DO UPDATE SET company_name = EXCLUDED.company_name
                    RETURNING id
                """, (symbol, company_name))
                stock_id = cur.fetchone()[0]
                # Bulk insert for performance
                rows = [
                    (
                        stock_id,
                        date.date(),
                        float(r["Open"]),
                        float(r["High"]),
                        float(r["Low"]),
                        float(r["Close"]),
                        int(r["Volume"]) if not pd.isna(r["Volume"]) else 0,
                    )
                    for date, r in df.iterrows()
                ]
                cur.executemany("""
                    INSERT INTO stock_prices (stock_id, date, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (stock_id, date) DO NOTHING
                """, rows)
            conn.commit()
        logging.info(f"💾 Stored {len(df)} records for {symbol}")
    except Exception as e:
        logging.error(f"Database error for {symbol}: {e}")

# -------------------------------------------------------------------------
# 🚀 Main Execution (threaded)
# -------------------------------------------------------------------------
def process_company(company):
    symbol = company["symbol"]
    name = company["name"]
    latest = get_latest_date(symbol)
    start_date = None
    if latest:
        start_date = latest + timedelta(days=1)
    df = fetch_stock_data(symbol, start_date)
    if df is not None and not df.empty:
        store_stock_data(symbol, name, df)

def main():
    logging.info("🚀 Starting Stock Data Fetch Pipeline...")
    if not DATABASE_URL:
        logging.error("❌ DATABASE_URL missing")
        return
    if not create_tables_if_not_exist():
        logging.error("❌ Database setup failed")
        return

    companies = get_sp500_companies()
    with ThreadPoolExecutor(max_workers=PIPELINE_WORKERS) as executor:  # set via env (1 recommended on free AV)
        futures = [executor.submit(process_company, c) for c in companies]
        for i, f in enumerate(as_completed(futures), start=1):
            try:
                f.result()
                logging.info(f"✅ ({i}/{len(companies)}) Done")
            except Exception as e:
                logging.error(f"Thread error: {e}")
    logging.info("🎯 All stock data updated successfully!")

if __name__ == "__main__":
    main()
