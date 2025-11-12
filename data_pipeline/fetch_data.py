import os
import psycopg
import yfinance as yf
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
from io import StringIO # <-- Add StringIO
import time
import requests
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

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
                        symbol VARCHAR(30) UNIQUE NOT NULL, -- MODIFIED: Increased length
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
# 🌐 NEW: Functions to fetch S&P 500 and NIFTY 500 stock lists
# -------------------------------------------------------------------------
def get_sp500_stocks():
    """Fetches the list of S&P 500 companies from Wikipedia."""
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        html = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}).text
        df = pd.read_html(StringIO(html))[0]
        df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)
        stocks = [{"symbol": row['Symbol'], "name": row['Security']} for _, row in df.iterrows()]
        logging.info(f"✅ Fetched {len(stocks)} S&P 500 stocks.")
        return stocks
    except Exception as e:
        logging.error(f"Could not fetch S&P 500 stocks: {e}")
        return []

def get_nifty500_stocks():
    """Fetches the list of NIFTY 500 companies from a public CSV."""
    try:
        url = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"
        df = pd.read_csv(url)
        # Indian stocks need a '.NS' suffix for yfinance
        stocks = [{"symbol": f"{row['Symbol']}.NS", "name": row['Company Name']} for _, row in df.iterrows()]
        logging.info(f"✅ Fetched {len(stocks)} NIFTY 500 stocks.")
        return stocks
    except Exception as e:
        logging.error(f"Could not fetch NIFTY 500 stocks: {e}")
        return []

def get_target_stocks():
    """Combines S&P 500 and NIFTY 500 lists."""
    sp500 = get_sp500_stocks()
    nifty500 = get_nifty500_stocks()
    
    # Add a few major indexes as well
    indexes = [
        {"symbol": "^NSEI", "name": "NIFTY 50"},
        {"symbol": "^BSESN", "name": "S&P BSE SENSEX"},
        {"symbol": "^IXIC", "name": "NASDAQ Composite"},
        {"symbol": "^GSPC", "name": "S&P 500"},
    ]
    
    all_stocks = sp500 + nifty500 + indexes
    logging.info(f"✅ Defined a target list of {len(all_stocks)} total stocks/indexes.")
    return all_stocks

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
# 📈 REMOVED: All Alpha Vantage logic is gone for simplicity
# -------------------------------------------------------------------------

def fetch_stock_data(symbol, start_date=None):
    """Fetches historical data exclusively from Yahoo Finance."""
    try:
        # Use yfinance to fetch data. It's robust.
        ticker = yf.Ticker(symbol)
        
        # Fetch data from the start date. If no start date, get max history.
        data = _with_backoff(lambda: ticker.history(
            start=start_date, 
            end=datetime.today(), 
            interval="1d",
            auto_adjust=False # Important for raw OHLCV data
        ))

        if data is None or data.empty:
            # If that fails, try a shorter period as a fallback
            data = _with_backoff(lambda: ticker.history(period="1y", interval="1d", auto_adjust=False))

        if data is None or data.empty:
            logging.warning(f"No data found for {symbol} after retries.")
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

    # --- RESUME LOGIC START ---
    # If we have data and the latest date is today or yesterday, skip it.
    # This makes the pipeline resumable and efficient.
    if latest:
        today = datetime.today().date()
        if latest >= today - timedelta(days=1):
            logging.info(f"✅ Skipping '{symbol}', already up-to-date ({latest}).")
            return # Exit this function for this symbol
        start_date = latest + timedelta(days=1)
    # --- RESUME LOGIC END ---
        
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

    companies = get_target_stocks() # <-- Use the new function
    with ThreadPoolExecutor(max_workers=PIPELINE_WORKERS) as executor:
        futures = [executor.submit(process_company, c) for c in companies]
        for i, f in enumerate(as_completed(futures), start=1):
            try:
                f.result()
                logging.info(f"✅ ({i}/{len(companies)}) Processed {companies[i-1]['symbol']}")
            except Exception as e:
                logging.error(f"Thread error: {e}")
    logging.info("🎯 All stock data updated successfully!")

if __name__ == "__main__":
    main()
