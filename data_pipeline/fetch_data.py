import os
import psycopg
import yfinance as yf
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
from io import StringIO
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

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
def fetch_stock_data(symbol, start_date=None):
    try:
        ticker = yf.Ticker(symbol)
        if start_date:
            data = ticker.history(start=start_date, end=datetime.today(), interval="1d")
        else:
            data = ticker.history(period="1y", interval="1d")
        if data.empty:
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
    with ThreadPoolExecutor(max_workers=10) as executor:
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
