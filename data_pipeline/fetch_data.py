import os
import psycopg
import yfinance as yf
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import time
import requests
from io import StringIO

# -------------------------------------------------------------------------
# 🔐 Load environment variables
# -------------------------------------------------------------------------
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# -------------------------------------------------------------------------
# 🧱 STEP 1: Ensure database tables exist
# -------------------------------------------------------------------------
def create_tables_if_not_exist():
    """Create necessary tables if they don't exist."""
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
        print("✅ Database tables ready")
        return True
    except Exception as e:
        print(f"❌ Error setting up tables: {e}")
        return False

# -------------------------------------------------------------------------
# 🌐 STEP 2: Fetch data from Yahoo Finance
# -------------------------------------------------------------------------
def fetch_stock_data(symbol):
    """Fetch 1-year daily historical stock data using Yahoo Finance."""
    try:
        print(f"🔄 Fetching data for {symbol} from Yahoo Finance...")
        ticker = yf.Ticker(symbol)
        data = ticker.history(period="1y")  # Can be changed to "5y" or "max"
        if data.empty:
            print(f"⚠️ No data found for {symbol}")
            return None
        print(f"✅ Data fetched successfully for {symbol}")
        return data
    except Exception as e:
        print(f"❌ Error fetching {symbol}: {e}")
        return None

# -------------------------------------------------------------------------
# 💾 STEP 3: Store stock data into PostgreSQL (safe & stable)
# -------------------------------------------------------------------------
def store_stock_data(symbol, df):
    """Store fetched data safely in PostgreSQL."""
    if df is None or df.empty:
        print(f"⚠️ No data to store for {symbol}")
        return

    try:
        # Disable prepared statements (fixes `_pg3_0` issue)
        with psycopg.connect(DATABASE_URL, prepare_threshold=None) as conn:
            with conn.cursor() as cur:
                # Insert or get stock ID
                cur.execute("""
                    INSERT INTO stocks (symbol, company_name)
                    VALUES (%s, %s)
                    ON CONFLICT (symbol) DO NOTHING
                    RETURNING id
                """, (symbol, symbol))

                result = cur.fetchone()
                if result:
                    stock_id = result[0]
                else:
                    cur.execute("SELECT id FROM stocks WHERE symbol = %s", (symbol,))
                    stock_id = cur.fetchone()[0]

                inserted = 0
                for date, row in df.iterrows():
                    cur.execute("""
                        INSERT INTO stock_prices (stock_id, date, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (stock_id, date) DO NOTHING
                    """, (
                        stock_id,
                        date.date(),
                        float(row["Open"]),
                        float(row["High"]),
                        float(row["Low"]),
                        float(row["Close"]),
                        int(row["Volume"]) if not pd.isna(row["Volume"]) else 0
                    ))
                    inserted += cur.rowcount

                conn.commit()
                print(f"💾 Stored {inserted} new records for {symbol}")
    except Exception as e:
        print(f"❌ Database error for {symbol}: {e}")


