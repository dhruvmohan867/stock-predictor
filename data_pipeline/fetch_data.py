import os
import psycopg
import yfinance as yf
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import time
import requests  # <-- Already imported, now we'll use it
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

# -------------------------------------------------------------------------
# 🚀 STEP 4: Main execution block to run the pipeline
# -------------------------------------------------------------------------
if __name__ == "__main__":
    print("🚀 Starting Stock Data Fetch Pipeline...\n")

    if not DATABASE_URL:
        print("❌ Missing DATABASE_URL in .env file")
        exit(1)

    if not create_tables_if_not_exist():
        print("❌ Database setup failed. Exiting.")
        exit(1)

    try:
        # --- Get the full list of S&P 500 symbols ---
        print("Fetching S&P 500 stock list from Wikipedia...")
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        sp500_df = pd.read_html(StringIO(response.text))[0]
        all_symbols = sp500_df['Symbol'].str.replace('.', '-', regex=False).tolist()
        print(f"✅ Found {len(all_symbols)} total symbols in S&P 500.")
    except Exception as e:
        print(f"Could not fetch S&P 500 list, using a default list. Error: {e}")
        all_symbols = ["MSFT", "AAPL", "GOOGL", "AMZN", "TSLA", "NVDA", "JNJ", "MA", "META", "F"]

    # --- NEW: Check for already processed symbols to allow resuming ---
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT symbol FROM stocks")
                # Create a set for fast lookups
                processed_symbols = {row[0] for row in cur.fetchall()}
        print(f"Found {len(processed_symbols)} symbols already in the database.")
        
        # Filter the list to only include symbols that have NOT been processed
        symbols_to_fetch = [s for s in all_symbols if s not in processed_symbols]
        
        if not symbols_to_fetch:
            print("\n✅ All S&P 500 stocks are already up-to-date in the database. Nothing to do.")
            exit(0)
            
        print(f"➡️ {len(symbols_to_fetch)} new symbols to fetch.")

    except Exception as e:
        print(f"⚠️ Could not check for existing symbols, will attempt to fetch all. Error: {e}")
        symbols_to_fetch = all_symbols
    # --- END NEW ---

    for i, symbol in enumerate(symbols_to_fetch, start=1):
        print(f"\n--- ({i}/{len(symbols_to_fetch)}) Processing {symbol} ---")
        data = fetch_stock_data(symbol)
        store_stock_data(symbol, data)
        # --- CRUCIAL: Wait for 1 second to avoid being rate-limited ---
        print("--- Waiting 1 second ---")
        time.sleep(1)

    print("\n✅ All data fetched and stored successfully!")


