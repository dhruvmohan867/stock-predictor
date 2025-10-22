import os
import time
import requests
import psycopg
from dotenv import load_dotenv
from datetime import datetime

# -------------------------------------------------------------------------
# ⚙️ STEP 0: Load environment variables
# -------------------------------------------------------------------------
load_dotenv()

API_KEY = os.getenv("FMP_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

# -------------------------------------------------------------------------
# 🧱 STEP 1: Ensure database tables exist
# -------------------------------------------------------------------------
def create_tables_if_not_exist():
    try:
        conn = psycopg.connect(DATABASE_URL)
        cur = conn.cursor()

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
    except Exception as e:
        print(f"❌ Error setting up tables: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()
    return True

# -------------------------------------------------------------------------
# 🌐 STEP 2: Fetch data from Financial Modeling Prep API
# -------------------------------------------------------------------------
def fetch_stock_data(symbol):
    if not API_KEY:
        print("❌ Error: FMP API key not found in .env")
        return None

    # FMP endpoint for daily historical prices
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}"
    params = {"apikey": API_KEY, "serietype": "line"}

    try:
        print(f"🔄 Fetching data for {symbol} from FMP...")
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if not data or "historical" not in data:
            print(f"⚠️ No data found for {symbol}. Keys: {list(data.keys())}")
            return None

        print(f"✅ Successfully fetched {len(data['historical'])} records for {symbol}")
        return data
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed for {symbol}: {e}")
        return None

# -------------------------------------------------------------------------
# 💾 STEP 3: Store stock data into PostgreSQL
# -------------------------------------------------------------------------
def store_stock_data(symbol, stock_data):
    if not stock_data or "historical" not in stock_data:
        print(f"⚠️ No valid data to store for {symbol}")
        return

    try:
        conn = psycopg.connect(DATABASE_URL)
        cur = conn.cursor()

        # Insert or get stock id
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
            result = cur.fetchone()
            if result:
                stock_id = result[0]
            else:
                print(f"❌ Could not find or create record for {symbol}")
                return

        # Insert price data
        inserted = 0
        for entry in stock_data["historical"]:
            cur.execute("""
                INSERT INTO stock_prices (stock_id, date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_id, date) DO NOTHING
            """, (
                stock_id,
                datetime.strptime(entry["date"], "%Y-%m-%d").date(),
                entry.get("open"),
                entry.get("high"),
                entry.get("low"),
                entry.get("close"),
                entry.get("volume", 0)
            ))
            inserted += 1

        conn.commit()
        print(f"💾 Stored {inserted} records for {symbol}")

    except Exception as e:
        print(f"❌ Database error for {symbol}: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()

# -------------------------------------------------------------------------
# 🧠 STEP 4: Fetch symbols from database
# -------------------------------------------------------------------------
def get_symbols_from_db():
    try:
        conn = psycopg.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT symbol FROM stocks ORDER BY symbol")
        rows = cur.fetchall()
        conn.close()
        if rows:
            return [r[0] for r in rows]
        else:
            print("⚠️ No symbols found in DB, using default list.")
            return ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA"]
    except Exception as e:
        print(f"❌ Error fetching symbols from DB: {e}")
        return ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA"]

# -------------------------------------------------------------------------
# 🚀 MAIN EXECUTION
# -------------------------------------------------------------------------
if __name__ == "__main__":
    print("🚀 Starting AlphaPredict Data Fetch Pipeline (FMP version)...\n")

    if not API_KEY:
        print("❌ Missing FMP_API_KEY in .env file")
        exit(1)

    if not DATABASE_URL:
        print("❌ Missing DATABASE_URL in .env file")
        exit(1)

    if not create_tables_if_not_exist():
        print("❌ Database setup failed. Exiting.")
        exit(1)

    symbols = get_symbols_from_db()
    print(f"📊 Found {len(symbols)} symbols to process: {symbols}")

    for i, symbol in enumerate(symbols, start=1):
        print(f"\n--- ({i}/{len(symbols)}) Processing {symbol} ---")
        data = fetch_stock_data(symbol)

        if data:
            store_stock_data(symbol, data)
        else:
            print(f"⚠️ Skipping {symbol}, no data returned.")

        # ⏳ FMP rate limit (avoid hitting free tier limit)
        print("🕒 Waiting 15 seconds before next request...")
        time.sleep(15)

    print("\n✅ All available stock data fetched successfully!")
