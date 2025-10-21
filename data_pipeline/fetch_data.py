import os
import requests
import psycopg
from dotenv import load_dotenv
from datetime import datetime
load_dotenv()

# --- MODIFICATION: Use the new FMP API Key ---
API_KEY = os.getenv("FMP_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

def create_tables_if_not_exist():
    """Create database tables if they don't exist"""
    try:
        conn = psycopg.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Create stocks table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stocks (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) UNIQUE NOT NULL,
                company_name VARCHAR(100),
                sector VARCHAR(50)
            )
        """)
        
        # Create stock_prices table
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
        print("✓ Database tables ready")
        
    except Exception as e:
        print(f"Error setting up tables: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()
    return True

# --- MODIFICATION: This function is completely replaced ---
def fetch_stock_data(symbol="MSFT"):
    """Fetches daily stock data from FinancialModelingPrep API."""
    if not API_KEY:
        print("Error: FMP_API_KEY not available in environment variables.")
        return None
        
    # FMP's endpoint for historical daily prices
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol.upper()}"
    params = { "apikey": API_KEY }
    
    try:
        print(f"Fetching data for {symbol} from FMP...")
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # FMP returns an empty object for invalid symbols
        if not data or "historical" not in data:
            print(f"API Error: No data returned for {symbol}. It may be an invalid symbol.")
            return None
            
        print(f"✓ Successfully fetched data for {symbol} from FMP")
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None

# --- MODIFICATION: This function is updated to parse the FMP response ---
def store_stock_data(symbol, stock_data):
    """Stores stock data from FMP into the database."""
    if not stock_data or "historical" not in stock_data:
        print("No valid stock data to store.")
        return
    
    try:
        conn = psycopg.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Insert or get stock info
        cur.execute("""
            INSERT INTO stocks (symbol) VALUES (%s) 
            ON CONFLICT (symbol) DO NOTHING RETURNING id
        """, (stock_data.get("symbol", symbol),))
        
        result = cur.fetchone()
        if result:
            stock_id = result[0]
        else:
            cur.execute("SELECT id FROM stocks WHERE symbol = %s", (stock_data.get("symbol", symbol),))
            stock_id = cur.fetchone()[0]
        
        # Insert price data
        time_series = stock_data["historical"]
        inserted_count = 0
        
        for prices in time_series:
            # FMP uses different key names than Alpha Vantage
            cur.execute("""
                INSERT INTO stock_prices (stock_id, date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_id, date) DO NOTHING
            """, (
                stock_id,
                datetime.strptime(prices["date"], "%Y-%m-%d").date(),
                float(prices["open"]),
                float(prices["high"]),
                float(prices["low"]),
                float(prices["close"]),
                int(prices["volume"])
            ))
            inserted_count += cur.rowcount
        
        conn.commit()
        print(f"✓ Successfully stored/updated {inserted_count} price records for {symbol}")
        
    except Exception as e:
        print(f"Database error: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    print("Starting stock data pipeline...")
    
    if not API_KEY or not DATABASE_URL:
        print("Error: Environment variables (API_KEY, DATABASE_URL) not set.")
        exit(1)
    
    if not create_tables_if_not_exist():
        print("Failed to set up database tables.")
        exit(1)
    
    # This list is now for pre-populating the DB with popular stocks.
    # The backend will handle fetching any other stock on-demand.
    stocks_to_prepopulate = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA"]
    
    print(f"Pre-populating database with {len(stocks_to_prepopulate)} symbols...")
    for symbol in stocks_to_prepopulate:
        print(f"\n--- Processing {symbol} ---")
        stock_data = fetch_stock_data(symbol)
        if stock_data:
            store_stock_data(symbol, stock_data)
        else:
            print(f"Could not fetch data for {symbol}. This may be due to an invalid symbol or API rate limits.")
    
    print("\nPipeline finished.")
