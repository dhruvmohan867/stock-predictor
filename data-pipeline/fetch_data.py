import os
import requests
import psycopg2
from dotenv import load_dotenv
from datetime import datetime
load_dotenv()

API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

def create_tables_if_not_exist():
    """Create database tables if they don't exist"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
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

def fetch_stock_data(symbol="MSFT"):
    if not API_KEY:
        print("Error: API key not available")
        return None
        
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": API_KEY
    }
    
    try:
        print(f"Fetching data for {symbol}...")
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Check for API errors
        if "Error Message" in data:
            print(f"API Error: {data['Error Message']}")
            return None
        
        if "Note" in data:
            print(f"API Note: {data['Note']}")
            return None
        
        if "Time Series (Daily)" not in data:
            print("Error: No time series data in response")
            print("Response keys:", list(data.keys()))
            return None
            
        print(f"✓ Successfully fetched data for {symbol}")
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None

def store_stock_data(symbol, stock_data):
    if not stock_data or "Time Series (Daily)" not in stock_data:
        print("No valid stock data to store")
        return
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Insert or get stock info
        cur.execute("""
            INSERT INTO stocks (symbol, company_name) 
            VALUES (%s, %s) 
            ON CONFLICT (symbol) DO NOTHING
            RETURNING id
        """, (symbol, stock_data.get("Meta Data", {}).get("2. Symbol", symbol)))
        
        result = cur.fetchone()
        if result:
            stock_id = result[0]
        else:
            cur.execute("SELECT id FROM stocks WHERE symbol = %s", (symbol,))
            result = cur.fetchone()
            if result:
                stock_id = result[0]
            else:
                print(f"Error: Could not find or create stock record for {symbol}")
                return
        
        # Insert price data
        time_series = stock_data["Time Series (Daily)"]
        inserted_count = 0
        
        for date_str, prices in time_series.items():
            cur.execute("""
                INSERT INTO stock_prices (stock_id, date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_id, date) DO NOTHING
            """, (
                stock_id,
                datetime.strptime(date_str, "%Y-%m-%d").date(),
                float(prices["1. open"]),
                float(prices["2. high"]),
                float(prices["3. low"]),
                float(prices["4. close"]),
                int(prices["5. volume"])
            ))
            inserted_count += 1
        
        conn.commit()
        print(f"✓ Successfully stored {inserted_count} price records for {symbol}")
        
    except Exception as e:
        print(f"Database error: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    print("Starting stock data pipeline...")
    
    # Check environment variables
    if not API_KEY:
        print("Error: ALPHA_VANTAGE_API_KEY not found in environment variables")
        exit(1)
    
    if not DATABASE_URL:
        print("Error: DATABASE_URL not found in environment variables")
        exit(1)
    
    print(f"✓ API Key loaded: {API_KEY[:8]}...")
    print(f"✓ Database URL loaded")
    
    # Create tables if they don't exist
    if not create_tables_if_not_exist():
        print("Failed to set up database tables")
        exit(1)
    
    # --- MODIFICATION START ---

    # 1. Define a list of companies you want to track
    stocks_to_fetch = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA"]
    print(f"Attempting to process symbols: {stocks_to_fetch}")

    # 2. Loop through each symbol in the list
    for symbol in stocks_to_fetch:
        print(f"\n--- Processing {symbol} ---")
        
        # 3. Fetch and store data for the current symbol
        stock_data = fetch_stock_data(symbol)
        if stock_data:
            store_stock_data(symbol, stock_data)
        else:
            # Add a more informative message here
            print(f"Could not fetch data for {symbol}. This may be due to an invalid symbol or API rate limits.")
    
    print("\nPipeline finished.")
