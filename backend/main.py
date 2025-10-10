import os
import sys
import psycopg2
from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv

# --- This block is new ---
# Add the parent directory of 'data-pipeline' to the Python path
# This allows us to import from a sibling directory
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from data_pipeline.fetch_data import fetch_stock_data, store_stock_data
# --- End of new block ---

load_dotenv()
app = FastAPI()
DATABASE_URL = os.getenv("DATABASE_URL")

def get_db_connection():
    if not DATABASE_URL:
        raise Exception("DATABASE_URL environment variable not set.")
    try:
        return psycopg2.connect(DATABASE_URL)
    except psycopg2.OperationalError as e:
        print(f"FATAL: Could not connect to the database: {e}")
        raise

def query_stock_data(symbol: str):
    """Queries the database for a symbol and returns formatted data."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id FROM stocks WHERE symbol = %s", (symbol.upper(),))
        stock_record = cur.fetchone()

        if not stock_record:
            return None # Signal that the stock was not found

        stock_id = stock_record[0]
        cur.execute("SELECT date, open, high, low, close, volume FROM stock_prices WHERE stock_id = %s ORDER BY date DESC", (stock_id,))
        prices = cur.fetchall()
        cur.close()

        price_data = [{"date": r[0].isoformat(), "open": float(r[1]), "high": float(r[2]), "low": float(r[3]), "close": float(r[4]), "volume": int(r[5])} for r in prices]
        return {"symbol": symbol.upper(), "prices": price_data}
    finally:
        if conn:
            conn.close()

@app.get("/")
def read_root():
    return {"message": "Stock Prediction API is running."}

@app.get("/api/stocks/{symbol}")
def get_stock_prices(symbol: str):
    """
    Fetches historical price data for a stock.
    If not in the database, it fetches from the API, stores it, then returns it.
    """
    print(f"Received request for symbol: {symbol}")
    
    # 1. Try to get data from our database first (the cache)
    data = query_stock_data(symbol)
    
    if data:
        print(f"Found {symbol} in database. Returning cached data.")
        return data
        
    # 2. If not in DB, fetch from the external API
    print(f"'{symbol}' not in DB. Fetching from Alpha Vantage...")
    new_stock_data = fetch_stock_data(symbol)
    
    if not new_stock_data:
        raise HTTPException(status_code=404, detail=f"Could not retrieve data for '{symbol}' from external API. It may be an invalid symbol.")
        
    # 3. Store the new data in our database
    store_stock_data(symbol, new_stock_data)
    
    # 4. Now, query it from our database to ensure consistency and return it
    print(f"Successfully stored {symbol}. Now returning data from DB.")
    return query_stock_data(symbol)
