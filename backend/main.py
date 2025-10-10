import os
import psycopg2
from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv

# Load environment variables from the .env file in the project root
# The .env file should be in the 'stock-predi' folder, not the 'backend' folder
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

# Initialize the FastAPI app
app = FastAPI()

# Get the database URL from environment variables
DATABASE_URL = os.getenv("DATABASE_URL")

def get_db_connection():
    """Establishes a connection to the database."""
    if not DATABASE_URL:
        raise Exception("DATABASE_URL environment variable not set.")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except psycopg2.OperationalError as e:
        print(f"FATAL: Could not connect to the database: {e}")
        raise

@app.get("/")
def read_root():
    """A simple root endpoint to confirm the API is running."""
    return {"message": "Stock Prediction API is running."}


@app.get("/api/stocks/{symbol}")
def get_stock_prices(symbol: str):
    """
    Fetches all historical price data for a given stock symbol
    from the database.
    """
    print(f"Received request for symbol: {symbol}")
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("SELECT id FROM stocks WHERE symbol = %s", (symbol.upper(),))
        stock_record = cur.fetchone()

        if not stock_record:
            raise HTTPException(status_code=404, detail=f"Stock symbol '{symbol}' not found in the database.")

        stock_id = stock_record[0]

        cur.execute("""
            SELECT date, open, high, low, close, volume 
            FROM stock_prices 
            WHERE stock_id = %s 
            ORDER BY date DESC
        """, (stock_id,))
        
        prices = cur.fetchall()
        
        price_data = [
            {
                "date": row[0].isoformat(),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": int(row[5])
            } for row in prices
        ]

        cur.close()
        return {"symbol": symbol.upper(), "prices": price_data}

    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        if conn:
            conn.close()
