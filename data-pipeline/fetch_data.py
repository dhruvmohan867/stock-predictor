import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

# Example: Fetch daily stock prices for Microsoft (MSFT)
def fetch_stock_data(symbol="MSFT"):  # Fixed: colon instead of semicolon
    url = "https://www.alphavantage.co/query"  # Fixed: removed curly brace
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": API_KEY
    }  # Fixed: proper indentation and removed comma
    
    response = requests.get(url, params=params)
    data = response.json()
    return data

if __name__ == "__main__":
    stock_data = fetch_stock_data("AAPL")  # Example: Apple stock
    print(stock_data)