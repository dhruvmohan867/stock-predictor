import os
import sys
import psycopg2
import pandas as pd
from fastapi import FastAPI, HTTPException, Depends , status
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import joblib
from pydantic import BaseModel
from datetime import timedelta

# --- This block is new ---
# Add the parent directory of 'data-pipeline' to the Python path
# This allows us to import from a sibling directory
ACCESS_TOKEN_EXPIRE_MINUTES = 30

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from data_pipeline.fetch_data import fetch_stock_data, store_stock_data
# --- End of new block ---

# --- MODIFICATION START ---
from auth import get_password_hash, verify_password, create_access_token, get_current_user
# --- MODIFICATION END ---

load_dotenv()
app = FastAPI()

# --- FIX STARTS HERE ---

# 2. Add the CORS middleware configuration
origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://127.0.0.1:5500", # Common for VS Code Live Server
    "null" # Important for allowing local file:// access
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"], # Allows all methods (GET, POST, etc.)
    allow_headers=["*"], # Allows all headers
)

# --- FIX ENDS HERE ---

DATABASE_URL = os.getenv("DATABASE_URL")

# Load the trained model when the application starts
MODEL_PATH = os.path.join(os.path.dirname(__file__), '..', 'ml_model', 'stock_predictor.joblib')
model = joblib.load(MODEL_PATH)

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

@app.post("/register")
def register_user(form_data: OAuth2PasswordRequestForm = Depends()):
    """Handles user registration."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Check if user already exists
    cur.execute("SELECT id FROM users WHERE username = %s", (form_data.username,))
    if cur.fetchone():
        raise HTTPException(status_code=400, detail="Username already registered")
        
    hashed_password = get_password_hash(form_data.password)
    cur.execute(
        "INSERT INTO users (username, hashed_password) VALUES (%s, %s)",
        (form_data.username, hashed_password)
    )
    conn.commit()
    cur.close()
    conn.close()
    return {"message": f"User '{form_data.username}' registered successfully"}

@app.post("/token")
def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    """Handles user login and returns a JWT."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT username, hashed_password FROM users WHERE username = %s", (form_data.username,))
    user = cur.fetchone()
    cur.close()
    conn.close()
    
    if not user or not verify_password(form_data.password, user[1]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
        
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user[0]}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

# --- PROTECT THE ENDPOINT ---
@app.get("/api/stocks/{symbol}")
def get_stock_prices(symbol: str, current_user: str = Depends(get_current_user)):
    """
    Fetches historical price data for a stock.
    This endpoint is now protected and requires a valid token.
    """
    print(f"Request for {symbol} by authenticated user: {current_user}")
    
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

# --- Define the data model for the prediction input.
# This tells FastAPI what the request body should look like.
class StockFeatures(BaseModel):
    open: float
    high: float
    low: float
    close: float
    volume: int

@app.post("/api/predict")
def predict_stock_price(features: StockFeatures):
    """Predicts the next day's closing price based on current day's features."""
    try:
        # Create a DataFrame in the same format as the training data
        input_data = pd.DataFrame([features.dict()])
        prediction = model.predict(input_data)[0]
        return {"predicted_next_day_close": prediction}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction failed: {e}")


