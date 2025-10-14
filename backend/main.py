import os
import sys
import psycopg2
import pandas as pd
from fastapi import FastAPI, HTTPException, Depends , status, Form
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import joblib
from pydantic import BaseModel
from datetime import timedelta
from google.oauth2 import id_token as google_id_token
from google.auth.transport import requests as google_requests
import secrets

# --- This block is new ---
# Add the parent directory of 'data-pipeline' to the Python path
# This allows us to import from a sibling directory
ACCESS_TOKEN_EXPIRE_MINUTES = 30

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from data_pipeline.fetch_data import fetch_stock_data, store_stock_data
# --- End of new block ---

# --- MODIFICATION START ---
from .auth import get_password_hash, verify_password, create_access_token, get_current_user
# --- MODIFICATION END ---

load_dotenv()
app = FastAPI()

# --- FIX STARTS HERE ---

# 2. Add the CORS middleware configuration
origins = [
    "http://localhost",
    "http://localhost:3000",
    "http://localhost:5173",
    "http://127.0.0.1:5500",
    "https://your-frontend-name.onrender.com",
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
def register_user(
    username: str = Form(...),
    password: str = Form(...),
    email: str = Form(...)
):
    """User registration with email."""
    conn = get_db_connection()
    cur = conn.cursor()

    # Check uniqueness
    cur.execute("SELECT 1 FROM users WHERE username = %s OR email = %s", (username, email))
    if cur.fetchone():
        cur.close(); conn.close()
        raise HTTPException(status_code=400, detail="Username or email already registered")

    hashed_password = get_password_hash(password)
    cur.execute(
        "INSERT INTO users (username, email, hashed_password) VALUES (%s, %s, %s)",
        (username, email, hashed_password)
    )
    conn.commit()
    cur.close(); conn.close()
    return {"message": f"User '{username}' registered successfully"}

# --- Google Sign-In ---
class GoogleLoginRequest(BaseModel):
    credential: str  # Google ID token (JWT) from frontend

@app.post("/google-login")
def google_login(payload: GoogleLoginRequest):
    CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
    if not CLIENT_ID:
        raise HTTPException(status_code=500, detail="GOOGLE_CLIENT_ID not configured")

    try:
        idinfo = google_id_token.verify_oauth2_token(
            payload.credential,
            google_requests.Request(),
            CLIENT_ID
        )
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid Google token")

    email = idinfo.get("email")
    sub = idinfo.get("sub")
    username_base = (idinfo.get("name") or email.split("@")[0]).replace(" ", "_")

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT username FROM users WHERE email = %s", (email,))
    row = cur.fetchone()

    if row:
        username = row[0]
    else:
        # Ensure unique username
        username = username_base
        cur.execute("SELECT 1 FROM users WHERE username = %s", (username,))
        if cur.fetchone():
            username = f"{username_base}_{sub[:6]}"

        # Store random password hash (not used by Google users)
        random_pwd_hash = get_password_hash(secrets.token_urlsafe(16))
        cur.execute(
            "INSERT INTO users (username, email, hashed_password, google_id) VALUES (%s, %s, %s, %s) RETURNING username",
            (username, email, random_pwd_hash, sub)
        )
        username = cur.fetchone()[0]
        conn.commit()

    cur.close(); conn.close()

    access_token = create_access_token(
        data={"sub": username},
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    return {"access_token": access_token, "token_type": "bearer", "username": username}

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

# --- FIX STARTS HERE ---

# 1. Define a new Pydantic model for the prediction request.
#    The frontend will send a JSON like: {"symbol": "AAPL"}
class PredictionRequest(BaseModel):
    symbol: str

# 2. Replace the entire old /api/predict endpoint with this new, secure version.
@app.post("/api/predict")
def predict_stock_price(request: PredictionRequest, current_user: str = Depends(get_current_user)):
    """
    Predicts the next day's closing price for a given stock symbol
    using the last 60 days of data. This endpoint is now protected.
    """
    # This logic is designed for an LSTM or time-series model that needs recent history.
    # For a simple Linear Regression model, this is overkill, but it aligns with a more advanced setup.
    
    print(f"Prediction request for {request.symbol} by user {current_user}")
    
    # Fetch the most recent day's data to use for prediction
    data = query_stock_data(request.symbol)
    if not data or not data["prices"]:
        raise HTTPException(
            status_code=404, 
            detail=f"Not enough historical data to predict for {request.symbol}."
        )

    # Get the latest day's features
    latest_features = data["prices"][0] # The query is ordered by date DESC

    try:
        # Create a DataFrame in the same format as the training data
        # Note: The 'date' field is not used by the model but is part of the dictionary.
        input_data = pd.DataFrame([{
            "open": latest_features["open"],
            "high": latest_features["high"],
            "low": latest_features["low"],
            "close": latest_features["close"],
            "volume": latest_features["volume"]
        }])
        
        prediction = model.predict(input_data)[0]
        return {"symbol": request.symbol, "predicted_next_day_close": float(prediction)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction failed: {e}")


