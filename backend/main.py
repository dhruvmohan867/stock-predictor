import os
import sys
import psycopg
import pandas as pd
from fastapi import FastAPI, HTTPException, Depends, status, Form
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import joblib
from pydantic import BaseModel
from datetime import timedelta
from google.oauth2 import id_token as google_id_token
from google.auth.transport import requests as google_requests
import secrets
import auth

ACCESS_TOKEN_EXPIRE_MINUTES = 30

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from data_pipeline.fetch_data import fetch_stock_data, store_stock_data

load_dotenv()

def _build_dsn_from_pg_env():
    """Optionally build DSN from PG* env vars if DATABASE_URL is not set."""
    host = os.getenv("PGHOST")
    db   = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    pwd  = os.getenv("PGPASSWORD")
    port = os.getenv("PGPORT", "5432")
    if all([host, db, user, pwd]):
        return f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
    return None

def _normalize_dsn(dsn: str) -> str:
    """Ensure sslmode=require for Supabase and return normalized DSN."""
    if "supabase.co" in dsn and "sslmode=" not in dsn:
        sep = "&" if "?" in dsn else "?"
        dsn = f"{dsn}{sep}sslmode=require"
    return dsn

def get_db_connection():
    """Return a psycopg connection or raise HTTPException with a precise reason."""
    dsn = os.getenv("DATABASE_URL") or _build_dsn_from_pg_env()
    if not dsn:
        raise HTTPException(
            status_code=500,
            detail="DATABASE_URL not configured. Set a full Postgres URI."
        )
    dsn = _normalize_dsn(dsn)

    try:
        return psycopg.connect(dsn)
    except psycopg.OperationalError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database connection failed: {str(e)}"
        )

# Create FastAPI app
app = FastAPI()

# ✅ FIXED CORS CONFIGURATION - Remove all duplicate CORS code
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "https://stock-predictor-five-opal.vercel.app",  # Your production frontend
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# Load the trained model when the application starts
MODEL_PATH = os.path.join(os.path.dirname(__file__), 'ml_model', 'stock_predictor.joblib')

try:
    model = joblib.load(MODEL_PATH)
    print(f"✓ ML model loaded successfully from {MODEL_PATH}")
except FileNotFoundError:
    print(f"⚠️ Warning: Model file not found at {MODEL_PATH}")
    print("Predictions will not work until you train and save the model.")
    model = None
except Exception as e:
    print(f"⚠️ Error loading model: {e}")
    model = None

def query_stock_data(symbol: str):
    """Queries the database for a symbol and returns formatted data."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id FROM stocks WHERE symbol = %s", (symbol.upper(),))
        stock_record = cur.fetchone()

        if not stock_record:
            return None

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

    cur.execute("SELECT 1 FROM users WHERE username = %s OR email = %s", (username, email))
    if cur.fetchone():
        cur.close(); conn.close()
        raise HTTPException(status_code=400, detail="Username or email already registered")

    hashed_password = auth.get_password_hash(password)
    cur.execute(
        "INSERT INTO users (username, email, hashed_password) VALUES (%s, %s, %s)",
        (username, email, hashed_password)
    )
    conn.commit()
    cur.close(); conn.close()
    return {"message": f"User '{username}' registered successfully"}

class GoogleLoginRequest(BaseModel):
    credential: str

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
        username = username_base
        cur.execute("SELECT 1 FROM users WHERE username = %s", (username,))
        if cur.fetchone():
            username = f"{username_base}_{sub[:6]}"

        random_pwd_hash = auth.get_password_hash(secrets.token_urlsafe(16))
        cur.execute(
            "INSERT INTO users (username, email, hashed_password, google_id) VALUES (%s, %s, %s, %s) RETURNING username",
            (username, email, random_pwd_hash, sub)
        )
        username = cur.fetchone()[0]
        conn.commit()

    cur.close(); conn.close()

    access_token = auth.create_access_token(
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
    
    if not user or not auth.verify_password(form_data.password, user[1]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
        
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = auth.create_access_token(
        data={"sub": user[0]}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/api/stocks/{symbol}")
def get_stock_prices(symbol: str, current_user: str = Depends(auth.get_current_user)):
    """Fetches historical price data for a stock."""
    print(f"Request for {symbol} by authenticated user: {current_user}")
    
    data = query_stock_data(symbol)
    
    if data:
        print(f"Found {symbol} in database. Returning cached data.")
        return data
        
    print(f"'{symbol}' not in DB. Fetching from Alpha Vantage...")
    new_stock_data = fetch_stock_data(symbol)
    
    if not new_stock_data:
        raise HTTPException(status_code=404, detail=f"Could not retrieve data for '{symbol}' from external API.")
        
    store_stock_data(symbol, new_stock_data)
    
    print(f"Successfully stored {symbol}. Now returning data from DB.")
    return query_stock_data(symbol)

class PredictionRequest(BaseModel):
    symbol: str

@app.post("/api/predict")
def predict_stock_price(request: PredictionRequest, current_user: str = Depends(auth.get_current_user)):
    """Predicts the next day's closing price for a given stock symbol."""
    if model is None:
        raise HTTPException(
            status_code=503,
            detail="ML model not loaded. Please contact the administrator."
        )
    
    print(f"Prediction request for {request.symbol} by user {current_user}")
    
    data = query_stock_data(request.symbol)
    if not data or not data["prices"]:
        raise HTTPException(
            status_code=404, 
            detail=f"Not enough historical data to predict for {request.symbol}."
        )

    latest_features = data["prices"][0]

    try:
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

@app.get("/health/db")
def health_db():
    """Quick DB connectivity check."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.fetchone()
    cur.close(); conn.close()
    return {"ok": True}


