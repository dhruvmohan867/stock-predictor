import os
import sys
import psycopg
import pandas as pd
from fastapi import FastAPI, HTTPException, Depends, status
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
from psycopg_pool import ConnectionPool

ACCESS_TOKEN_EXPIRE_MINUTES = 30

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from data_pipeline.fetch_data import fetch_stock_data, store_stock_data

load_dotenv()

# --- DATABASE CONNECTION POOL ---
pool = None

def _normalize_dsn(dsn: str) -> str:
    if "supabase.co" in dsn and "sslmode=" not in dsn:
        return f"{dsn}{'?' if '?' not in dsn else '&'}sslmode=require"
    return dsn

def get_pool():
    global pool
    if pool is None:
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            raise RuntimeError("DATABASE_URL not configured.")
        dsn = _normalize_dsn(dsn)
        pool = ConnectionPool(conninfo=dsn, min_size=1, max_size=10)
        print("✓ Database connection pool initialized.")
    return pool

def get_db_connection():
    db_pool = get_pool()
    with db_pool.connection() as conn:
        yield conn

# --- FASTAPI APP SETUP ---
app = FastAPI()

@app.on_event("startup")
async def startup_event():
    get_pool()

@app.on_event("shutdown")
async def shutdown_event():
    global pool
    if pool:
        pool.close()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "https://stock-predictor-five-opal.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- LOAD MODEL ---
MODEL_PATH = os.path.join(os.path.dirname(__file__), 'ml_model', 'stock_predictor.joblib')
model = None
try:
    model = joblib.load(MODEL_PATH)
    print(f"✓ ML model loaded successfully from {MODEL_PATH}")
except Exception as e:
    print(f"⚠️ Error loading model: {e}")

# --- SCHEMAS ---
class UserRegister(BaseModel):
    username: str
    password: str
    email: str

class GoogleLoginRequest(BaseModel):
    credential: str

class PredictionRequest(BaseModel):
    symbol: str

# --- HELPER: QUERY STOCK DATA ---
def query_stock_data(symbol: str, conn: psycopg.Connection):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM stocks WHERE symbol = %s", (symbol.upper(),))
        stock_record = cur.fetchone()
        if not stock_record:
            return None
        stock_id = stock_record[0]
        cur.execute("SELECT date, open, high, low, close, volume FROM stock_prices WHERE stock_id = %s ORDER BY date DESC", (stock_id,))
        prices = cur.fetchall()
        return {"symbol": symbol.upper(), "prices": [{"date": r[0].isoformat(), "open": float(r[1]), "high": float(r[2]), "low": float(r[3]), "close": float(r[4]), "volume": int(r[5])} for r in prices]}

# --- ROUTES ---
@app.get("/")
def read_root():
    return {"message": "Stock Prediction API is running."}

@app.post("/register")
def register_user(user: UserRegister, conn: psycopg.Connection = Depends(get_db_connection)):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM users WHERE username = %s OR email = %s", (user.username, user.email))
        if cur.fetchone():
            raise HTTPException(status_code=400, detail="Username or email already registered")
        hashed_password = auth.get_password_hash(user.password)
        cur.execute("INSERT INTO users (username, email, hashed_password) VALUES (%s, %s, %s)", (user.username, user.email, hashed_password))
        conn.commit()
    return {"message": f"User '{user.username}' registered successfully"}

@app.post("/token")
def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(), conn: psycopg.Connection = Depends(get_db_connection)):
    with conn.cursor() as cur:
        cur.execute("SELECT username, hashed_password FROM users WHERE username = %s", (form_data.username,))
        user = cur.fetchone()
    if not user or not auth.verify_password(form_data.password, user[1]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username or password")
    access_token = auth.create_access_token(data={"sub": user[0]}, expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/google-login")
def google_login(payload: GoogleLoginRequest, conn: psycopg.Connection = Depends(get_db_connection)):
    CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
    try:
        idinfo = google_id_token.verify_oauth2_token(payload.credential, google_requests.Request(), CLIENT_ID)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid Google token")
    email, sub = idinfo.get("email"), idinfo.get("sub")
    username_base = (idinfo.get("name") or email.split("@")[0]).replace(" ", "_")
    with conn.cursor() as cur:
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
            cur.execute("INSERT INTO users (username, email, hashed_password, google_id) VALUES (%s, %s, %s, %s) RETURNING username", (username, email, random_pwd_hash, sub))
            username = cur.fetchone()[0]
            conn.commit()
    access_token = auth.create_access_token(data={"sub": username}, expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    return {"access_token": access_token, "token_type": "bearer", "username": username}

@app.get("/api/stocks/{symbol}")
def get_stock_prices(symbol: str, conn: psycopg.Connection = Depends(get_db_connection), current_user: str = Depends(auth.get_current_user)):
    # --- HYBRID STRATEGY ---
    # 1. First, try to get data from our own database (fast).
    data = query_stock_data(symbol, conn)
    
    # 2. If data is found, return it immediately.
    if data:
        print(f"✓ Found cached data for {symbol} in DB.")
        return data
    
    # 3. If not in DB, fetch it live from the API (slower, but dynamic).
    print(f"⚠️ Data for {symbol} not in cache. Fetching from API...")
    new_stock_data = fetch_stock_data(symbol)
    
    # If the API call fails (e.g., invalid symbol or rate limit), raise an error.
    if not new_stock_data:
        raise HTTPException(status_code=404, detail=f"Could not fetch data for '{symbol}'. It may be an invalid symbol or the API limit was reached.")
    
    # 4. Store the newly fetched data in our database for future requests.
    store_stock_data(symbol, new_stock_data)
    
    # 5. Return the newly cached data to the user.
    print(f"✓ Successfully cached and returning data for {symbol}.")
    return query_stock_data(symbol, conn)

@app.post("/api/predict")
def predict_stock_price(request: PredictionRequest, conn: psycopg.Connection = Depends(get_db_connection), current_user: str = Depends(auth.get_current_user)):
    if model is None:
        raise HTTPException(status_code=503, detail="ML model not loaded on server")
    data = query_stock_data(request.symbol, conn)
    if not data or not data["prices"]:
        raise HTTPException(status_code=404, detail=f"Not enough historical data for {request.symbol}")
    try:
        latest = data["prices"][0]
        input_data = pd.DataFrame([{"open": latest["open"], "high": latest["high"], "low": latest["low"], "close": latest["close"], "volume": latest["volume"]}])
        prediction = model.predict(input_data)[0]
        return {"symbol": request.symbol, "predicted_next_day_close": float(prediction)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction failed: {e}")

@app.get("/health/db")
def health_db(conn: psycopg.Connection = Depends(get_db_connection)):
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        cur.fetchone()
    return {"ok": True}
