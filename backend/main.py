import os
import sys
import psycopg
import yfinance as yf  # <-- ADD: Import yfinance
import pandas as pd    # <-- ADD: Import pandas
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

# --- NEW: Add the exact same data fetching functions from your pipeline ---

def fetch_stock_data(symbol):
    """Fetches daily stock data from Yahoo Finance."""
    try:
        print(f"🔄 Fetching data for {symbol} from Yahoo Finance...")
        ticker = yf.Ticker(symbol)
        data = ticker.history(period="1y")
        if data.empty:
            print(f"⚠️ No data found for {symbol}")
            return None
        print(f"✅ Data fetched successfully for {symbol}")
        return data
    except Exception as e:
        print(f"❌ Error fetching {symbol}: {e}")
        return None

def store_stock_data(symbol, df):
    """Stores stock data from Yahoo Finance into the database."""
    if df is None or df.empty:
        return

    try:
        conn = psycopg.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("INSERT INTO stocks (symbol) VALUES (%s) ON CONFLICT (symbol) DO NOTHING RETURNING id", (symbol,))
        result = cur.fetchone()
        stock_id = result[0] if result else cur.execute("SELECT id FROM stocks WHERE symbol = %s", (symbol,)).fetchone()[0]

        for date, row in df.iterrows():
            cur.execute("""
                INSERT INTO stock_prices (stock_id, date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (stock_id, date) DO NOTHING
            """, (stock_id, date.date(), float(row["Open"]), float(row["High"]), float(row["Low"]), float(row["Close"]), int(row["Volume"]) if not pd.isna(row["Volume"]) else 0))
        
        conn.commit()
    except Exception as e:
        print(f"❌ Database error for {symbol}: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

# --- END of new functions ---


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
    data = query_stock_data(symbol, conn)
    
    if data:
        print(f"✓ Found cached data for {symbol} in DB.")
        return data
    
    print(f"⚠️ Data for {symbol} not in cache. Fetching from API...")
    new_stock_data_df = fetch_stock_data(symbol) # This now calls the yfinance function
    
    if new_stock_data_df is None:
        raise HTTPException(status_code=404, detail=f"Could not fetch data for '{symbol}'. It may be an invalid symbol.")
    
    store_stock_data(symbol, new_stock_data_df)
    
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
