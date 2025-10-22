import os
import sys
import psycopg
import yfinance as yf  
import pandas as pd    
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import joblib
from pydantic import BaseModel
from datetime import timedelta, datetime, timezone # <-- MODIFICATION: Import datetime and timezone
from google.oauth2 import id_token as google_id_token
from google.auth.transport import requests as google_requests
import secrets
import auth
from psycopg_pool import ConnectionPool

ACCESS_TOKEN_EXPIRE_MINUTES = 30

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

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
        # --- FIX: Add prepare_threshold=None to the connection pool ---
        # This prevents the "prepared statement already exists" error on the live server.
        pool = ConnectionPool(conninfo=dsn, min_size=1, max_size=10, kwargs={'prepare_threshold': None})
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

# --- FIX: This function now accepts a 'conn' object ---
def store_stock_data(symbol, df, conn: psycopg.Connection):
    """Stores stock data from Yahoo Finance into the database."""
    if df is None or df.empty:
        return

    try:
        # It now uses the connection passed to it instead of creating a new one.
        with conn.cursor() as cur:
            cur.execute("INSERT INTO stocks (symbol) VALUES (%s) ON CONFLICT (symbol) DO NOTHING RETURNING id", (symbol,))
            result = cur.fetchone()
            
            # --- FIX: Correctly fetch the stock_id if it already exists ---
            if result:
                stock_id = result[0]
            else:
                # 1. Execute the query first
                cur.execute("SELECT id FROM stocks WHERE symbol = %s", (symbol,))
                # 2. Then fetch the result
                stock_id_result = cur.fetchone()
                if stock_id_result:
                    stock_id = stock_id_result[0]
                else:
                    # This case should ideally not happen, but it's good to handle it.
                    print(f"❌ Critical error: Could not find or create stock_id for {symbol}")
                    return 
            # --- END FIX ---

            for date, row in df.iterrows():
                cur.execute("""
                    INSERT INTO stock_prices (stock_id, date, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (stock_id, date) DO NOTHING
                """, (stock_id, date.date(), float(row["Open"]), float(row["High"]), float(row["Low"]), float(row["Close"]), int(row["Volume"]) if not pd.isna(row["Volume"]) else 0))
            
            conn.commit()
    except Exception as e:
        print(f"❌ Database error for {symbol}: {e}")
        conn.rollback() # Rollback on error

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
    
    # --- FIX STARTS HERE: Check if data is stale ---
    is_stale = False
    if data and data["prices"]:
        # Get the date of the most recent price entry in our database
        latest_db_date_str = data["prices"][0]["date"]
        latest_db_date = datetime.fromisoformat(latest_db_date_str).date()
        
        # Get today's date
        today = datetime.now(timezone.utc).date()
        
        # If the latest data we have is from before today, it's stale
        if latest_db_date < today:
            is_stale = True
            print(f"⚠️ Data for {symbol} is stale (latest: {latest_db_date}). Re-fetching...")
    # --- FIX ENDS HERE ---

    # If we have data and it's NOT stale, return it immediately.
    if data and not is_stale:
        print(f"✓ Found fresh data for {symbol} in DB.")
        return data
    
    # If data is missing OR stale, fetch new data.
    print(f"🔄 Fetching new/updated data for {symbol} from API...")
    new_stock_data_df = fetch_stock_data(symbol)
    
    if new_stock_data_df is None:
        # If fetching fails but we had stale data, it's better to return the old data than nothing.
        if data:
            print(f"⚠️ API fetch failed, returning stale data for {symbol}.")
            return data
        raise HTTPException(status_code=404, detail=f"Could not fetch data for '{symbol}'. It may be an invalid symbol.")
    
    # Store the newly fetched data. `store_stock_data` is smart and will only add new days.
    store_stock_data(symbol, new_stock_data_df, conn)
    
    # Re-query the database to get the complete, updated dataset.
    print(f"✓ Successfully cached and returning updated data for {symbol}.")
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
