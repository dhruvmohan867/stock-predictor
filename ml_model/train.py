import os
import sys
import psycopg2
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
# --- FIX STARTS HERE ---
# Change from 'tensorflow.keras' to just 'keras'
from keras.models import Sequential
from keras.layers import LSTM, Dense, Dropout
# --- FIX ENDS HERE ---
import joblib
from dotenv import load_dotenv

# Allow importing from the root directory
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
LOOK_BACK_PERIOD = 60 # Use the last 60 days of data to predict the next day

def train_and_save_model():
    """Fetches all data, trains an LSTM model, and saves it."""
    print("Connecting to database to fetch training data...")
    conn = psycopg2.connect(DATABASE_URL)
    
    # Fetch all price data, focusing on the closing price
    df = pd.read_sql("SELECT date, close FROM stock_prices ORDER BY date", conn)
    conn.close()
    
    if len(df) < LOOK_BACK_PERIOD:
        print(f"Not enough data to train. Need at least {LOOK_BACK_PERIOD} records, but found {len(df)}.")
        return

    print(f"Fetched {len(df)} records for training.")
    
    # --- Data Preparation ---
    # 1. Scale the data (LSTMs work best with normalized data between 0 and 1)
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaled_data = scaler.fit_transform(df['close'].values.reshape(-1,1))

    # 2. Create sequences of data
    X_train, y_train = [], []
    for i in range(LOOK_BACK_PERIOD, len(scaled_data)):
        X_train.append(scaled_data[i-LOOK_BACK_PERIOD:i, 0])
        y_train.append(scaled_data[i, 0])
    
    X_train, y_train = np.array(X_train), np.array(y_train)
    
    # Reshape data for LSTM [samples, timesteps, features]
    X_train = np.reshape(X_train, (X_train.shape[0], X_train.shape[1], 1))

    # --- Build and Train the LSTM Model ---
    print("Building LSTM model...")
    model = Sequential([
        LSTM(units=50, return_sequences=True, input_shape=(X_train.shape[1], 1)),
        Dropout(0.2),
        LSTM(units=50, return_sequences=False),
        Dropout(0.2),
        Dense(units=25),
        Dense(units=1)
    ])
    
    model.compile(optimizer='adam', loss='mean_squared_error')
    
    print("Training model... (This may take a few minutes)")
    model.fit(X_train, y_train, batch_size=32, epochs=25)

    # --- Save the Model and the Scaler ---
    model_dir = os.path.dirname(__file__)
    model.save(os.path.join(model_dir, 'stock_predictor_lstm.keras'))
    joblib.dump(scaler, os.path.join(model_dir, 'data_scaler.joblib'))
    
    print(f"Model and scaler saved successfully in {model_dir}")

if __name__ == "__main__":
    train_and_save_model()