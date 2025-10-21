import os
import sys
import psycopg  
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
from dotenv import load_dotenv

# Allow importing from the root directory
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

def train_and_save_model():
    """Fetches all data, trains a simple model, and saves it."""
    print("Connecting to database to fetch training data...")
    conn = psycopg.connect(DATABASE_URL)  # <-- CHANGE: Use psycopg connect
    
    # Fetch all price data from the database
    df = pd.read_sql("SELECT date, open, high, low, close, volume FROM stock_prices ORDER BY date", conn)
    conn.close()
    
    if df.empty:
        print("No data found in the database. Cannot train model.")
        return

    print(f"Fetched {len(df)} records for training.")
    
    # --- Feature Engineering (Simple Example) ---
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    df['target'] = df['close'].shift(-1) # Predict next day's close
    df.dropna(inplace=True)

    # Define features (X) and target (y)
    features = ['open', 'high', 'low', 'close', 'volume']
    X = df[features]
    y = df['target']

    split_index = int(len(df) * 0.8)
    X_train, y_train = X[:split_index], y[:split_index]
    X_test, y_test = X[split_index:], y[split_index:]
    # Train a simple Linear Regression model
    print("Training Linear Regression model...")
    model = LinearRegression()
    model.fit(X_train, y_train)

    # --- FIX STARTS HERE: Comprehensive Model Evaluation ---

    # 3. Make predictions on the test set (the "future" data)
    y_pred = model.predict(X_test)

    # 4. Calculate and print multiple performance metrics
    print("\n--- Model Evaluation ---")
    r2 = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    rmse = np.sqrt(mse)

    print(f"R-squared (R²): {r2:.4f}")
    print(f"Mean Absolute Error (MAE): ${mae:.2f}")
    print(f"Root Mean Squared Error (RMSE): ${rmse:.2f}")
    print("------------------------\n")

    # --- FIX ENDS HERE ---

    # Save the trained model to a file
    model_path = os.path.join(os.path.dirname(__file__), 'stock_predictor.joblib')
    joblib.dump(model, model_path)
    print(f"Model saved successfully to {model_path}")

if __name__ == "__main__":
    train_and_save_model()