import os
import sys
import psycopg2
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import joblib
from dotenv import load_dotenv

# Allow importing from the root directory
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

def train_and_save_model():
    """Fetches all data, trains a simple model, and saves it."""
    print("Connecting to database to fetch training data...")
    conn = psycopg2.connect(DATABASE_URL)
    
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

    # Split data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Train a simple Linear Regression model
    print("Training Linear Regression model...")
    model = LinearRegression()
    model.fit(X_train, y_train)

    # Evaluate the model
    score = model.score(X_test, y_test)
    print(f"Model R^2 score: {score:.2f}")

    # Save the trained model to a file
    model_path = os.path.join(os.path.dirname(__file__), 'stock_predictor.joblib')
    joblib.dump(model, model_path)
    print(f"Model saved successfully to {model_path}")

if __name__ == "__main__":
    train_and_save_model()