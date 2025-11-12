import os
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
# MODIFIED: Use the Render environment variable if available, otherwise use the correct hardcoded URL.
API_BASE_URL = os.getenv("RENDER_EXTERNAL_URL", "https://stock-predictor-ujiu.onrender.com") 
REFRESH_SECRET = os.getenv("REFRESH_SECRET")

def run_full_refresh():
    """Calls the secure endpoint to trigger a full data refresh."""
    if not REFRESH_SECRET:
        print("❌ Error: REFRESH_SECRET is not set in your .env file.")
        return

    endpoint = f"{API_BASE_URL}/internal/refresh-all"
    params = {"secret": REFRESH_SECRET}

    print(f"▶️  Sending request to trigger full data refresh at {API_BASE_URL}...")

    try:
        response = requests.post(endpoint, params=params, timeout=30)
        
        if response.status_code == 200:
            print("✅ Success! Server responded:")
            print(f"   {response.json().get('message')}")
            print("\nℹ️  You can monitor the backend server logs to see the progress.")
        else:
            print(f"❌ Error: Server returned status code {response.status_code}")
            try:
                print(f"   Response: {response.json()}")
            except requests.exceptions.JSONDecodeError:
                print(f"   Response: {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Critical Error: Could not connect to the server.")
        print(f"   Details: {e}")
        print("\n   Please ensure your backend server is running and accessible at the specified URL.")

if __name__ == "__main__":
    run_full_refresh()