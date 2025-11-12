import os
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
API_BASE_URL = os.getenv("RENDER_EXTERNAL_URL", "https://stock-predictor-ujiu.onrender.com") 
REFRESH_SECRET = os.getenv("REFRESH_SECRET")

def run_full_refresh():
    """Calls the secure streaming endpoint to trigger a full data refresh and print logs."""
    if not REFRESH_SECRET:
        print("❌ Error: REFRESH_SECRET is not set in your .env file.")
        return

    # MODIFIED: Use the new streaming endpoint
    endpoint = f"{API_BASE_URL}/internal/refresh-all-stream"
    params = {"secret": REFRESH_SECRET}

    print(f"▶️  Connecting to stream logs from {API_BASE_URL}...")

    try:
        # MODIFIED: Use stream=True to handle the response as a stream
        with requests.post(endpoint, params=params, stream=True, timeout=1800) as response:
            if response.status_code == 200:
                print("✅ Connection successful. Streaming logs:\n")
                # Iterate over the response line by line and print to the terminal
                for line in response.iter_lines():
                    if line:
                        print(line.decode('utf-8'))
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