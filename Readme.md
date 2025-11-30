# 📈 AlphaPredict: Stock Analysis & Prediction Platform

A full-stack web application that fetches daily stock data, stores it in a PostgreSQL database, and uses a Gradient Boosting model to predict next-day closing prices. Built with FastAPI, React, and LightGBM.

![Stock Predictor Dashboard](./frontend/public/Dashboard.png)

## ✨ Features

- 🚀 **High-Performance Backend**: Built with FastAPI, serving data asynchronously.
- 🧠 **Advanced ML Predictions**: Uses a **LightGBM (Gradient Boosting)** model for higher accuracy predictions.
- 🔄 **Automated Data Pipeline**: A **GitHub Actions** workflow fetches daily data for over 1,000 stocks from US (S&P 500) and Indian (NIFTY 500) markets using `yfinance`.
- 📊 **Interactive Dashboard**: A modern, responsive UI built with React and Vite, featuring interactive charts (Recharts) and a dark mode.
- 💾 **Robust & Scalable Database**: Uses PostgreSQL hosted on Supabase for reliable, persistent data storage.
- ☁️ **Cloud-Native Architecture**: Deploys separate components to the best-suited platforms (Vercel, Render, GitHub Actions) for optimal performance and scalability.

## 🚀 Tech Stack

| Component | Technology |
|-----------|------------|
| **Frontend** | React 19, Vite, TailwindCSS, Recharts, Framer Motion |
| **Backend** | FastAPI, Python 3.11, Uvicorn |
| **Database** | PostgreSQL (hosted on Supabase) |
| **ML Model** | **LightGBM** (Gradient Boosting), Scikit-learn, Joblib |
| **Data Pipeline** | **GitHub Actions**, Python, Pandas, `yfinance` |
| **Authentication** | (Helpers in place for JWT & bcrypt) |

## 📦 Project Structure

```
stock-predi/
├── .github/workflows/
│   └── daily_data_fetch.yml # Automated daily data collection workflow
├── backend/
│   ├── main.py              # FastAPI app, API endpoints
│   ├── ml_model/            # Copy of the deployed model
│   └── requirements.txt     # Backend Python dependencies
├── data_pipeline/
│   ├── fetch_data.py        # Script to fetch data from yfinance
│   └── db_setup.sql         # Initial database schema
├── frontend/
│   ├── src/
│   │   └── components/
│   │       └── Dashboard.jsx  # The main UI component
│   └── package.json         # Frontend Node.js dependencies
├── ml_model/
│   ├── train.py             # Script to train the LightGBM model
│   └── stock_predictor.joblib # The trained model artifact
├── Dockerfile                 # Containerizes the backend for deployment
└── README.md
```

## 🛠️ Local Development Setup

### Prerequisites
- Python 3.11+
- Node.js 18+
- A PostgreSQL database (e.g., via Supabase)

### Environment Setup
This project uses environment variables for configuration. For local development, you can set them in your shell before running the application.

**Required Variables:**
- `DATABASE_URL`: Your full PostgreSQL connection string.
- `SECRET_KEY`: A long, random string for signing tokens.
- `GOOGLE_CLIENT_ID`: Your Google OAuth Client ID.

### Backend Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/dhruvmohan867/stock-predictor.git
   cd stock-predi
   ```

2. **Create and activate virtual environment**
   ```bash
   python -m venv venv
   # Windows
   .\venv\Scripts\activate
   # macOS/Linux
   source venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r backend/requirements.txt
   pip install -r data_pipeline/requirements.txt
   ```

4. **Set up database**
   Run the SQL schema from `data_pipeline/db_setup.sql` on your PostgreSQL instance to create the necessary tables.

5. **Fetch initial stock data**
   *(Ensure `DATABASE_URL` is set in your shell)*
   ```bash
   python data_pipeline/fetch_data.py
   ```

6. **Train the ML model**
   *(This will create the `stock_predictor.joblib` file)*
   ```bash
   python ml_model/train.py
   ```

7. **Start the API server**
   ```bash
   uvicorn backend.main:app --reload
   ```
   The backend will be available at `http://127.0.0.1:8000`.

### Frontend Setup

1. **Navigate to frontend directory**
   ```bash
   cd frontend
   ```

2. **Install dependencies**
   ```bash
   npm install
   ```

3. **Configure environment variables**
   For the frontend, Vite uses a `.env` file. Create `frontend/.env` with the following content:
   ```env
   VITE_API_BASE=http://127.0.0.1:8000
   VITE_GOOGLE_CLIENT_ID=your_google_client_id
   ```

4. **Start development server**
   ```bash
   npm run dev
   ```
   The frontend will be available at `http://localhost:5173`.

## 🌐 Deployment

This project is deployed as three separate services.

### 1. Data Pipeline (GitHub Actions)
The pipeline runs automatically on a schedule. It requires the `DATABASE_URL` to be set as a repository secret in GitHub.
1. In your GitHub repo, go to **Settings > Secrets and variables > Actions**.
2. Create a new repository secret named `DATABASE_URL` and paste your Supabase connection string.

### 2. Backend (Render)
1. Create a new **Web Service** on Render.
2. Connect your GitHub repository.
3. Configure:
   - **Root Directory**: `backend`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn main:app --host 0.0.0.0 --port $PORT`
4. Add your `DATABASE_URL`, `SECRET_KEY`, and `GOOGLE_CLIENT_ID` as environment variables in the Render dashboard.
5. Deploy and note your API URL.

### 3. Frontend (Vercel)
1. Import your repository on Vercel.
2. Configure:
   - **Framework Preset**: Vite
   - **Root Directory**: `frontend`
3. Add environment variables:
   - `VITE_API_BASE`: Your backend URL from Render.
   - `VITE_GOOGLE_CLIENT_ID`: Your Google Client ID.
4. Deploy.

## 📖 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Health check for the API server. |
| GET | `/api/stocks/{symbol}` | Get historical price data for a stock. |
| GET | `/api/symbols` | Get a list of all available stock symbols. |
| POST | `/api/predict` | Predict the next-day closing price for a stock. |
| GET | `/health/db` | Check the database connection status. |

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.