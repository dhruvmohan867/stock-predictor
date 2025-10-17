# 📈 Stock Price Predictor

A full-stack web application that fetches real-time stock data, stores it in a PostgreSQL database, and uses machine learning to predict next-day closing prices. Built with FastAPI, React, and scikit-learn.

![Stock Predictor Dashboard](./frontend/public/Dashboard.png)

## ✨ Features

- 🔐 **User Authentication**: Sign up/sign in with email & password or Google OAuth
- 📊 **Real-Time Stock Data**: Fetches historical prices from Alpha Vantage API
- 🤖 **ML Predictions**: Linear regression model predicts next-day closing prices
- 🎨 **Modern UI**: Dark-themed dashboard with interactive charts (Recharts)
- 🔒 **Secure**: JWT-based authentication, bcrypt password hashing
- 💾 **Persistent Storage**: PostgreSQL database hosted on Supabase

## 🚀 Tech Stack

**Frontend**
- React 19 + Vite
- TailwindCSS for styling
- Recharts for data visualization
- Framer Motion for animations
- Lucide React for icons

**Backend**
- FastAPI (Python)
- PostgreSQL (Supabase)
- scikit-learn for ML
- passlib + python-jose for auth
- Google OAuth2 integration

## 📦 Project Structure

```
stock-predi/
├── backend/
│   ├── main.py              # FastAPI app + endpoints
│   ├── auth.py              # JWT & password utilities
│   └── requirements.txt     # Python dependencies
├── frontend/
│   ├── src/
│   │   ├── components/
│   │   │   ├── AuthForm.jsx      # Login/signup UI
│   │   │   └── Dashboard.jsx     # Main dashboard
│   │   ├── App.jsx
│   │   └── main.jsx
│   ├── package.json
│   └── .env                 # VITE_API_BASE, VITE_GOOGLE_CLIENT_ID
├── data_pipeline/
│   ├── fetch_data.py        # Alpha Vantage data ingestion
│   └── db_setup.sql         # Database schema
├── ml_model/
│   ├── train.py             # Model training script
│   └── stock_predictor.joblib  # Trained model
├── .env                      # Backend secrets (not in git)
├── .gitignore
├── LICENSE
└── README.md
```

## 🛠️ Local Development Setup

### Prerequisites
- Python 3.11+
- Node.js 18+
- PostgreSQL (or Supabase account)
- Alpha Vantage API key (free at [alphavantage.co](https://www.alphavantage.co))
- Google OAuth client ID (optional, for Google sign-in)

### Backend Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/stock-predi.git
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
   ```

4. **Configure environment variables**
   Create `.env` in the project root:
   ```env
   DATABASE_URL=postgresql://user:pass@host:5432/dbname?sslmode=require
   ALPHA_VANTAGE_API_KEY=your_api_key
   SECRET_KEY=your_secret_key_here
   GOOGLE_CLIENT_ID=your_google_client_id
   ```

5. **Set up database**
   Run the SQL schema in `data_pipeline/db_setup.sql` on your PostgreSQL instance.

6. **Fetch initial stock data**
   ```bash
   python data_pipeline/fetch_data.py
   ```

7. **Train the ML model**
   ```bash
   python ml_model/train.py
   ```

8. **Start the API server**
   ```bash
   uvicorn backend.main:app --reload
   ```
   Backend runs at `http://127.0.0.1:8000`

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
   Create `frontend/.env`:
   ```env
   VITE_API_BASE=http://127.0.0.1:8000
   VITE_GOOGLE_CLIENT_ID=your_google_client_id
   ```

4. **Start development server**
   ```bash
   npm run dev
   ```
   Frontend runs at `http://localhost:5173`

## 🌐 Deployment

### Backend (Render)

1. Create a new **Web Service** on [Render](https://render.com)
2. Connect your GitHub repository
3. Configure:
   - **Build Command**: `pip install -r backend/requirements.txt`
   - **Start Command**: `uvicorn backend.main:app --host 0.0.0.0 --port $PORT`
4. Add environment variables:
   - `DATABASE_URL`
   - `SECRET_KEY`
   - `ALPHA_VANTAGE_API_KEY`
   - `GOOGLE_CLIENT_ID`
5. Deploy and note your API URL (e.g., `https://stock-api.onrender.com`)

### Frontend (Vercel)

1. Import your repository on [Vercel](https://vercel.com)
2. Framework preset: **Vite**
3. Root directory: `frontend`
4. Add environment variables:
   - `VITE_API_BASE=https://stock-api.onrender.com`
   - `VITE_GOOGLE_CLIENT_ID=your_client_id`
5. Deploy

### Post-Deployment

1. **Update CORS**: Add your Vercel URL to `origins` in `backend/main.py`
2. **Google OAuth**: Add your production frontend URL to "Authorized JavaScript origins" in Google Cloud Console
3. **Redeploy backend** to apply CORS changes

## 📖 API Endpoints

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| GET | `/` | No | Health check |
| POST | `/register` | No | Create new user |
| POST | `/token` | No | Login with username/password |
| POST | `/google-login` | No | Login with Google OAuth |
| GET | `/api/stocks/{symbol}` | Yes | Get stock price history |
| POST | `/api/predict` | Yes | Predict next-day close price |
| GET | `/health/db` | No | Database connectivity check |

## 🔐 Environment Variables Reference

**Root `.env` (Backend)**
```env
DATABASE_URL=postgresql://...
ALPHA_VANTAGE_API_KEY=your_key
SECRET_KEY=generate_with_openssl_rand_hex_32
GOOGLE_CLIENT_ID=your_oauth_client_id
```

**`frontend/.env`**
```env
VITE_API_BASE=http://127.0.0.1:8000  # or production URL
VITE_GOOGLE_CLIENT_ID=your_oauth_client_id
```

## 🧪 Testing

```bash
# Backend
curl http://127.0.0.1:8000/health/db

# Frontend (after login)
# Check browser console for successful API calls
```

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 🙏 Acknowledgments

- [Alpha Vantage](https://www.alphavantage.co/) for stock data API
- [Supabase](https://supabase.com/) for database hosting
- [FastAPI](https://fastapi.tiangolo.com/) for the backend framework
- [Recharts](https://recharts.org/) for beautiful charts

## 📧 Contact

Your Name - [@yourtwitter](https://twitter.com/yourtwitter) - your.email@example.com

Project Link: [https://github.com/yourusername/stock-predi](https://github.com/yourusername/stock-predi)

---

Made with ❤️ by [Your Name]