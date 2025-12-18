# Blockchain Transaction Analytics & Fraud Detection Platform

A production-ready full-stack platform for analyzing blockchain transactions and detecting fraudulent wallet activity using machine learning.

## 🏗️ Project Structure

```
blockchain-analytics/
├── frontend/               # React dashboard (Vite + TailwindCSS)
├── backend/                # FastAPI backend
│   ├── app/
│   │   ├── main.py         # Application entry point
│   │   ├── api/routes/     # API endpoints
│   │   ├── core/           # Configuration & BigQuery client
│   │   ├── schemas/        # Pydantic models
│   │   └── services/       # Business logic
│   └── requirements.txt
├── data_engineering/
│   ├── ingestion/          # ETL scripts (placeholder)
│   ├── sql/                # BigQuery SQL queries
│   └── dbt/                # dbt models (placeholder)
├── data_science/           # ML models (placeholder)
├── notebooks/              # Jupyter notebooks (placeholder)
├── infra/                  # Deployment configs (placeholder)
└── README.md
```

## 🚀 Features

### Backend API
- **Dashboard Summary**: Total transactions, volume, wallets, and suspicious count
- **Wallet Analytics**: Detailed wallet stats with daily transaction volumes
- **Fraud Detection**: ML-based fraud scores with filtering and pagination
- **Health Check**: System status monitoring

### Technical Highlights
- Async FastAPI with BigQuery integration
- Parameterized queries for SQL injection prevention
- CORS enabled for frontend integration
- Comprehensive API documentation (Swagger/ReDoc)
- Mock data mode for development

## 🛠️ Tech Stack

| Component | Technology |
|-----------|------------|
| Frontend | React, Vite, TailwindCSS, Recharts |
| Backend | FastAPI, Pydantic v2, Uvicorn |
| Database | Google BigQuery |
| ML | Python (placeholder for models) |

## 📦 Quick Start

### Prerequisites
- Python 3.10+
- Node.js 18+
- Google Cloud account with BigQuery enabled

### Backend Setup

```bash
cd backend

# Create virtual environment
python -m venv venv
.\venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp env.example .env
# Edit .env with your GCP credentials

# Run server
uvicorn app.main:app --reload --port 8000
```

### Frontend Setup

```bash
cd frontend

# Install dependencies
npm install

# Run development server
npm run dev
```

## 📡 API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/api/dashboard/summary` | GET | Dashboard statistics |
| `/api/wallet/{address}` | GET | Wallet details |
| `/api/fraud/wallets` | GET | Fraud wallet list |

### Example Requests

```bash
# Health check
curl http://localhost:8000/health

# Dashboard summary (mock data)
curl "http://localhost:8000/api/dashboard/summary?use_mock=true"

# Wallet details
curl "http://localhost:8000/api/wallet/0x742d35Cc?use_mock=true"

# Fraud wallets with filtering
curl "http://localhost:8000/api/fraud/wallets?is_suspicious=true&use_mock=true"
```

## 📊 BigQuery Schema

### Tables Required

**`blockchain_analytics.fact_transactions`**
- Transaction records with addresses, values, timestamps

**`blockchain_analytics.dim_wallet`**
- Wallet dimension table with aggregated stats

**`blockchain_ml.wallet_fraud_scores`**
- ML-generated fraud scores and risk categories

See `data_engineering/sql/create_tables.sql` for full schema definitions.

## 🔒 Security Features

- Parameterized SQL queries
- CORS configuration
- Input validation with Pydantic
- Error message sanitization in production

## 🚧 Development

### Using Mock Data

Set `DEBUG=true` in `.env` or add `?use_mock=true` to API requests for development without BigQuery.

### Running Tests

```bash
cd backend
pytest --cov=app
```

### API Documentation

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## 📝 License

MIT License

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Open a Pull Request


