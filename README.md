# Blockchain Transaction Analytics & Fraud Detection Platform

A production-ready full-stack platform for analyzing blockchain transactions and detecting fraudulent wallet activity. Features a complete data engineering pipeline that ingests real Ethereum transaction data from Etherscan, transforms it through staging layers, and serves it via a FastAPI backend to a React dashboard.

![Architecture](https://img.shields.io/badge/Architecture-Full%20Stack-blue)
![Python](https://img.shields.io/badge/Python-3.10+-green)
![React](https://img.shields.io/badge/React-18+-61DAFB)
![BigQuery](https://img.shields.io/badge/Database-BigQuery-4285F4)

## 🏗️ Project Structure

```
blockchain-analytics/
├── frontend/                    # React dashboard (Vite + TailwindCSS)
│   ├── src/
│   │   ├── pages/              # Dashboard, Wallet, Fraud pages
│   │   ├── components/         # Reusable UI components
│   │   └── api/                # API client configuration
│   └── package.json
│
├── backend/                     # FastAPI backend
│   ├── app/
│   │   ├── main.py             # Application entry point
│   │   ├── api/routes/         # API endpoints
│   │   ├── core/               # Configuration & BigQuery client
│   │   ├── schemas/            # Pydantic models
│   │   └── services/           # Business logic
│   └── requirements.txt
│
├── data_engineering/            # Complete data pipeline
│   ├── ingestion/              # Python ingestion scripts
│   │   ├── config.py           # Configuration management
│   │   ├── utils.py            # BigQuery helpers, checkpoints
│   │   ├── etherscan_client.py # Etherscan API V2 client
│   │   ├── ingest_transactions.py  # Transaction pipeline
│   │   └── ingest_wallets.py   # Wallet extraction pipeline
│   ├── sql/
│   │   ├── staging/            # Staging transformations
│   │   └── analytics/          # Fact/Dim tables
│   ├── dbt/                    # dbt models
│   │   ├── models/staging/     # Staging models
│   │   └── models/marts/       # Analytics models
│   ├── airflow/                # Orchestration DAGs
│   └── requirements.txt
│
├── data_science/               # ML fraud detection
│   ├── config.py              # Configuration management
│   ├── utils.py               # BigQuery helpers, utilities
│   ├── feature_engineering.py # 30+ wallet features
│   ├── fraud_model.py         # Isolation Forest, LOF, DBSCAN
│   ├── model_evaluation.py    # Analysis and visualization
│   ├── run_pipeline.py        # Pipeline orchestrator
│   └── sql/                   # Feature extraction queries
│
├── notebooks/                  # Jupyter notebooks (placeholder)
├── infra/                      # Deployment configs (placeholder)
└── README.md
```

## 🌟 Features

### Data Engineering Pipeline
- **Real-time Ingestion**: Fetch blockchain data from Etherscan API V2
- **Idempotent Processing**: Checkpoint-based tracking prevents duplicates
- **BigQuery Integration**: Batch loading optimized for free tier
- **Modular Design**: Separate raw, staging, and analytics layers

### Backend API
- **Dashboard Summary**: Real transaction counts, volume, and wallet stats
- **Wallet Analytics**: Detailed wallet stats with daily transaction volumes
- **Fraud Detection**: Risk scoring with filtering and pagination
- **Health Check**: System and BigQuery connection monitoring

### Frontend Dashboard
- **Interactive Dashboard**: Real-time metrics and charts
- **Wallet Explorer**: Search and analyze any Ethereum address
- **Fraud Detection**: View wallets sorted by risk score

### Machine Learning (Data Science)
- **Feature Engineering**: 30+ wallet-level features (basic, behavioral, temporal)
- **Anomaly Detection**: Isolation Forest, LOF, DBSCAN models
- **Ensemble Scoring**: Weighted combination for robust fraud detection
- **Risk Categories**: Low, Medium, High, Critical classifications

## 🔄 Data Pipeline Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Etherscan     │────▶│   Raw Layer     │────▶│  Staging Layer  │
│   API V2        │     │ (blockchain_raw)│     │  (cleaned data) │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                       │
                              ┌─────────────────────────┤
                              │                         │
                              ▼                         ▼
                    ┌─────────────────┐     ┌─────────────────┐
                    │  ML Pipeline    │     │ Analytics Layer │
                    │ (fraud scoring) │     │ (fact/dim)      │
                    └─────────────────┘     └─────────────────┘
                              │                         │
                              └─────────────────────────┤
                                                       ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   React         │◀────│   FastAPI       │◀────│ Fraud Scores    │
│   Frontend      │     │   Backend       │     │ (blockchain_ml) │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

### BigQuery Datasets

| Dataset | Description |
|---------|-------------|
| `blockchain_raw` | Raw ingested data (transactions, wallets) |
| `blockchain_staging` | Cleaned and normalized data |
| `blockchain_analytics` | Fact and dimension tables |
| `blockchain_ml` | ML features and predictions |

## 🛠️ Tech Stack

| Component | Technology |
|-----------|------------|
| Frontend | React 18, Vite, TailwindCSS, Recharts, Sonner |
| Backend | FastAPI, Pydantic v2, Uvicorn, AsyncIO |
| Database | Google BigQuery |
| Data Engineering | Python, dbt, Airflow |
| Data Science | scikit-learn, pandas, numpy |
| ML Models | Isolation Forest, LOF, DBSCAN |
| Data Source | Etherscan API V2 |

## 📦 Quick Start

### Prerequisites
- Python 3.10+
- Node.js 18+
- Google Cloud account with BigQuery enabled
- Etherscan API key (free tier available)

### 1. Data Engineering Setup

```bash
cd data_engineering

# Create virtual environment
python -m venv venv
.\venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp env.example .env
# Edit .env with your credentials:
# - GOOGLE_CLOUD_PROJECT=your-project-id
# - ETHERSCAN_API_KEY=your-api-key

# Ingest transaction data
python -m ingestion.ingest_transactions \
    --addresses 0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045 \
    --start-block 21000000

# Extract wallet data
python -m ingestion.ingest_wallets --from-transactions --limit 1000
```

### 2. Backend Setup

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
# Edit .env with your GCP project ID

# Run server
uvicorn app.main:app --host 0.0.0.0 --port 8080
```

### 3. Data Science Setup (ML Pipeline)

```bash
cd data_science

# Create virtual environment
python -m venv venv
.\venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp env.example .env
# Edit .env with your GCP credentials

# Run full ML pipeline
python -m data_science.run_pipeline --mode full --model-type ensemble

# Or run steps individually
python -m data_science.run_pipeline --mode features  # Feature engineering
python -m data_science.run_pipeline --mode train     # Model training
python -m data_science.run_pipeline --mode score     # Score wallets
```

### 4. Frontend Setup

```bash
cd frontend

# Install dependencies
npm install

# Run development server
npm run dev
```

### Access Points
- **Frontend**: http://localhost:5173
- **Backend API**: http://localhost:8080
- **API Docs**: http://localhost:8080/docs

## 📡 API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check with BigQuery status |
| `/api/dashboard/summary` | GET | Dashboard statistics |
| `/api/wallet/{address}` | GET | Wallet details and history |
| `/api/fraud/wallets` | GET | Fraud wallet list with filters |

### Example Requests

```bash
# Health check
curl http://localhost:8080/health

# Dashboard summary (real data)
curl http://localhost:8080/api/dashboard/summary

# Wallet details (Vitalik's wallet)
curl http://localhost:8080/api/wallet/0xd8da6bf26964af9d7eed9e03e53415d37aa96045

# Fraud wallets with filtering
curl "http://localhost:8080/api/fraud/wallets?page_size=10&sort_by=fraud_score&sort_order=desc"

# High-risk wallets only
curl "http://localhost:8080/api/fraud/wallets?min_fraud_score=0.7"
```

### Sample API Response

```json
{
  "total_transactions": 10000,
  "total_volume": 17619256.14,
  "total_wallets": 6468,
  "suspicious_wallet_count": 4,
  "last_updated": "2025-12-18T15:16:44.979623"
}
```

## 📊 Data Ingestion

### Transaction Ingestion

```bash
# Ingest transactions for specific addresses
python -m ingestion.ingest_transactions \
    --addresses 0x... 0x... \
    --start-block 19000000 \
    --resume

# From a file
python -m ingestion.ingest_transactions \
    --addresses-file addresses.txt \
    --include-internal
```

### Wallet Extraction

```bash
# Extract wallets from transactions
python -m ingestion.ingest_wallets \
    --from-transactions \
    --limit 10000

# Ingest specific addresses with balance enrichment
python -m ingestion.ingest_wallets \
    --addresses 0x... 0x...
```

### Features
- ✅ Rate-limited API calls (respects Etherscan limits)
- ✅ Checkpoint-based resumable processing
- ✅ Deduplication using transaction hashes
- ✅ Batch loading for BigQuery free tier
- ✅ Error handling with exponential backoff

## 🗄️ BigQuery Schema

### Raw Layer (`blockchain_raw`)

**`raw_transactions`**
- `transaction_hash`, `block_number`, `from_address`, `to_address`
- `value_wei` (NUMERIC), `value_eth` (FLOAT)
- `gas`, `gas_price`, `gas_used`
- `transaction_timestamp`, `ingested_at`

**`raw_wallets`**
- `wallet_address`, `balance_wei`, `balance_eth`
- `total_transactions_in/out`, `total_value_in/out`
- `first_seen_timestamp`, `last_seen_timestamp`

### Analytics Layer (`blockchain_analytics`)

**`fact_transactions`** - Partitioned by date, clustered by addresses

**`dim_wallet`** - Wallet dimension with risk indicators

**`dim_time`** - Date dimension (2015-2030)

**`agg_daily_metrics`** - Pre-aggregated daily statistics

### ML Layer (`blockchain_ml`)

**`wallet_features`** - 30+ computed features per wallet
- Transaction counts (in/out), value statistics
- Behavioral patterns (counterparty concentration, self-transactions)
- Temporal patterns (frequency, time distributions)

**`wallet_fraud_scores`**
- `fraud_score` (FLOAT): 0.0 (safe) to 1.0 (suspicious)
- `risk_category`: 'low', 'medium', 'high', 'critical'
- `isolation_forest_score`, `lof_score`, `dbscan_is_noise`

## 🔒 Security Features

- ✅ Parameterized SQL queries (SQL injection prevention)
- ✅ CORS configuration for frontend integration
- ✅ Input validation with Pydantic
- ✅ Error message sanitization
- ✅ Service account authentication for BigQuery

## 🚧 Development

### Using Mock Data

For development without BigQuery, the API supports mock data mode:

```bash
# Set in backend/.env
DEBUG=True

# Or use query parameter
curl "http://localhost:8080/api/dashboard/summary?use_mock=true"
```

### Running dbt Models

```bash
cd data_engineering/dbt

# Run all models
dbt run

# Run specific models
dbt run --select staging+
dbt run --select marts.core

# Test models
dbt test
```

### API Documentation

- **Swagger UI**: http://localhost:8080/docs
- **ReDoc**: http://localhost:8080/redoc

## 📈 Sample Data Statistics

After running the ingestion pipeline:

| Metric | Value |
|--------|-------|
| Total Transactions | 10,000+ |
| Total Volume | 17.6M+ ETH |
| Unique Wallets | 6,400+ |
| Date Range | Oct 2024 - Present |

## 🗺️ Roadmap

- [x] Machine learning fraud detection models
- [ ] Real-time streaming ingestion
- [ ] Token transfer tracking (ERC-20)
- [ ] Multi-chain support (Polygon, BSC)
- [ ] Alert system for suspicious activity
- [ ] Kubernetes deployment configs
- [ ] Model monitoring and drift detection
- [ ] Graph neural networks for transaction patterns

## 📝 License

MIT License

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 👨‍💻 Author

Built as a portfolio project demonstrating:
- Full-stack development (React + FastAPI)
- Data engineering (ETL pipelines, BigQuery)
- Data science (ML-based anomaly detection)
- Cloud services (Google Cloud Platform)
- Blockchain data analysis
