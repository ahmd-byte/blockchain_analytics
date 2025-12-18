# Blockchain Analytics API

Production-ready FastAPI backend for the Blockchain Transaction Analytics & Fraud Detection Platform.

## Features

- **Dashboard API**: Summary statistics including total transactions, volume, wallets, and suspicious wallet count
- **Wallet API**: Detailed wallet information with daily transaction volumes
- **Fraud Detection API**: ML-based fraud scores and suspicious wallet data
- **Health Check**: System health monitoring with BigQuery connection status

## Tech Stack

- **Framework**: FastAPI (async Python web framework)
- **Database**: Google BigQuery
- **Validation**: Pydantic v2
- **Server**: Uvicorn (ASGI)

## Project Structure

```
backend/
├── app/
│   ├── main.py                    # FastAPI application entry point
│   ├── api/
│   │   └── routes/
│   │       ├── dashboard.py       # Dashboard endpoints
│   │       ├── wallet.py          # Wallet endpoints
│   │       ├── fraud.py           # Fraud detection endpoints
│   │       └── health.py          # Health check endpoints
│   ├── core/
│   │   ├── config.py              # Application configuration
│   │   └── bigquery_client.py     # BigQuery client wrapper
│   ├── schemas/
│   │   ├── dashboard.py           # Dashboard response schemas
│   │   ├── wallet.py              # Wallet response schemas
│   │   ├── fraud.py               # Fraud response schemas
│   │   └── health.py              # Health check schemas
│   └── services/
│       ├── dashboard_service.py   # Dashboard business logic
│       ├── wallet_service.py      # Wallet business logic
│       └── fraud_service.py       # Fraud detection business logic
└── requirements.txt               # Python dependencies
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check with BigQuery status |
| `/api/dashboard/summary` | GET | Dashboard summary statistics |
| `/api/wallet/{wallet_address}` | GET | Wallet details with daily volumes |
| `/api/fraud/wallets` | GET | Paginated fraud wallet list |

## Setup

### Prerequisites

- Python 3.10+
- Google Cloud Project with BigQuery enabled
- Service account with BigQuery access

### Installation

1. **Clone the repository**
   ```bash
   cd backend
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   
   # Windows
   .\venv\Scripts\activate
   
   # Linux/Mac
   source venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**
   
   Create a `.env` file in the backend directory:
   ```env
   # Google Cloud Configuration
   GOOGLE_CLOUD_PROJECT=your-gcp-project-id
   GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
   
   # Application Settings
   DEBUG=true  # Set to false in production
   
   # CORS Origins (comma-separated for multiple)
   CORS_ORIGINS=http://localhost:3000,http://localhost:5173
   ```

5. **Run the server**
   ```bash
   # Development mode with auto-reload
   uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
   
   # Or run directly
   python -m app.main
   ```

## API Documentation

Once the server is running, access the interactive API documentation:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI JSON**: http://localhost:8000/openapi.json

## Development

### Using Mock Data

For development without BigQuery access, use the `use_mock=true` query parameter:

```bash
# Dashboard summary with mock data
curl "http://localhost:8000/api/dashboard/summary?use_mock=true"

# Wallet details with mock data
curl "http://localhost:8000/api/wallet/0x742d35Cc6634C0532925a3b844Bc9e7595f2bD5e?use_mock=true"

# Fraud wallets with mock data
curl "http://localhost:8000/api/fraud/wallets?use_mock=true"
```

Or set `DEBUG=true` in your `.env` file to always use mock data.

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=app --cov-report=html

# Run specific test file
pytest tests/test_dashboard.py -v
```

### Code Formatting

```bash
# Format code
black app/
isort app/

# Lint code
ruff check app/
```

## BigQuery Schema

### Required Tables

**Dataset: `blockchain_analytics`**

1. **`fact_transactions`**
   - `transaction_hash`: STRING
   - `from_address`: STRING
   - `to_address`: STRING
   - `value`: FLOAT64
   - `transaction_timestamp`: TIMESTAMP
   - `block_number`: INT64

2. **`dim_wallet`**
   - `wallet_address`: STRING (Primary Key)
   - `total_transactions`: INT64
   - `total_volume`: FLOAT64
   - `first_transaction_date`: DATE
   - `last_transaction_date`: DATE
   - `unique_counterparties`: INT64

**Dataset: `blockchain_ml`**

3. **`wallet_fraud_scores`**
   - `wallet_address`: STRING (Primary Key)
   - `fraud_score`: FLOAT64 (0-1)
   - `is_suspicious`: BOOL
   - `tx_count`: INT64
   - `total_value`: FLOAT64
   - `last_activity`: TIMESTAMP

## Security Considerations

- **Parameterized Queries**: All BigQuery queries use parameterized queries to prevent SQL injection
- **CORS**: Configured to allow only specified origins
- **Input Validation**: Pydantic models validate all input data
- **Error Handling**: Sensitive error details are hidden in production mode

## Production Deployment

For production deployment:

1. Set `DEBUG=false` in environment
2. Configure proper CORS origins
3. Use a production ASGI server (Gunicorn with Uvicorn workers)
4. Set up proper logging and monitoring
5. Implement authentication (API keys, OAuth2)
6. Add rate limiting

Example production run:
```bash
gunicorn app.main:app -w 4 -k uvicorn.workers.UvicornWorker -b 0.0.0.0:8000
```

## License

MIT License


