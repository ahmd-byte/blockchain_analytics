# Data Engineering Layer

Production-ready data engineering infrastructure for the Blockchain Transaction Analytics Platform.

## 📁 Directory Structure

```
data_engineering/
├── ingestion/               # Python data ingestion scripts
│   ├── __init__.py
│   ├── config.py           # Configuration management
│   ├── utils.py            # Shared utilities and helpers
│   ├── etherscan_client.py # Etherscan API client
│   ├── ingest_transactions.py  # Transaction ingestion pipeline
│   └── ingest_wallets.py   # Wallet data ingestion pipeline
│
├── sql/                    # SQL transformations for BigQuery
│   ├── staging/            # Staging layer transformations
│   │   ├── stg_transactions.sql
│   │   └── stg_wallets.sql
│   └── analytics/          # Analytics layer transformations
│       ├── dim_time.sql
│       ├── dim_wallet.sql
│       ├── fact_transactions.sql
│       └── agg_daily_metrics.sql
│
├── dbt/                    # dbt models (alternative to raw SQL)
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   ├── macros/
│   │   ├── generate_schema_name.sql
│   │   └── data_quality.sql
│   └── models/
│       ├── staging/
│       │   ├── sources.yml
│       │   ├── schema.yml
│       │   ├── stg_blockchain__transactions.sql
│       │   └── stg_blockchain__wallets.sql
│       └── marts/
│           ├── core/
│           │   ├── schema.yml
│           │   ├── dim_time.sql
│           │   ├── dim_wallet.sql
│           │   └── fct_transactions.sql
│           └── aggregates/
│               └── agg_daily_metrics.sql
│
├── airflow/                # Airflow DAGs
│   └── dags/
│       └── blockchain_daily_pipeline.py
│
├── requirements.txt        # Python dependencies
└── README.md              # This file
```

## 🏗️ Architecture

### Data Flow

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Data Sources  │────▶│   Raw Layer     │────▶│  Staging Layer  │
│   (Etherscan)   │     │ (blockchain_raw)│     │(blockchain_stg) │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   ML Features   │◀────│ Analytics Layer │◀────│    Transform    │
│ (blockchain_ml) │     │(blockchain_anlyt│     │   (dbt/SQL)     │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

### BigQuery Datasets

| Dataset | Description |
|---------|-------------|
| `blockchain_raw` | Raw ingested data from sources |
| `blockchain_staging` | Cleaned and normalized data |
| `blockchain_analytics` | Fact and dimension tables |
| `blockchain_ml` | ML features and predictions |

## 🚀 Quick Start

### Prerequisites

1. **Google Cloud Project** with BigQuery enabled
2. **Service Account** with BigQuery Admin role
3. **Python 3.9+** installed
4. **Etherscan API Key** (free tier available)

### Installation

```bash
# Navigate to data engineering directory
cd data_engineering

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: .\venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```

### Configuration

1. **Environment Variables** (`.env` file):

```env
# Google Cloud
GOOGLE_CLOUD_PROJECT=your-project-id
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json

# Etherscan
ETHERSCAN_API_KEY=your-etherscan-api-key

# Optional: Web3 Provider
WEB3_PROVIDER_URL=https://mainnet.infura.io/v3/your-project-id

# Logging
LOG_LEVEL=INFO
```

2. **dbt Profile** (`~/.dbt/profiles.yml`):

```yaml
blockchain_analytics:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: your-project-id
      dataset: blockchain_dev
      threads: 4
      keyfile: /path/to/service-account-key.json
```

## 📊 Running the Pipeline

### Manual Execution

#### 1. Ingest Transactions

```bash
# Ingest transactions for specific addresses
python -m ingestion.ingest_transactions \
    --addresses 0x... 0x... \
    --start-block 18000000 \
    --resume

# Or from a file
python -m ingestion.ingest_transactions \
    --addresses-file addresses.txt \
    --include-internal
```

#### 2. Ingest Wallets

```bash
# Extract wallets from existing transactions
python -m ingestion.ingest_wallets --from-transactions --limit 10000

# Or ingest specific addresses
python -m ingestion.ingest_wallets --addresses 0x... 0x...
```

#### 3. Run SQL Transformations

```bash
# Using BigQuery CLI
bq query --use_legacy_sql=false < sql/staging/stg_transactions.sql
bq query --use_legacy_sql=false < sql/analytics/fact_transactions.sql
```

#### 4. Run dbt Models

```bash
cd dbt

# Run all models
dbt run

# Run specific models
dbt run --select staging+
dbt run --select marts.core

# Test models
dbt test
```

### Automated Execution (Airflow)

```bash
# Deploy DAG
cp airflow/dags/*.py $AIRFLOW_HOME/dags/

# Set Airflow variables
airflow variables set gcp_project_id "your-project-id"
airflow variables set etherscan_api_key "your-api-key"
airflow variables set monitored_addresses '["0x...", "0x..."]'

# Trigger DAG manually
airflow dags trigger blockchain_daily_pipeline
```

## 📋 Data Models

### Raw Layer

**`raw_transactions`**
- `transaction_hash`: Unique transaction identifier
- `block_number`: Block containing the transaction
- `from_address`, `to_address`: Sender and receiver
- `value_wei`, `value_eth`: Transaction value
- `gas`, `gas_price`, `gas_used`: Gas metrics
- `ingested_at`: Ingestion timestamp

**`raw_wallets`**
- `wallet_address`: Ethereum address
- `balance_wei`, `balance_eth`: Current balance
- `total_transactions_in/out`: Transaction counts
- `total_value_in/out`: Total value transferred

### Staging Layer

**`stg_transactions`**
- Deduplicated transactions
- Normalized addresses (lowercase)
- Derived fields: `transaction_type`, `is_contract_interaction`
- Data quality score

**`stg_wallets`**
- Deduplicated wallets
- Wallet classification: `wallet_type`
- Calculated metrics: `activity_days`, `transactions_per_day`

### Analytics Layer

**`dim_time`**
- Date dimension from 2015-07-30 to 2030
- Date hierarchies (year, quarter, month, week)
- Calendar attributes (is_weekend, is_month_end)

**`dim_wallet`**
- Wallet attributes and classifications
- Activity metrics and balance tiers
- Risk indicators (fraud_score, is_suspicious)

**`fact_transactions`**
- Transaction fact table with foreign keys
- Value measures (ETH, Wei, USD)
- Gas measures and efficiency
- Transaction classification

**`agg_daily_metrics`**
- Pre-aggregated daily metrics
- Transaction counts and success rates
- Value statistics and distributions
- Moving averages and trends

## 🔧 Key Features

### Idempotent Ingestion
- Checkpoint-based tracking prevents duplicate processing
- Deduplication using transaction hashes
- Resume capability for interrupted runs

### Data Quality
- Quality scores on all staged records
- Quality issue tracking per record
- Automated validation checks

### Incremental Processing
- dbt incremental models for efficient updates
- Partition-based queries for performance
- Merge operations prevent duplicates

### Observability
- Structured logging throughout
- Airflow task monitoring
- Data quality metrics

## 🧪 Testing

```bash
# Run unit tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=ingestion --cov-report=html

# Run dbt tests
cd dbt && dbt test
```

## 📈 Monitoring

### Airflow Metrics
- DAG run duration
- Task success/failure rates
- Data lag indicators

### BigQuery Monitoring
- Query costs
- Slot utilization
- Table sizes

### Data Quality Metrics
- Record counts per layer
- Quality score distributions
- Duplicate rates

## 🔐 Security

- Service account authentication
- No hardcoded credentials
- Environment variable configuration
- Least privilege access

## 📝 Contributing

1. Follow existing code style
2. Add docstrings to all functions
3. Include tests for new features
4. Update documentation

## 📄 License

MIT License - See LICENSE file for details.

