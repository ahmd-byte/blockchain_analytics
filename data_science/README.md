# Data Science Module - Blockchain Fraud Detection

This module provides machine learning capabilities for detecting suspicious and fraudulent wallet activity on the Ethereum blockchain.

## Overview

The data science layer uses unsupervised machine learning to identify anomalous wallet behavior patterns that may indicate fraud, money laundering, or other suspicious activities.

### Key Features

- **Feature Engineering**: Computes 30+ wallet-level features from raw transaction data
- **Anomaly Detection**: Multiple unsupervised ML models (Isolation Forest, LOF, DBSCAN)
- **Ensemble Scoring**: Combines model outputs for robust fraud scores
- **BigQuery Integration**: Reads from and writes to Google BigQuery
- **Evaluation Tools**: Comprehensive model analysis and visualization

## Architecture

```
data_science/
├── __init__.py              # Module initialization
├── config.py                # Configuration management
├── utils.py                 # Utility functions
├── feature_engineering.py   # Feature computation pipeline
├── fraud_model.py           # ML model training and scoring
├── model_evaluation.py      # Model analysis and reporting
├── run_pipeline.py          # Main pipeline orchestrator
├── sql/
│   └── feature_extraction.sql  # SQL queries for features
├── requirements.txt         # Python dependencies
├── env.example              # Environment template
└── README.md                # This file
```

## Quick Start

### 1. Installation

```bash
cd data_science
pip install -r requirements.txt
```

### 2. Configuration

```bash
cp env.example .env
# Edit .env with your GCP credentials
```

### 3. Run the Pipeline

```bash
# Run complete pipeline
python -m data_science.run_pipeline --mode full

# Run specific steps
python -m data_science.run_pipeline --mode features  # Only features
python -m data_science.run_pipeline --mode train     # Only training
python -m data_science.run_pipeline --mode score     # Only scoring
```

## Feature Engineering

### Basic Features
| Feature | Description |
|---------|-------------|
| `tx_count` | Total number of transactions |
| `tx_count_in` / `tx_count_out` | Incoming/outgoing transaction counts |
| `total_value` | Sum of all transaction values (ETH) |
| `avg_value` / `std_value` | Mean and standard deviation of values |
| `unique_counterparties` | Number of unique addresses interacted with |
| `in_out_ratio` | Ratio of incoming to outgoing transactions |
| `net_flow` | Total value in minus total value out |

### Behavioral Features
| Feature | Description |
|---------|-------------|
| `counterparty_concentration` | How focused transactions are on few addresses |
| `self_transactions` | Number of transactions to self |
| `round_value_ratio` | Proportion of round-number transactions |
| `high_value_tx_ratio` | Proportion of high-value transactions |

### Temporal Features
| Feature | Description |
|---------|-------------|
| `tx_frequency_per_hour` | Transaction rate over activity period |
| `avg_hours_between_tx` | Average time between transactions |
| `tx_count_7d` / `tx_count_30d` | Recent activity counts |
| `weekend_tx_ratio` | Proportion of weekend transactions |
| `night_tx_ratio` | Proportion of nighttime transactions |

## ML Models

### Isolation Forest (Primary)
- Efficient for high-dimensional data
- Isolates anomalies by random partitioning
- Score: distance from normal behavior

### Local Outlier Factor (LOF)
- Density-based outlier detection
- Compares local density to neighbors
- Good for clustered data

### DBSCAN
- Clustering-based approach
- Points not in any cluster = potential outliers
- Handles arbitrary cluster shapes

### Ensemble Method
- Combines all models with weighted voting
- Default weights: IF (50%), LOF (30%), DBSCAN (20%)
- More robust than single model

## Output Schema

### `blockchain_ml.wallet_features`
```sql
wallet_address      STRING
tx_count            INTEGER
tx_count_in         INTEGER
tx_count_out        INTEGER
total_value         FLOAT
-- ... (30+ features)
feature_timestamp   TIMESTAMP
feature_version     STRING
```

### `blockchain_ml.wallet_fraud_scores`
```sql
wallet_address          STRING
fraud_score             FLOAT     -- 0.0 (safe) to 1.0 (suspicious)
risk_category           STRING    -- 'low', 'medium', 'high', 'critical'
isolation_forest_score  FLOAT
lof_score               FLOAT
dbscan_is_noise         INTEGER
scored_at               TIMESTAMP
model_version           STRING
```

## Risk Categories

| Category | Score Range | Description |
|----------|-------------|-------------|
| Low | 0.0 - 0.39 | Normal wallet behavior |
| Medium | 0.4 - 0.69 | Some anomalous patterns |
| High | 0.7 - 0.89 | Significant suspicious activity |
| Critical | 0.9 - 1.0 | Highly anomalous, investigate immediately |

## API Integration

The backend queries fraud scores for the frontend:

```python
# Example query for backend integration
SELECT 
    wallet_address,
    fraud_score,
    risk_category
FROM `project.blockchain_ml.wallet_fraud_scores`
WHERE wallet_address = @address
```

## Configuration Options

### Model Hyperparameters

```python
# config.py
ModelConfig:
    random_seed: 42
    test_size: 0.2
    
    isolation_forest:
        n_estimators: 100
        contamination: 0.1
    
    lof:
        n_neighbors: 20
        contamination: 0.1
    
    dbscan:
        eps: 0.5
        min_samples: 5
```

### Feature Engineering

```python
FeatureConfig:
    min_transactions: 2        # Filter low-activity wallets
    time_windows: [7, 30, 90]  # Days for temporal features
    scaling_method: "standard"  # Feature normalization
```

## Evaluation Metrics

Since we use unsupervised learning (no ground truth labels), we evaluate using:

1. **Score Distribution Analysis**
   - Mean, std, percentiles of fraud scores
   - Risk category distribution

2. **Cluster Quality** (for DBSCAN)
   - Silhouette score
   - Davies-Bouldin index
   - Calinski-Harabasz score

3. **Feature Importance**
   - Permutation importance analysis
   - Top discriminating features

4. **High-Risk Analysis**
   - Feature comparison: high-risk vs. normal wallets
   - Identifying key risk indicators

## Usage Examples

### Feature Engineering Only

```python
from data_science.feature_engineering import FeatureEngineer

engineer = FeatureEngineer()
features_df = engineer.compute_all_features()
engineer.save_features(features_df)
```

### Train and Score

```python
from data_science.fraud_model import FraudDetector

detector = FraudDetector()
detector.fit(features_df, model_type="ensemble")
scores_df = detector.predict(features_df)
detector.save_scores(scores_df)
```

### Load Scores for Analysis

```python
from data_science.utils import BigQueryMLHelper

bq = BigQueryMLHelper()
query = """
SELECT * FROM `project.blockchain_ml.wallet_fraud_scores`
WHERE risk_category IN ('high', 'critical')
ORDER BY fraud_score DESC
"""
high_risk_wallets = bq.execute_query(query)
```

## Dependencies

- Python 3.9+
- scikit-learn 1.3+
- pandas 2.0+
- google-cloud-bigquery 3.11+
- numpy 1.24+

## Future Enhancements

1. **Supervised Learning**: Train on labeled fraud data when available
2. **Graph Neural Networks**: Incorporate transaction graph structure
3. **Real-time Scoring**: Stream processing for live fraud detection
4. **Feature Store**: Centralized feature management
5. **Model Monitoring**: Track model drift over time

## Troubleshooting

### Common Issues

1. **BigQuery Authentication**
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
   ```

2. **Missing Features**
   - Ensure data_engineering pipeline has run first
   - Check raw_transactions table exists

3. **Memory Issues**
   - Reduce batch_size in config
   - Use BigQuery for feature computation

## License

MIT License - See LICENSE file for details.

