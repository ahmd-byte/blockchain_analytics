-- Blockchain Analytics - BigQuery Table Definitions
-- This file contains CREATE TABLE statements for the required BigQuery tables

-- ===========================================
-- Dataset: blockchain_analytics
-- ===========================================

-- Create dataset if not exists
-- CREATE SCHEMA IF NOT EXISTS `your-project-id.blockchain_analytics`;

-- ---------------------------------------------
-- Table: fact_transactions
-- Description: Stores all blockchain transactions
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS `your-project-id.blockchain_analytics.fact_transactions` (
    transaction_hash STRING NOT NULL,
    block_number INT64,
    block_timestamp TIMESTAMP,
    transaction_timestamp TIMESTAMP NOT NULL,
    from_address STRING NOT NULL,
    to_address STRING,
    value FLOAT64,
    gas_used INT64,
    gas_price FLOAT64,
    transaction_fee FLOAT64,
    transaction_type STRING,
    status STRING,
    input_data STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(transaction_timestamp)
CLUSTER BY from_address, to_address
OPTIONS (
    description = 'Blockchain transactions fact table partitioned by date'
);

-- ---------------------------------------------
-- Table: dim_wallet
-- Description: Wallet dimension table with aggregated statistics
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS `your-project-id.blockchain_analytics.dim_wallet` (
    wallet_address STRING NOT NULL,
    first_transaction_date DATE,
    last_transaction_date DATE,
    total_transactions INT64 DEFAULT 0,
    total_volume FLOAT64 DEFAULT 0,
    total_sent FLOAT64 DEFAULT 0,
    total_received FLOAT64 DEFAULT 0,
    unique_counterparties INT64 DEFAULT 0,
    avg_transaction_value FLOAT64,
    max_transaction_value FLOAT64,
    min_transaction_value FLOAT64,
    wallet_age_days INT64,
    is_contract BOOL DEFAULT FALSE,
    wallet_label STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY wallet_address
OPTIONS (
    description = 'Wallet dimension table with aggregated statistics'
);


-- ===========================================
-- Dataset: blockchain_ml
-- ===========================================

-- Create dataset if not exists
-- CREATE SCHEMA IF NOT EXISTS `your-project-id.blockchain_ml`;

-- ---------------------------------------------
-- Table: wallet_fraud_scores
-- Description: ML-generated fraud scores for wallets
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS `your-project-id.blockchain_ml.wallet_fraud_scores` (
    wallet_address STRING NOT NULL,
    fraud_score FLOAT64 NOT NULL,  -- Range: 0.0 to 1.0
    is_suspicious BOOL DEFAULT FALSE,
    risk_category STRING,  -- low, medium, high, critical
    tx_count INT64 DEFAULT 0,
    total_value FLOAT64 DEFAULT 0,
    last_activity TIMESTAMP,
    model_version STRING,
    prediction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    confidence_score FLOAT64,
    feature_importance JSON,
    flagged_reason STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY wallet_address, is_suspicious
OPTIONS (
    description = 'ML-generated fraud scores and risk assessments for wallets'
);


-- ===========================================
-- Indexes and Additional Constraints
-- ===========================================

-- Note: BigQuery doesn't support traditional indexes.
-- Use CLUSTER BY and PARTITION BY for query optimization.
-- Consider creating materialized views for frequently accessed aggregations.


