-- =============================================================================
-- FEATURE EXTRACTION QUERIES FOR FRAUD DETECTION
-- =============================================================================
-- 
-- This file contains BigQuery SQL queries used to extract wallet-level features
-- from blockchain transaction data. These features are used for ML-based fraud
-- detection.
-- 
-- Usage:
--   These queries are called by the Python feature engineering pipeline,
--   but can also be run directly in BigQuery Console for exploration.
-- 
-- Author: Blockchain Analytics Team
-- Version: 1.0.0
-- =============================================================================


-- -----------------------------------------------------------------------------
-- QUERY 1: BASIC WALLET FEATURES
-- -----------------------------------------------------------------------------
-- Extracts fundamental transaction statistics per wallet:
-- - Transaction counts (total, in, out)
-- - Value statistics (sum, avg, std, min, max)
-- - Counterparty analysis
-- - Gas usage patterns

WITH wallet_transactions AS (
    -- Combine outgoing and incoming transactions
    SELECT
        from_address AS wallet_address,
        value_eth,
        gas_price,
        gas_used,
        to_address AS counterparty,
        block_timestamp,
        'out' AS direction
    FROM `PROJECT_ID.blockchain_raw.raw_transactions`
    WHERE from_address IS NOT NULL
    
    UNION ALL
    
    SELECT
        to_address AS wallet_address,
        value_eth,
        gas_price,
        gas_used,
        from_address AS counterparty,
        block_timestamp,
        'in' AS direction
    FROM `PROJECT_ID.blockchain_raw.raw_transactions`
    WHERE to_address IS NOT NULL
)

SELECT
    wallet_address,
    
    -- Transaction counts
    COUNT(*) AS tx_count,
    COUNTIF(direction = 'in') AS tx_count_in,
    COUNTIF(direction = 'out') AS tx_count_out,
    
    -- Value statistics
    SUM(value_eth) AS total_value,
    SUM(CASE WHEN direction = 'in' THEN value_eth ELSE 0 END) AS total_value_in,
    SUM(CASE WHEN direction = 'out' THEN value_eth ELSE 0 END) AS total_value_out,
    AVG(value_eth) AS avg_value,
    STDDEV(value_eth) AS std_value,
    MIN(value_eth) AS min_value,
    MAX(value_eth) AS max_value,
    
    -- Counterparty analysis
    COUNT(DISTINCT counterparty) AS unique_counterparties,
    
    -- Gas analysis
    AVG(gas_used) AS avg_gas_used,
    AVG(gas_price) AS avg_gas_price,
    
    -- Derived features
    SAFE_DIVIDE(COUNTIF(direction = 'in'), COUNTIF(direction = 'out')) AS in_out_ratio,
    SUM(CASE WHEN direction = 'in' THEN value_eth ELSE -value_eth END) AS net_flow

FROM wallet_transactions
GROUP BY wallet_address
HAVING COUNT(*) >= 2;  -- Minimum transactions filter


-- -----------------------------------------------------------------------------
-- QUERY 2: TEMPORAL FEATURES
-- -----------------------------------------------------------------------------
-- Extracts time-based features for each wallet:
-- - Activity patterns over different time windows
-- - Transaction frequency
-- - Day of week and hour of day distributions

WITH wallet_transactions AS (
    SELECT
        from_address AS wallet_address,
        value_eth,
        block_timestamp
    FROM `PROJECT_ID.blockchain_raw.raw_transactions`
    WHERE from_address IS NOT NULL
    
    UNION ALL
    
    SELECT
        to_address AS wallet_address,
        value_eth,
        block_timestamp
    FROM `PROJECT_ID.blockchain_raw.raw_transactions`
    WHERE to_address IS NOT NULL
),

current_time AS (
    SELECT MAX(block_timestamp) AS max_time
    FROM wallet_transactions
)

SELECT
    wt.wallet_address,
    
    -- Transaction frequency (per hour over total activity period)
    COUNT(*) / GREATEST(
        TIMESTAMP_DIFF(MAX(wt.block_timestamp), MIN(wt.block_timestamp), HOUR), 
        1
    ) AS tx_frequency_per_hour,
    
    -- Average time between transactions (in hours)
    SAFE_DIVIDE(
        TIMESTAMP_DIFF(MAX(wt.block_timestamp), MIN(wt.block_timestamp), HOUR),
        GREATEST(COUNT(*) - 1, 1)
    ) AS avg_hours_between_tx,
    
    -- Activity in last 7 days
    COUNTIF(wt.block_timestamp >= TIMESTAMP_SUB(ct.max_time, INTERVAL 7 DAY)) AS tx_count_7d,
    SUM(CASE WHEN wt.block_timestamp >= TIMESTAMP_SUB(ct.max_time, INTERVAL 7 DAY) 
        THEN wt.value_eth ELSE 0 END) AS value_7d,
    
    -- Activity in last 30 days
    COUNTIF(wt.block_timestamp >= TIMESTAMP_SUB(ct.max_time, INTERVAL 30 DAY)) AS tx_count_30d,
    SUM(CASE WHEN wt.block_timestamp >= TIMESTAMP_SUB(ct.max_time, INTERVAL 30 DAY) 
        THEN wt.value_eth ELSE 0 END) AS value_30d,
    
    -- Hour distribution
    COUNT(DISTINCT EXTRACT(HOUR FROM wt.block_timestamp)) AS unique_hours_active,
    
    -- Weekend vs weekday activity
    SAFE_DIVIDE(
        COUNTIF(EXTRACT(DAYOFWEEK FROM wt.block_timestamp) IN (1, 7)),
        COUNT(*)
    ) AS weekend_tx_ratio,
    
    -- Night activity (0-6 hours UTC)
    SAFE_DIVIDE(
        COUNTIF(EXTRACT(HOUR FROM wt.block_timestamp) BETWEEN 0 AND 6),
        COUNT(*)
    ) AS night_tx_ratio

FROM wallet_transactions wt
CROSS JOIN current_time ct
GROUP BY wt.wallet_address
HAVING COUNT(*) >= 2;


-- -----------------------------------------------------------------------------
-- QUERY 3: BEHAVIORAL FEATURES
-- -----------------------------------------------------------------------------
-- Extracts behavioral patterns that may indicate suspicious activity:
-- - Counterparty concentration
-- - Round value patterns
-- - Self-transactions

WITH wallet_transactions AS (
    SELECT
        from_address AS wallet_address,
        value_eth,
        to_address AS counterparty
    FROM `PROJECT_ID.blockchain_raw.raw_transactions`
    WHERE from_address IS NOT NULL
    
    UNION ALL
    
    SELECT
        to_address AS wallet_address,
        value_eth,
        from_address AS counterparty
    FROM `PROJECT_ID.blockchain_raw.raw_transactions`
    WHERE to_address IS NOT NULL
),

counterparty_stats AS (
    SELECT
        wallet_address,
        counterparty,
        COUNT(*) AS tx_with_counterparty,
        SUM(value_eth) AS value_with_counterparty
    FROM wallet_transactions
    GROUP BY wallet_address, counterparty
)

SELECT
    wallet_address,
    
    -- Average value per counterparty
    AVG(value_with_counterparty) AS avg_counterparty_value,
    
    -- Counterparty concentration (max tx with single counterparty / total)
    MAX(tx_with_counterparty) / SUM(tx_with_counterparty) AS counterparty_concentration,
    
    -- Self-transactions count
    COUNTIF(wallet_address = counterparty) AS self_transactions

FROM counterparty_stats
GROUP BY wallet_address;


-- -----------------------------------------------------------------------------
-- QUERY 4: LOAD FEATURES FOR ML
-- -----------------------------------------------------------------------------
-- Query to load computed features from BigQuery for model training

SELECT
    wallet_address,
    tx_count,
    tx_count_in,
    tx_count_out,
    total_value,
    total_value_in,
    total_value_out,
    avg_value,
    std_value,
    min_value,
    max_value,
    unique_counterparties,
    avg_gas_used,
    avg_gas_price,
    in_out_ratio,
    net_flow,
    tx_per_active_day,
    value_per_tx,
    avg_counterparty_value,
    counterparty_concentration,
    self_transactions,
    round_value_ratio,
    high_value_tx_ratio,
    zero_value_tx_ratio,
    tx_frequency_per_hour,
    avg_hours_between_tx,
    tx_count_7d,
    value_7d,
    tx_count_30d,
    value_30d,
    unique_hours_active,
    unique_days_of_week_active,
    weekend_tx_ratio,
    night_tx_ratio,
    feature_timestamp
FROM `PROJECT_ID.blockchain_ml.wallet_features`
ORDER BY feature_timestamp DESC;


-- -----------------------------------------------------------------------------
-- QUERY 5: GET FRAUD SCORES FOR BACKEND
-- -----------------------------------------------------------------------------
-- Query used by the backend to fetch fraud scores

SELECT
    wallet_address,
    fraud_score,
    risk_category,
    isolation_forest_score,
    scored_at,
    model_version
FROM `PROJECT_ID.blockchain_ml.wallet_fraud_scores`
ORDER BY fraud_score DESC;


-- -----------------------------------------------------------------------------
-- QUERY 6: HIGH RISK WALLETS SUMMARY
-- -----------------------------------------------------------------------------
-- Summary of high-risk wallets for dashboard

SELECT
    risk_category,
    COUNT(*) AS wallet_count,
    AVG(fraud_score) AS avg_fraud_score,
    MIN(fraud_score) AS min_fraud_score,
    MAX(fraud_score) AS max_fraud_score
FROM `PROJECT_ID.blockchain_ml.wallet_fraud_scores`
GROUP BY risk_category
ORDER BY 
    CASE risk_category
        WHEN 'critical' THEN 1
        WHEN 'high' THEN 2
        WHEN 'medium' THEN 3
        WHEN 'low' THEN 4
    END;


-- -----------------------------------------------------------------------------
-- QUERY 7: JOIN FRAUD SCORES WITH FEATURES FOR ANALYSIS
-- -----------------------------------------------------------------------------
-- Combines fraud scores with original features for investigation

SELECT
    fs.wallet_address,
    fs.fraud_score,
    fs.risk_category,
    wf.tx_count,
    wf.total_value,
    wf.unique_counterparties,
    wf.in_out_ratio,
    wf.net_flow,
    wf.counterparty_concentration,
    wf.self_transactions,
    wf.high_value_tx_ratio
FROM `PROJECT_ID.blockchain_ml.wallet_fraud_scores` fs
JOIN `PROJECT_ID.blockchain_ml.wallet_features` wf
    ON fs.wallet_address = wf.wallet_address
WHERE fs.risk_category IN ('high', 'critical')
ORDER BY fs.fraud_score DESC
LIMIT 100;

