-- Blockchain Analytics - Dashboard Queries
-- These queries are used by the dashboard API endpoints

-- ===========================================
-- Dashboard Summary Statistics
-- ===========================================

-- Query 1: Get total transactions and volume
-- Used in: GET /api/dashboard/summary
SELECT 
    COUNT(*) as total_transactions,
    COALESCE(SUM(value), 0) as total_volume,
    COUNT(DISTINCT from_address) as unique_senders,
    COUNT(DISTINCT to_address) as unique_receivers,
    AVG(value) as avg_transaction_value,
    MAX(transaction_timestamp) as last_transaction_time
FROM `your-project-id.blockchain_analytics.fact_transactions`;


-- Query 2: Get total unique wallets count
-- Used in: GET /api/dashboard/summary
SELECT 
    COUNT(DISTINCT wallet_address) as total_wallets
FROM `your-project-id.blockchain_analytics.dim_wallet`;


-- Query 3: Get suspicious wallet count
-- Used in: GET /api/dashboard/summary
SELECT 
    COUNT(*) as suspicious_count,
    AVG(fraud_score) as avg_fraud_score
FROM `your-project-id.blockchain_ml.wallet_fraud_scores`
WHERE is_suspicious = TRUE;


-- ===========================================
-- Time-series Dashboard Metrics
-- ===========================================

-- Query 4: Daily transaction volume (last 30 days)
SELECT 
    DATE(transaction_timestamp) as date,
    COUNT(*) as transaction_count,
    SUM(value) as total_volume,
    COUNT(DISTINCT from_address) as unique_senders,
    COUNT(DISTINCT to_address) as unique_receivers
FROM `your-project-id.blockchain_analytics.fact_transactions`
WHERE DATE(transaction_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY DATE(transaction_timestamp)
ORDER BY date DESC;


-- Query 5: Hourly transaction volume (last 24 hours)
SELECT 
    TIMESTAMP_TRUNC(transaction_timestamp, HOUR) as hour,
    COUNT(*) as transaction_count,
    SUM(value) as total_volume
FROM `your-project-id.blockchain_analytics.fact_transactions`
WHERE transaction_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY TIMESTAMP_TRUNC(transaction_timestamp, HOUR)
ORDER BY hour DESC;


-- Query 6: Weekly transaction summary
SELECT 
    DATE_TRUNC(DATE(transaction_timestamp), WEEK) as week_start,
    COUNT(*) as transaction_count,
    SUM(value) as total_volume,
    COUNT(DISTINCT from_address) + COUNT(DISTINCT to_address) as active_wallets
FROM `your-project-id.blockchain_analytics.fact_transactions`
WHERE DATE(transaction_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)
GROUP BY DATE_TRUNC(DATE(transaction_timestamp), WEEK)
ORDER BY week_start DESC;


-- ===========================================
-- Risk Distribution Metrics
-- ===========================================

-- Query 7: Fraud score distribution
SELECT 
    CASE 
        WHEN fraud_score >= 0.9 THEN 'critical'
        WHEN fraud_score >= 0.7 THEN 'high'
        WHEN fraud_score >= 0.4 THEN 'medium'
        ELSE 'low'
    END as risk_category,
    COUNT(*) as wallet_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM `your-project-id.blockchain_ml.wallet_fraud_scores`
GROUP BY 
    CASE 
        WHEN fraud_score >= 0.9 THEN 'critical'
        WHEN fraud_score >= 0.7 THEN 'high'
        WHEN fraud_score >= 0.4 THEN 'medium'
        ELSE 'low'
    END
ORDER BY 
    CASE risk_category
        WHEN 'critical' THEN 1
        WHEN 'high' THEN 2
        WHEN 'medium' THEN 3
        ELSE 4
    END;


-- Query 8: Top suspicious wallets by volume
SELECT 
    f.wallet_address,
    f.fraud_score,
    f.tx_count,
    f.total_value,
    f.last_activity
FROM `your-project-id.blockchain_ml.wallet_fraud_scores` f
WHERE f.is_suspicious = TRUE
ORDER BY f.total_value DESC
LIMIT 10;

