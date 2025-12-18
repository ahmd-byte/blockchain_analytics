-- Blockchain Analytics - Fraud Detection Queries
-- These queries are used by the fraud detection API endpoints

-- ===========================================
-- Fraud Wallet List Queries
-- ===========================================

-- Query 1: Get fraud wallets with pagination and filtering (parameterized)
-- Used in: GET /api/fraud/wallets
-- Parameters: Various filters
SELECT 
    wallet_address,
    fraud_score,
    is_suspicious,
    tx_count,
    total_value,
    last_activity,
    risk_category,
    flagged_reason,
    model_version,
    confidence_score
FROM `your-project-id.blockchain_ml.wallet_fraud_scores`
WHERE 1=1
    -- Optional filters (uncomment as needed):
    -- AND fraud_score >= @min_fraud_score
    -- AND fraud_score <= @max_fraud_score
    -- AND is_suspicious = @is_suspicious
    -- AND tx_count >= @min_tx_count
ORDER BY fraud_score DESC
LIMIT @page_size
OFFSET @offset;


-- Query 2: Count query for pagination
SELECT 
    COUNT(*) as total_count,
    COUNTIF(is_suspicious = TRUE) as suspicious_count
FROM `your-project-id.blockchain_ml.wallet_fraud_scores`
WHERE 1=1;
    -- Apply same filters as main query


-- ===========================================
-- High-Risk Wallet Analysis
-- ===========================================

-- Query 3: Get critical risk wallets (fraud_score >= 0.9)
SELECT 
    wallet_address,
    fraud_score,
    tx_count,
    total_value,
    last_activity,
    flagged_reason
FROM `your-project-id.blockchain_ml.wallet_fraud_scores`
WHERE fraud_score >= 0.9
ORDER BY total_value DESC;


-- Query 4: Get recently flagged suspicious wallets
SELECT 
    wallet_address,
    fraud_score,
    is_suspicious,
    tx_count,
    total_value,
    last_activity,
    updated_at
FROM `your-project-id.blockchain_ml.wallet_fraud_scores`
WHERE is_suspicious = TRUE
    AND updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY updated_at DESC
LIMIT 100;


-- ===========================================
-- Fraud Analytics Aggregations
-- ===========================================

-- Query 5: Daily new suspicious wallets trend
SELECT 
    DATE(updated_at) as date,
    COUNT(*) as new_suspicious_count,
    AVG(fraud_score) as avg_fraud_score
FROM `your-project-id.blockchain_ml.wallet_fraud_scores`
WHERE is_suspicious = TRUE
    AND DATE(updated_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY DATE(updated_at)
ORDER BY date DESC;


-- Query 6: Fraud score percentile distribution
SELECT 
    APPROX_QUANTILES(fraud_score, 100)[OFFSET(25)] as p25,
    APPROX_QUANTILES(fraud_score, 100)[OFFSET(50)] as p50_median,
    APPROX_QUANTILES(fraud_score, 100)[OFFSET(75)] as p75,
    APPROX_QUANTILES(fraud_score, 100)[OFFSET(90)] as p90,
    APPROX_QUANTILES(fraud_score, 100)[OFFSET(95)] as p95,
    APPROX_QUANTILES(fraud_score, 100)[OFFSET(99)] as p99
FROM `your-project-id.blockchain_ml.wallet_fraud_scores`;


-- Query 7: Suspicious wallet volume analysis
SELECT 
    CASE 
        WHEN total_value < 1000 THEN 'small (<1K)'
        WHEN total_value < 10000 THEN 'medium (1K-10K)'
        WHEN total_value < 100000 THEN 'large (10K-100K)'
        ELSE 'whale (>100K)'
    END as volume_tier,
    COUNT(*) as wallet_count,
    AVG(fraud_score) as avg_fraud_score,
    SUM(total_value) as total_volume
FROM `your-project-id.blockchain_ml.wallet_fraud_scores`
WHERE is_suspicious = TRUE
GROUP BY 
    CASE 
        WHEN total_value < 1000 THEN 'small (<1K)'
        WHEN total_value < 10000 THEN 'medium (1K-10K)'
        WHEN total_value < 100000 THEN 'large (10K-100K)'
        ELSE 'whale (>100K)'
    END
ORDER BY total_volume DESC;


-- ===========================================
-- Connected Suspicious Wallets Analysis
-- ===========================================

-- Query 8: Find wallets connected to suspicious addresses
-- Parameters: @suspicious_threshold (FLOAT64)
WITH suspicious_wallets AS (
    SELECT wallet_address
    FROM `your-project-id.blockchain_ml.wallet_fraud_scores`
    WHERE fraud_score >= @suspicious_threshold
),
connected_wallets AS (
    SELECT DISTINCT
        CASE 
            WHEN t.from_address IN (SELECT wallet_address FROM suspicious_wallets) 
            THEN t.to_address 
            ELSE t.from_address 
        END as connected_wallet
    FROM `your-project-id.blockchain_analytics.fact_transactions` t
    WHERE t.from_address IN (SELECT wallet_address FROM suspicious_wallets)
       OR t.to_address IN (SELECT wallet_address FROM suspicious_wallets)
)
SELECT 
    c.connected_wallet as wallet_address,
    f.fraud_score,
    f.is_suspicious,
    f.tx_count,
    f.total_value
FROM connected_wallets c
LEFT JOIN `your-project-id.blockchain_ml.wallet_fraud_scores` f
    ON c.connected_wallet = f.wallet_address
WHERE c.connected_wallet NOT IN (SELECT wallet_address FROM suspicious_wallets)
ORDER BY f.fraud_score DESC NULLS LAST
LIMIT 100;


-- ===========================================
-- Model Performance Tracking
-- ===========================================

-- Query 9: Track fraud score changes over time
SELECT 
    model_version,
    COUNT(*) as predictions_count,
    AVG(fraud_score) as avg_score,
    COUNTIF(is_suspicious) as suspicious_count,
    MIN(prediction_timestamp) as first_prediction,
    MAX(prediction_timestamp) as last_prediction
FROM `your-project-id.blockchain_ml.wallet_fraud_scores`
GROUP BY model_version
ORDER BY last_prediction DESC;

