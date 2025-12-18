-- Blockchain Analytics - Wallet Queries
-- These queries are used by the wallet API endpoints

-- ===========================================
-- Wallet Statistics Queries
-- ===========================================

-- Query 1: Get wallet statistics with fraud score (parameterized)
-- Used in: GET /api/wallet/{wallet_address}
-- Parameters: @wallet_address (STRING)
SELECT 
    w.wallet_address,
    COALESCE(w.total_transactions, 0) as total_transactions,
    COALESCE(w.total_volume, 0) as total_volume,
    w.first_transaction_date,
    w.last_transaction_date,
    COALESCE(w.unique_counterparties, 0) as unique_counterparties,
    COALESCE(w.avg_transaction_value, 0) as avg_transaction_value,
    COALESCE(f.fraud_score, 0) as fraud_score,
    COALESCE(f.is_suspicious, FALSE) as is_suspicious,
    f.risk_category,
    f.flagged_reason
FROM `your-project-id.blockchain_analytics.dim_wallet` w
LEFT JOIN `your-project-id.blockchain_ml.wallet_fraud_scores` f
    ON w.wallet_address = f.wallet_address
WHERE w.wallet_address = @wallet_address;


-- Query 2: Get daily transaction volumes for a wallet (parameterized)
-- Used in: GET /api/wallet/{wallet_address}
-- Parameters: @wallet_address (STRING), @days (INT64)
SELECT 
    DATE(transaction_timestamp) as date,
    COUNT(*) as transaction_count,
    COALESCE(SUM(value), 0) as total_value,
    COALESCE(SUM(CASE WHEN to_address = @wallet_address THEN value ELSE 0 END), 0) as inflow,
    COALESCE(SUM(CASE WHEN from_address = @wallet_address THEN value ELSE 0 END), 0) as outflow
FROM `your-project-id.blockchain_analytics.fact_transactions`
WHERE (from_address = @wallet_address OR to_address = @wallet_address)
    AND DATE(transaction_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
GROUP BY DATE(transaction_timestamp)
ORDER BY date DESC;


-- ===========================================
-- Wallet Transaction History
-- ===========================================

-- Query 3: Get recent transactions for a wallet (parameterized)
-- Parameters: @wallet_address (STRING), @limit (INT64)
SELECT 
    transaction_hash,
    block_number,
    transaction_timestamp,
    from_address,
    to_address,
    value,
    gas_used,
    transaction_fee,
    CASE 
        WHEN from_address = @wallet_address THEN 'outgoing'
        ELSE 'incoming'
    END as direction
FROM `your-project-id.blockchain_analytics.fact_transactions`
WHERE from_address = @wallet_address OR to_address = @wallet_address
ORDER BY transaction_timestamp DESC
LIMIT @limit;


-- Query 4: Get wallet's top counterparties
-- Parameters: @wallet_address (STRING), @limit (INT64)
SELECT 
    counterparty,
    COUNT(*) as transaction_count,
    SUM(value) as total_value,
    MAX(transaction_timestamp) as last_interaction
FROM (
    SELECT 
        to_address as counterparty,
        value,
        transaction_timestamp
    FROM `your-project-id.blockchain_analytics.fact_transactions`
    WHERE from_address = @wallet_address
    
    UNION ALL
    
    SELECT 
        from_address as counterparty,
        value,
        transaction_timestamp
    FROM `your-project-id.blockchain_analytics.fact_transactions`
    WHERE to_address = @wallet_address
)
GROUP BY counterparty
ORDER BY total_value DESC
LIMIT @limit;


-- ===========================================
-- Wallet Search and Discovery
-- ===========================================

-- Query 5: Search wallets by address prefix
-- Parameters: @address_prefix (STRING)
SELECT 
    wallet_address,
    total_transactions,
    total_volume,
    last_transaction_date
FROM `your-project-id.blockchain_analytics.dim_wallet`
WHERE wallet_address LIKE CONCAT(@address_prefix, '%')
ORDER BY total_volume DESC
LIMIT 10;


-- Query 6: Get most active wallets
SELECT 
    wallet_address,
    total_transactions,
    total_volume,
    unique_counterparties,
    first_transaction_date,
    last_transaction_date
FROM `your-project-id.blockchain_analytics.dim_wallet`
ORDER BY total_transactions DESC
LIMIT 100;


-- Query 7: Get wallets with highest volume
SELECT 
    wallet_address,
    total_transactions,
    total_volume,
    avg_transaction_value,
    max_transaction_value
FROM `your-project-id.blockchain_analytics.dim_wallet`
ORDER BY total_volume DESC
LIMIT 100;


