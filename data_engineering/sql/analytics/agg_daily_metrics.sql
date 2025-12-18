-- ============================================================================
-- AGGREGATION: AGG_DAILY_METRICS
-- ============================================================================
-- Purpose: Pre-aggregated daily metrics for dashboard performance
-- Source: blockchain_analytics.fact_transactions
-- Target: blockchain_analytics.agg_daily_metrics
-- 
-- This table provides daily aggregate metrics for fast dashboard queries.
-- ============================================================================

-- Create analytics dataset if not exists
CREATE SCHEMA IF NOT EXISTS `${project_id}.blockchain_analytics`
OPTIONS(
    description = "Analytics layer with fact and dimension tables",
    location = "US"
);

-- Create daily metrics aggregation table
CREATE TABLE IF NOT EXISTS `${project_id}.blockchain_analytics.agg_daily_metrics`
(
    -- Primary key
    metric_date DATE NOT NULL,                    -- Aggregation date
    
    -- Transaction counts
    total_transactions INT64,                     -- Total transaction count
    successful_transactions INT64,                -- Successful transactions
    failed_transactions INT64,                    -- Failed transactions
    success_rate FLOAT64,                         -- Success rate (0-1)
    
    -- Transaction types
    value_transfers INT64,                        -- Value transfer count
    contract_calls INT64,                         -- Contract call count
    contract_creations INT64,                     -- Contract creation count
    
    -- Value metrics
    total_value_eth FLOAT64,                      -- Total ETH transferred
    avg_value_eth FLOAT64,                        -- Average transaction value
    median_value_eth FLOAT64,                     -- Median transaction value
    max_value_eth FLOAT64,                        -- Maximum transaction value
    min_value_eth FLOAT64,                        -- Minimum transaction value (non-zero)
    
    -- Value distribution
    zero_value_count INT64,                       -- Zero-value transactions
    micro_transaction_count INT64,                -- < 0.001 ETH
    small_transaction_count INT64,                -- 0.001 - 0.1 ETH
    medium_transaction_count INT64,               -- 0.1 - 1 ETH
    large_transaction_count INT64,                -- 1 - 10 ETH
    whale_transaction_count INT64,                -- > 10 ETH
    
    -- Gas metrics
    total_gas_used INT64,                         -- Total gas consumed
    avg_gas_used FLOAT64,                         -- Average gas per transaction
    avg_gas_price_gwei FLOAT64,                   -- Average gas price
    total_gas_fees_eth FLOAT64,                   -- Total gas fees
    avg_gas_efficiency FLOAT64,                   -- Average gas efficiency
    
    -- Wallet metrics
    unique_senders INT64,                         -- Unique from addresses
    unique_receivers INT64,                       -- Unique to addresses
    unique_wallets INT64,                         -- Total unique wallets
    new_wallets INT64,                            -- First-time wallets
    
    -- Network activity
    transactions_per_hour FLOAT64,                -- Average TPS by hour
    peak_hour INT64,                              -- Hour with most transactions
    peak_hour_transactions INT64,                 -- Transactions in peak hour
    
    -- Comparisons
    tx_count_7d_avg FLOAT64,                      -- 7-day moving average
    tx_count_30d_avg FLOAT64,                     -- 30-day moving average
    value_7d_avg FLOAT64,                         -- 7-day value average
    value_30d_avg FLOAT64,                        -- 30-day value average
    
    -- Day-over-day changes
    tx_count_change_pct FLOAT64,                  -- % change from previous day
    value_change_pct FLOAT64,                     -- % change in value
    wallet_change_pct FLOAT64,                    -- % change in unique wallets
    
    -- Metadata
    loaded_at TIMESTAMP NOT NULL,
)
PARTITION BY metric_date
OPTIONS(
    description = "Daily aggregated metrics for dashboards"
);

-- ============================================================================
-- TRANSFORMATION QUERY
-- ============================================================================

MERGE INTO `${project_id}.blockchain_analytics.agg_daily_metrics` AS target
USING (
    WITH daily_stats AS (
        SELECT
            transaction_date AS metric_date,
            
            -- Transaction counts
            COUNT(*) AS total_transactions,
            COUNTIF(is_successful) AS successful_transactions,
            COUNTIF(NOT is_successful) AS failed_transactions,
            
            -- Transaction types
            COUNTIF(is_value_transfer) AS value_transfers,
            COUNTIF(is_contract_call) AS contract_calls,
            COUNTIF(is_contract_creation) AS contract_creations,
            
            -- Value metrics
            SUM(value_eth) AS total_value_eth,
            AVG(value_eth) AS avg_value_eth,
            APPROX_QUANTILES(value_eth, 2)[OFFSET(1)] AS median_value_eth,
            MAX(value_eth) AS max_value_eth,
            MIN(CASE WHEN value_eth > 0 THEN value_eth END) AS min_value_eth,
            
            -- Value distribution
            COUNTIF(value_eth = 0) AS zero_value_count,
            COUNTIF(value_eth > 0 AND value_eth < 0.001) AS micro_transaction_count,
            COUNTIF(value_eth >= 0.001 AND value_eth < 0.1) AS small_transaction_count,
            COUNTIF(value_eth >= 0.1 AND value_eth < 1) AS medium_transaction_count,
            COUNTIF(value_eth >= 1 AND value_eth < 10) AS large_transaction_count,
            COUNTIF(value_eth >= 10) AS whale_transaction_count,
            
            -- Gas metrics
            SUM(gas_used) AS total_gas_used,
            AVG(gas_used) AS avg_gas_used,
            AVG(gas_price_gwei) AS avg_gas_price_gwei,
            SUM(gas_fee_eth) AS total_gas_fees_eth,
            AVG(gas_efficiency) AS avg_gas_efficiency,
            
            -- Wallet metrics
            COUNT(DISTINCT from_address) AS unique_senders,
            COUNT(DISTINCT to_address) AS unique_receivers,
            COUNT(DISTINCT from_address) + COUNT(DISTINCT to_address) AS unique_wallets_raw,
            
            -- Hourly distribution
            COUNT(*) / 24.0 AS transactions_per_hour,
            
        FROM `${project_id}.blockchain_analytics.fact_transactions`
        GROUP BY transaction_date
    ),
    hourly_peak AS (
        SELECT
            transaction_date,
            transaction_hour AS peak_hour,
            COUNT(*) AS peak_hour_transactions,
            ROW_NUMBER() OVER (PARTITION BY transaction_date ORDER BY COUNT(*) DESC) AS rn
        FROM `${project_id}.blockchain_analytics.fact_transactions`
        GROUP BY transaction_date, transaction_hour
    ),
    unique_wallets AS (
        SELECT
            transaction_date,
            COUNT(DISTINCT wallet) AS unique_wallets
        FROM (
            SELECT transaction_date, from_address AS wallet 
            FROM `${project_id}.blockchain_analytics.fact_transactions`
            UNION DISTINCT
            SELECT transaction_date, to_address AS wallet 
            FROM `${project_id}.blockchain_analytics.fact_transactions`
            WHERE to_address IS NOT NULL
        )
        GROUP BY transaction_date
    ),
    with_peak AS (
        SELECT
            ds.*,
            hp.peak_hour,
            hp.peak_hour_transactions,
            uw.unique_wallets
        FROM daily_stats ds
        LEFT JOIN hourly_peak hp 
            ON ds.metric_date = hp.transaction_date AND hp.rn = 1
        LEFT JOIN unique_wallets uw 
            ON ds.metric_date = uw.transaction_date
    ),
    with_moving_avg AS (
        SELECT
            *,
            AVG(total_transactions) OVER (
                ORDER BY metric_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS tx_count_7d_avg,
            AVG(total_transactions) OVER (
                ORDER BY metric_date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) AS tx_count_30d_avg,
            AVG(total_value_eth) OVER (
                ORDER BY metric_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS value_7d_avg,
            AVG(total_value_eth) OVER (
                ORDER BY metric_date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) AS value_30d_avg,
            LAG(total_transactions) OVER (ORDER BY metric_date) AS prev_day_tx_count,
            LAG(total_value_eth) OVER (ORDER BY metric_date) AS prev_day_value,
            LAG(unique_wallets) OVER (ORDER BY metric_date) AS prev_day_wallets
        FROM with_peak
    )
    SELECT
        metric_date,
        
        -- Transaction counts
        total_transactions,
        successful_transactions,
        failed_transactions,
        SAFE_DIVIDE(successful_transactions, total_transactions) AS success_rate,
        
        -- Transaction types
        value_transfers,
        contract_calls,
        contract_creations,
        
        -- Value metrics
        total_value_eth,
        avg_value_eth,
        median_value_eth,
        max_value_eth,
        min_value_eth,
        
        -- Value distribution
        zero_value_count,
        micro_transaction_count,
        small_transaction_count,
        medium_transaction_count,
        large_transaction_count,
        whale_transaction_count,
        
        -- Gas metrics
        total_gas_used,
        avg_gas_used,
        avg_gas_price_gwei,
        total_gas_fees_eth,
        avg_gas_efficiency,
        
        -- Wallet metrics
        unique_senders,
        unique_receivers,
        unique_wallets,
        CAST(NULL AS INT64) AS new_wallets,  -- Would need historical comparison
        
        -- Network activity
        transactions_per_hour,
        peak_hour,
        peak_hour_transactions,
        
        -- Moving averages
        tx_count_7d_avg,
        tx_count_30d_avg,
        value_7d_avg,
        value_30d_avg,
        
        -- Day-over-day changes
        SAFE_DIVIDE(total_transactions - prev_day_tx_count, prev_day_tx_count) * 100 AS tx_count_change_pct,
        SAFE_DIVIDE(total_value_eth - prev_day_value, prev_day_value) * 100 AS value_change_pct,
        SAFE_DIVIDE(unique_wallets - prev_day_wallets, prev_day_wallets) * 100 AS wallet_change_pct,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS loaded_at
        
    FROM with_moving_avg
) AS source
ON target.metric_date = source.metric_date
WHEN MATCHED THEN
    UPDATE SET
        total_transactions = source.total_transactions,
        successful_transactions = source.successful_transactions,
        failed_transactions = source.failed_transactions,
        success_rate = source.success_rate,
        value_transfers = source.value_transfers,
        contract_calls = source.contract_calls,
        contract_creations = source.contract_creations,
        total_value_eth = source.total_value_eth,
        avg_value_eth = source.avg_value_eth,
        median_value_eth = source.median_value_eth,
        max_value_eth = source.max_value_eth,
        min_value_eth = source.min_value_eth,
        zero_value_count = source.zero_value_count,
        micro_transaction_count = source.micro_transaction_count,
        small_transaction_count = source.small_transaction_count,
        medium_transaction_count = source.medium_transaction_count,
        large_transaction_count = source.large_transaction_count,
        whale_transaction_count = source.whale_transaction_count,
        total_gas_used = source.total_gas_used,
        avg_gas_used = source.avg_gas_used,
        avg_gas_price_gwei = source.avg_gas_price_gwei,
        total_gas_fees_eth = source.total_gas_fees_eth,
        avg_gas_efficiency = source.avg_gas_efficiency,
        unique_senders = source.unique_senders,
        unique_receivers = source.unique_receivers,
        unique_wallets = source.unique_wallets,
        new_wallets = source.new_wallets,
        transactions_per_hour = source.transactions_per_hour,
        peak_hour = source.peak_hour,
        peak_hour_transactions = source.peak_hour_transactions,
        tx_count_7d_avg = source.tx_count_7d_avg,
        tx_count_30d_avg = source.tx_count_30d_avg,
        value_7d_avg = source.value_7d_avg,
        value_30d_avg = source.value_30d_avg,
        tx_count_change_pct = source.tx_count_change_pct,
        value_change_pct = source.value_change_pct,
        wallet_change_pct = source.wallet_change_pct,
        loaded_at = source.loaded_at
WHEN NOT MATCHED THEN
    INSERT ROW;

