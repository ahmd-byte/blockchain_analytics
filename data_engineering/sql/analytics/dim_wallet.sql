-- ============================================================================
-- DIMENSION: DIM_WALLET
-- ============================================================================
-- Purpose: Wallet dimension table for analytics
-- Source: blockchain_staging.stg_wallets
-- Target: blockchain_analytics.dim_wallet
-- 
-- This table provides wallet attributes for join-based analytics,
-- including wallet classifications, risk indicators, and activity metrics.
-- ============================================================================

-- Create analytics dataset if not exists
CREATE SCHEMA IF NOT EXISTS `${project_id}.blockchain_analytics`
OPTIONS(
    description = "Analytics layer with fact and dimension tables",
    location = "US"
);

-- Create wallet dimension table
CREATE TABLE IF NOT EXISTS `${project_id}.blockchain_analytics.dim_wallet`
(
    -- Primary keys
    wallet_key STRING NOT NULL,                   -- Surrogate key (same as address for simplicity)
    wallet_address STRING NOT NULL,               -- Ethereum address (lowercase)
    
    -- Basic attributes
    first_seen_date DATE,                         -- First activity date
    last_seen_date DATE,                          -- Last activity date
    account_age_days INT64,                       -- Days since first seen
    
    -- Classification
    wallet_type STRING,                           -- Type classification
    activity_level STRING,                        -- Activity level (high/medium/low)
    is_contract BOOL,                             -- Is smart contract
    is_exchange BOOL,                             -- Is exchange address
    is_known_entity BOOL,                         -- Is known entity (labeled)
    entity_name STRING,                           -- Entity name if known
    entity_category STRING,                       -- Entity category if known
    
    -- Current balance
    current_balance_eth FLOAT64,                  -- Current ETH balance
    balance_tier STRING,                          -- Balance tier (whale/large/medium/small/dust)
    
    -- Transaction statistics
    total_transactions INT64,                     -- Total transaction count
    total_sent INT64,                             -- Transactions sent
    total_received INT64,                         -- Transactions received
    unique_counterparties INT64,                  -- Unique addresses interacted with
    
    -- Value statistics
    total_value_sent_eth FLOAT64,                 -- Total ETH sent
    total_value_received_eth FLOAT64,             -- Total ETH received
    net_flow_eth FLOAT64,                         -- Net ETH flow (received - sent)
    avg_transaction_eth FLOAT64,                  -- Average transaction value
    
    -- Activity patterns
    avg_transactions_per_day FLOAT64,             -- Average daily transactions
    days_active INT64,                            -- Number of active days
    activity_consistency FLOAT64,                 -- Activity consistency score (0-1)
    
    -- Risk indicators
    fraud_score FLOAT64,                          -- Fraud score (0-1)
    risk_level STRING,                            -- Risk level (high/medium/low)
    is_suspicious BOOL,                           -- Flagged as suspicious
    suspicious_flags ARRAY<STRING>,               -- List of suspicious indicators
    
    -- Data quality
    data_quality_score FLOAT64,                   -- Data completeness score
    
    -- SCD Type 2 fields (for historical tracking)
    is_current BOOL NOT NULL,                     -- Current record flag
    valid_from TIMESTAMP NOT NULL,                -- Record validity start
    valid_to TIMESTAMP,                           -- Record validity end
    
    -- Metadata
    source STRING,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
)
CLUSTER BY wallet_address, wallet_type
OPTIONS(
    description = "Wallet dimension with current and historical attributes"
);

-- ============================================================================
-- TRANSFORMATION QUERY
-- ============================================================================
-- Implements SCD Type 2 for historical tracking

-- First, expire old records that have changed
UPDATE `${project_id}.blockchain_analytics.dim_wallet` target
SET 
    is_current = FALSE,
    valid_to = CURRENT_TIMESTAMP(),
    updated_at = CURRENT_TIMESTAMP()
WHERE target.is_current = TRUE
    AND EXISTS (
        SELECT 1 
        FROM `${project_id}.blockchain_staging.stg_wallets` source
        WHERE target.wallet_address = source.wallet_address
            AND (
                target.current_balance_eth != source.balance_eth
                OR target.total_transactions != source.total_transactions
                OR target.wallet_type != source.wallet_type
            )
    );

-- Then, insert new/updated records
INSERT INTO `${project_id}.blockchain_analytics.dim_wallet`
SELECT
    -- Primary keys
    wallet_address AS wallet_key,
    wallet_address,
    
    -- Basic attributes
    first_seen_date,
    last_seen_date,
    DATE_DIFF(CURRENT_DATE(), first_seen_date, DAY) AS account_age_days,
    
    -- Classification
    wallet_type,
    CASE
        WHEN transactions_per_day >= 10 THEN 'high'
        WHEN transactions_per_day >= 1 THEN 'medium'
        ELSE 'low'
    END AS activity_level,
    is_contract,
    is_exchange,
    FALSE AS is_known_entity,  -- Would be enriched from external data
    CAST(NULL AS STRING) AS entity_name,
    CAST(NULL AS STRING) AS entity_category,
    
    -- Current balance
    balance_eth AS current_balance_eth,
    CASE
        WHEN balance_eth >= 10000 THEN 'whale'
        WHEN balance_eth >= 1000 THEN 'large'
        WHEN balance_eth >= 10 THEN 'medium'
        WHEN balance_eth >= 0.1 THEN 'small'
        ELSE 'dust'
    END AS balance_tier,
    
    -- Transaction statistics
    total_transactions,
    total_transactions_out AS total_sent,
    total_transactions_in AS total_received,
    unique_counterparties,
    
    -- Value statistics
    total_value_out_eth AS total_value_sent_eth,
    total_value_in_eth AS total_value_received_eth,
    total_value_in_eth - total_value_out_eth AS net_flow_eth,
    avg_transaction_value_eth AS avg_transaction_eth,
    
    -- Activity patterns
    transactions_per_day AS avg_transactions_per_day,
    activity_days AS days_active,
    -- Activity consistency: ratio of active days to total account age
    CASE 
        WHEN DATE_DIFF(last_seen_date, first_seen_date, DAY) > 0
        THEN CAST(activity_days AS FLOAT64) / DATE_DIFF(last_seen_date, first_seen_date, DAY)
        ELSE 1.0
    END AS activity_consistency,
    
    -- Risk indicators (placeholder - would come from ML model)
    CAST(NULL AS FLOAT64) AS fraud_score,
    'unknown' AS risk_level,
    FALSE AS is_suspicious,
    CAST([] AS ARRAY<STRING>) AS suspicious_flags,
    
    -- Data quality
    data_quality_score,
    
    -- SCD Type 2 fields
    TRUE AS is_current,
    CURRENT_TIMESTAMP() AS valid_from,
    CAST(NULL AS TIMESTAMP) AS valid_to,
    
    -- Metadata
    source,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at

FROM `${project_id}.blockchain_staging.stg_wallets` source
WHERE NOT EXISTS (
    SELECT 1 
    FROM `${project_id}.blockchain_analytics.dim_wallet` target
    WHERE target.wallet_address = source.wallet_address
        AND target.is_current = TRUE
);

