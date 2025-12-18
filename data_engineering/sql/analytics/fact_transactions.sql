-- ============================================================================
-- FACT: FACT_TRANSACTIONS
-- ============================================================================
-- Purpose: Transaction fact table for analytics
-- Source: blockchain_staging.stg_transactions
-- Target: blockchain_analytics.fact_transactions
-- 
-- This table provides the central fact table for transaction analytics,
-- with foreign keys to dimension tables and pre-calculated measures.
-- ============================================================================

-- Create analytics dataset if not exists
CREATE SCHEMA IF NOT EXISTS `${project_id}.blockchain_analytics`
OPTIONS(
    description = "Analytics layer with fact and dimension tables",
    location = "US"
);

-- Create transaction fact table with partitioning
CREATE TABLE IF NOT EXISTS `${project_id}.blockchain_analytics.fact_transactions`
(
    -- Primary key
    transaction_key STRING NOT NULL,              -- Surrogate key
    
    -- Natural keys
    transaction_hash STRING NOT NULL,             -- Original transaction hash
    block_number INT64 NOT NULL,                  -- Block number
    
    -- Foreign keys to dimensions
    time_key INT64 NOT NULL,                      -- FK to dim_time
    from_wallet_key STRING NOT NULL,              -- FK to dim_wallet (sender)
    to_wallet_key STRING,                         -- FK to dim_wallet (receiver)
    
    -- Date/time for partitioning and filtering
    transaction_date DATE NOT NULL,               -- Transaction date
    transaction_timestamp TIMESTAMP NOT NULL,     -- Full timestamp
    transaction_hour INT64,                       -- Hour (0-23)
    
    -- Transaction addresses (denormalized for performance)
    from_address STRING NOT NULL,                 -- Sender address
    to_address STRING,                            -- Receiver address
    
    -- Value measures
    value_eth FLOAT64 NOT NULL,                   -- Transaction value in ETH
    value_wei INT64 NOT NULL,                     -- Transaction value in Wei
    value_usd FLOAT64,                            -- Value in USD (if available)
    
    -- Gas measures
    gas_limit INT64,                              -- Gas limit
    gas_used INT64,                               -- Actual gas used
    gas_price_gwei FLOAT64,                       -- Gas price in Gwei
    gas_fee_eth FLOAT64,                          -- Total gas fee in ETH
    gas_fee_usd FLOAT64,                          -- Gas fee in USD
    gas_efficiency FLOAT64,                       -- gas_used / gas_limit
    
    -- Transaction attributes
    transaction_type STRING,                      -- Type classification
    is_contract_creation BOOL,                    -- Is contract creation
    is_contract_call BOOL,                        -- Is contract interaction
    is_value_transfer BOOL,                       -- Has value transfer
    is_successful BOOL,                           -- Transaction succeeded
    
    -- Contract details
    method_id STRING,                             -- Method signature
    contract_address STRING,                      -- Created contract (if any)
    
    -- Calculated measures
    is_high_value BOOL,                           -- Value >= 10 ETH
    is_micro_transaction BOOL,                    -- Value < 0.001 ETH
    value_tier STRING,                            -- Value tier classification
    
    -- Data quality
    data_quality_score FLOAT64,                   -- Quality score
    
    -- Metadata
    source STRING,
    staged_at TIMESTAMP,
    loaded_at TIMESTAMP NOT NULL,
)
PARTITION BY transaction_date
CLUSTER BY from_address, to_address, transaction_type
OPTIONS(
    description = "Transaction fact table",
    require_partition_filter = false
);

-- ============================================================================
-- TRANSFORMATION QUERY
-- ============================================================================
-- Incremental load from staging

MERGE INTO `${project_id}.blockchain_analytics.fact_transactions` AS target
USING (
    SELECT
        -- Primary key
        transaction_id AS transaction_key,
        
        -- Natural keys
        transaction_hash,
        block_number,
        
        -- Foreign keys
        CAST(FORMAT_DATE('%Y%m%d', transaction_date) AS INT64) AS time_key,
        from_address AS from_wallet_key,
        to_address AS to_wallet_key,
        
        -- Date/time
        transaction_date,
        transaction_timestamp,
        transaction_hour,
        
        -- Addresses
        from_address,
        to_address,
        
        -- Value measures
        COALESCE(value_eth, 0) AS value_eth,
        COALESCE(value_wei, 0) AS value_wei,
        value_usd,  -- NULL if not available
        
        -- Gas measures
        gas_limit,
        gas_used,
        CAST(gas_price_wei AS FLOAT64) / 1e9 AS gas_price_gwei,
        gas_fee_eth,
        CAST(NULL AS FLOAT64) AS gas_fee_usd,  -- Would need price data
        CASE 
            WHEN gas_limit > 0 THEN CAST(gas_used AS FLOAT64) / gas_limit
            ELSE NULL
        END AS gas_efficiency,
        
        -- Transaction attributes
        transaction_type,
        is_contract_creation,
        is_contract_interaction AS is_contract_call,
        COALESCE(value_eth, 0) > 0 AS is_value_transfer,
        is_successful,
        
        -- Contract details
        method_id,
        contract_address,
        
        -- Calculated measures
        COALESCE(value_eth, 0) >= 10 AS is_high_value,
        COALESCE(value_eth, 0) < 0.001 AND COALESCE(value_eth, 0) > 0 AS is_micro_transaction,
        CASE
            WHEN value_eth IS NULL OR value_eth = 0 THEN 'zero'
            WHEN value_eth < 0.001 THEN 'micro'
            WHEN value_eth < 0.1 THEN 'small'
            WHEN value_eth < 1 THEN 'medium'
            WHEN value_eth < 10 THEN 'large'
            WHEN value_eth < 100 THEN 'very_large'
            ELSE 'whale'
        END AS value_tier,
        
        -- Data quality
        data_quality_score,
        
        -- Metadata
        source,
        staged_at,
        CURRENT_TIMESTAMP() AS loaded_at
        
    FROM `${project_id}.blockchain_staging.stg_transactions`
    WHERE data_quality_score >= 0.5  -- Filter low quality records
) AS source
ON target.transaction_key = source.transaction_key
WHEN MATCHED AND target.loaded_at < source.loaded_at THEN
    UPDATE SET
        transaction_hash = source.transaction_hash,
        block_number = source.block_number,
        time_key = source.time_key,
        from_wallet_key = source.from_wallet_key,
        to_wallet_key = source.to_wallet_key,
        transaction_date = source.transaction_date,
        transaction_timestamp = source.transaction_timestamp,
        transaction_hour = source.transaction_hour,
        from_address = source.from_address,
        to_address = source.to_address,
        value_eth = source.value_eth,
        value_wei = source.value_wei,
        value_usd = source.value_usd,
        gas_limit = source.gas_limit,
        gas_used = source.gas_used,
        gas_price_gwei = source.gas_price_gwei,
        gas_fee_eth = source.gas_fee_eth,
        gas_fee_usd = source.gas_fee_usd,
        gas_efficiency = source.gas_efficiency,
        transaction_type = source.transaction_type,
        is_contract_creation = source.is_contract_creation,
        is_contract_call = source.is_contract_call,
        is_value_transfer = source.is_value_transfer,
        is_successful = source.is_successful,
        method_id = source.method_id,
        contract_address = source.contract_address,
        is_high_value = source.is_high_value,
        is_micro_transaction = source.is_micro_transaction,
        value_tier = source.value_tier,
        data_quality_score = source.data_quality_score,
        source = source.source,
        staged_at = source.staged_at,
        loaded_at = source.loaded_at
WHEN NOT MATCHED THEN
    INSERT ROW;

-- ============================================================================
-- ADDITIONAL INDEXES/CLUSTERING (Recommendations for production)
-- ============================================================================
-- BigQuery handles indexing automatically, but clustering helps with:
-- - from_address: Queries filtering by sender
-- - to_address: Queries filtering by receiver
-- - transaction_type: Queries filtering by type
-- - Partitioning by transaction_date optimizes time-range queries

