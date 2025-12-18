-- ============================================================================
-- STAGING: STG_WALLETS
-- ============================================================================
-- Purpose: Clean and normalize raw wallet data
-- Source: blockchain_raw.raw_wallets
-- Target: blockchain_staging.stg_wallets
-- 
-- Transformations:
-- - Deduplicate by wallet_address (keep latest)
-- - Normalize addresses to lowercase
-- - Calculate derived metrics
-- - Add wallet classification
-- ============================================================================

-- Create staging dataset if not exists
CREATE SCHEMA IF NOT EXISTS `${project_id}.blockchain_staging`
OPTIONS(
    description = "Staging layer for cleaned blockchain data",
    location = "US"
);

-- Create staging wallets table
CREATE TABLE IF NOT EXISTS `${project_id}.blockchain_staging.stg_wallets`
(
    -- Primary identifier
    wallet_id STRING NOT NULL,                    -- Unique wallet identifier
    wallet_address STRING NOT NULL,               -- Ethereum address (lowercase)
    
    -- Activity timestamps
    first_seen_at TIMESTAMP,                      -- First transaction timestamp
    last_seen_at TIMESTAMP,                       -- Last transaction timestamp
    first_seen_date DATE,                         -- First transaction date
    last_seen_date DATE,                          -- Last transaction date
    
    -- Balance information
    balance_wei INT64,                            -- Current balance in Wei
    balance_eth FLOAT64,                          -- Current balance in Ether
    
    -- Transaction statistics
    total_transactions INT64,                     -- Total transaction count
    total_transactions_in INT64,                  -- Incoming transaction count
    total_transactions_out INT64,                 -- Outgoing transaction count
    
    -- Value statistics (Wei)
    total_value_in_wei INT64,                     -- Total incoming value
    total_value_out_wei INT64,                    -- Total outgoing value
    net_value_wei INT64,                          -- Net value (in - out)
    
    -- Value statistics (ETH)
    total_value_in_eth FLOAT64,                   -- Total incoming value in ETH
    total_value_out_eth FLOAT64,                  -- Total outgoing value in ETH
    net_value_eth FLOAT64,                        -- Net value in ETH
    
    -- Activity metrics
    unique_counterparties INT64,                  -- Unique addresses interacted with
    avg_transaction_value_eth FLOAT64,            -- Average transaction value
    max_transaction_value_eth FLOAT64,            -- Maximum transaction value
    
    -- Derived metrics
    activity_days INT64,                          -- Days between first and last activity
    transactions_per_day FLOAT64,                 -- Average transactions per day
    in_out_ratio FLOAT64,                         -- Ratio of incoming to outgoing
    
    -- Classification
    wallet_type STRING,                           -- Wallet classification
    is_contract BOOL,                             -- Is contract address
    is_exchange BOOL,                             -- Is exchange address (placeholder)
    
    -- Data quality
    data_quality_score FLOAT64,                   -- Quality score (0-1)
    
    -- Metadata
    source STRING,                                -- Data source
    ingested_at TIMESTAMP,                        -- Original ingestion timestamp
    staged_at TIMESTAMP NOT NULL,                 -- Staging timestamp
)
CLUSTER BY wallet_address
OPTIONS(
    description = "Staged and cleaned wallet data"
);

-- ============================================================================
-- TRANSFORMATION QUERY
-- ============================================================================
-- Run this to populate/refresh the staging table

MERGE INTO `${project_id}.blockchain_staging.stg_wallets` AS target
USING (
    WITH deduplicated AS (
        -- Deduplicate raw wallets, keeping the latest ingestion
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY wallet_address
                ORDER BY ingested_at DESC
            ) AS row_num
        FROM `${project_id}.blockchain_raw.raw_wallets`
    ),
    cleaned AS (
        SELECT
            -- Primary identifier
            LOWER(TRIM(wallet_address)) AS wallet_id,
            LOWER(TRIM(wallet_address)) AS wallet_address,
            
            -- Activity timestamps
            first_seen_timestamp AS first_seen_at,
            last_seen_timestamp AS last_seen_at,
            DATE(first_seen_timestamp) AS first_seen_date,
            DATE(last_seen_timestamp) AS last_seen_date,
            
            -- Balance information
            CAST(balance_wei AS INT64) AS balance_wei,
            CAST(balance_eth AS FLOAT64) AS balance_eth,
            
            -- Transaction statistics
            COALESCE(total_transactions_in, 0) + COALESCE(total_transactions_out, 0) AS total_transactions,
            CAST(COALESCE(total_transactions_in, 0) AS INT64) AS total_transactions_in,
            CAST(COALESCE(total_transactions_out, 0) AS INT64) AS total_transactions_out,
            
            -- Value statistics (Wei)
            CAST(COALESCE(total_value_in_wei, 0) AS INT64) AS total_value_in_wei,
            CAST(COALESCE(total_value_out_wei, 0) AS INT64) AS total_value_out_wei,
            CAST(COALESCE(total_value_in_wei, 0) AS INT64) - CAST(COALESCE(total_value_out_wei, 0) AS INT64) AS net_value_wei,
            
            -- Value statistics (ETH)
            CAST(COALESCE(total_value_in_eth, 0) AS FLOAT64) AS total_value_in_eth,
            CAST(COALESCE(total_value_out_eth, 0) AS FLOAT64) AS total_value_out_eth,
            CAST(COALESCE(total_value_in_eth, 0) AS FLOAT64) - CAST(COALESCE(total_value_out_eth, 0) AS FLOAT64) AS net_value_eth,
            
            -- Activity metrics
            CAST(COALESCE(unique_counterparties, 0) AS INT64) AS unique_counterparties,
            
            -- Average transaction value
            CASE 
                WHEN (COALESCE(total_transactions_in, 0) + COALESCE(total_transactions_out, 0)) > 0
                THEN (COALESCE(total_value_in_eth, 0) + COALESCE(total_value_out_eth, 0)) / 
                     (COALESCE(total_transactions_in, 0) + COALESCE(total_transactions_out, 0))
                ELSE 0
            END AS avg_transaction_value_eth,
            
            -- Max transaction value (would need to be calculated from transactions)
            CAST(NULL AS FLOAT64) AS max_transaction_value_eth,
            
            -- Activity days
            CASE 
                WHEN first_seen_timestamp IS NOT NULL AND last_seen_timestamp IS NOT NULL
                THEN DATE_DIFF(DATE(last_seen_timestamp), DATE(first_seen_timestamp), DAY) + 1
                ELSE 0
            END AS activity_days,
            
            -- Transactions per day
            CASE 
                WHEN first_seen_timestamp IS NOT NULL 
                    AND last_seen_timestamp IS NOT NULL
                    AND DATE_DIFF(DATE(last_seen_timestamp), DATE(first_seen_timestamp), DAY) > 0
                THEN (COALESCE(total_transactions_in, 0) + COALESCE(total_transactions_out, 0)) / 
                     NULLIF(DATE_DIFF(DATE(last_seen_timestamp), DATE(first_seen_timestamp), DAY), 0)
                ELSE CAST(COALESCE(total_transactions_in, 0) + COALESCE(total_transactions_out, 0) AS FLOAT64)
            END AS transactions_per_day,
            
            -- In/Out ratio
            CASE 
                WHEN COALESCE(total_transactions_out, 0) > 0
                THEN CAST(COALESCE(total_transactions_in, 0) AS FLOAT64) / COALESCE(total_transactions_out, 0)
                ELSE NULL
            END AS in_out_ratio,
            
            -- Wallet classification
            CASE
                WHEN COALESCE(is_contract, FALSE) THEN 'contract'
                WHEN COALESCE(total_transactions_out, 0) = 0 AND COALESCE(total_transactions_in, 0) > 0 THEN 'receive_only'
                WHEN COALESCE(total_transactions_in, 0) = 0 AND COALESCE(total_transactions_out, 0) > 0 THEN 'send_only'
                WHEN (COALESCE(total_transactions_in, 0) + COALESCE(total_transactions_out, 0)) > 1000 THEN 'high_activity'
                WHEN (COALESCE(total_transactions_in, 0) + COALESCE(total_transactions_out, 0)) > 100 THEN 'medium_activity'
                WHEN (COALESCE(total_transactions_in, 0) + COALESCE(total_transactions_out, 0)) > 10 THEN 'low_activity'
                ELSE 'minimal_activity'
            END AS wallet_type,
            
            COALESCE(is_contract, FALSE) AS is_contract,
            FALSE AS is_exchange,  -- Placeholder - would need external data
            
            -- Data quality score
            CASE
                WHEN wallet_address IS NULL THEN 0.0
                WHEN first_seen_timestamp IS NULL AND total_transactions_in = 0 AND total_transactions_out = 0 THEN 0.5
                ELSE 1.0
            END AS data_quality_score,
            
            -- Metadata
            source,
            ingested_at,
            CURRENT_TIMESTAMP() AS staged_at
            
        FROM deduplicated
        WHERE row_num = 1
            AND wallet_address IS NOT NULL
            AND TRIM(wallet_address) != ''
    )
    SELECT * FROM cleaned
) AS source
ON target.wallet_id = source.wallet_id
WHEN MATCHED AND target.staged_at < source.staged_at THEN
    UPDATE SET
        wallet_address = source.wallet_address,
        first_seen_at = source.first_seen_at,
        last_seen_at = source.last_seen_at,
        first_seen_date = source.first_seen_date,
        last_seen_date = source.last_seen_date,
        balance_wei = source.balance_wei,
        balance_eth = source.balance_eth,
        total_transactions = source.total_transactions,
        total_transactions_in = source.total_transactions_in,
        total_transactions_out = source.total_transactions_out,
        total_value_in_wei = source.total_value_in_wei,
        total_value_out_wei = source.total_value_out_wei,
        net_value_wei = source.net_value_wei,
        total_value_in_eth = source.total_value_in_eth,
        total_value_out_eth = source.total_value_out_eth,
        net_value_eth = source.net_value_eth,
        unique_counterparties = source.unique_counterparties,
        avg_transaction_value_eth = source.avg_transaction_value_eth,
        max_transaction_value_eth = source.max_transaction_value_eth,
        activity_days = source.activity_days,
        transactions_per_day = source.transactions_per_day,
        in_out_ratio = source.in_out_ratio,
        wallet_type = source.wallet_type,
        is_contract = source.is_contract,
        is_exchange = source.is_exchange,
        data_quality_score = source.data_quality_score,
        source = source.source,
        ingested_at = source.ingested_at,
        staged_at = source.staged_at
WHEN NOT MATCHED THEN
    INSERT ROW;

