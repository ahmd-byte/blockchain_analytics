-- ============================================================================
-- STAGING: STG_TRANSACTIONS
-- ============================================================================
-- Purpose: Clean and normalize raw transaction data
-- Source: blockchain_raw.raw_transactions
-- Target: blockchain_staging.stg_transactions
-- 
-- Transformations:
-- - Deduplicate by transaction_hash (keep latest ingestion)
-- - Normalize addresses to lowercase
-- - Convert timestamps to consistent format
-- - Add derived fields (transaction_type, is_contract_interaction)
-- - Validate data quality
-- ============================================================================

-- Create staging dataset if not exists
CREATE SCHEMA IF NOT EXISTS `${project_id}.blockchain_staging`
OPTIONS(
    description = "Staging layer for cleaned blockchain data",
    location = "US"
);

-- Create staging transactions table with partitioning
CREATE TABLE IF NOT EXISTS `${project_id}.blockchain_staging.stg_transactions`
(
    -- Primary identifiers
    transaction_id STRING NOT NULL,           -- Unique transaction identifier
    transaction_hash STRING NOT NULL,         -- Original transaction hash
    block_number INT64 NOT NULL,              -- Block number
    block_hash STRING,                        -- Block hash
    
    -- Timestamp fields
    transaction_timestamp TIMESTAMP NOT NULL, -- Transaction timestamp
    transaction_date DATE NOT NULL,           -- Transaction date (derived)
    transaction_hour INT64,                   -- Hour of transaction (0-23)
    day_of_week INT64,                        -- Day of week (1-7, Sunday=1)
    
    -- Address fields (normalized)
    from_address STRING NOT NULL,             -- Sender address (lowercase)
    to_address STRING,                        -- Receiver address (lowercase)
    
    -- Value fields
    value_wei INT64,                          -- Transaction value in Wei
    value_eth FLOAT64,                        -- Transaction value in Ether
    value_usd FLOAT64,                        -- Transaction value in USD (if available)
    
    -- Gas fields
    gas_limit INT64,                          -- Gas limit
    gas_price_wei INT64,                      -- Gas price in Wei
    gas_used INT64,                           -- Actual gas used
    gas_fee_wei INT64,                        -- Total gas fee (gas_used * gas_price)
    gas_fee_eth FLOAT64,                      -- Gas fee in Ether
    
    -- Transaction metadata
    nonce INT64,                              -- Transaction nonce
    transaction_index INT64,                  -- Position in block
    input_data STRING,                        -- Input data (truncated)
    input_data_length INT64,                  -- Length of input data
    
    -- Contract fields
    contract_address STRING,                  -- Created contract address
    is_contract_creation BOOL,                -- Is contract creation tx
    is_contract_interaction BOOL,             -- Interaction with contract
    method_id STRING,                         -- Method signature (first 4 bytes)
    
    -- Status fields
    is_successful BOOL,                       -- Transaction success status
    is_error BOOL,                            -- Error flag
    
    -- Classification
    transaction_type STRING,                  -- Type classification
    
    -- Data quality
    data_quality_score FLOAT64,               -- Quality score (0-1)
    data_quality_issues ARRAY<STRING>,        -- List of quality issues
    
    -- Metadata
    source STRING,                            -- Data source
    ingested_at TIMESTAMP,                    -- Original ingestion timestamp
    staged_at TIMESTAMP NOT NULL,             -- Staging timestamp
)
PARTITION BY transaction_date
CLUSTER BY from_address, to_address
OPTIONS(
    description = "Staged and cleaned transaction data",
    require_partition_filter = false
);

-- ============================================================================
-- TRANSFORMATION QUERY
-- ============================================================================
-- Run this to populate/refresh the staging table

MERGE INTO `${project_id}.blockchain_staging.stg_transactions` AS target
USING (
    WITH deduplicated AS (
        -- Deduplicate raw transactions, keeping the latest ingestion
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_hash
                ORDER BY ingested_at DESC
            ) AS row_num
        FROM `${project_id}.blockchain_raw.raw_transactions`
    ),
    cleaned AS (
        SELECT
            -- Primary identifiers
            CONCAT(transaction_hash, '-', CAST(block_number AS STRING)) AS transaction_id,
            LOWER(TRIM(transaction_hash)) AS transaction_hash,
            CAST(block_number AS INT64) AS block_number,
            LOWER(TRIM(block_hash)) AS block_hash,
            
            -- Timestamp transformations
            transaction_timestamp,
            DATE(transaction_timestamp) AS transaction_date,
            EXTRACT(HOUR FROM transaction_timestamp) AS transaction_hour,
            EXTRACT(DAYOFWEEK FROM transaction_timestamp) AS day_of_week,
            
            -- Address normalization
            LOWER(TRIM(from_address)) AS from_address,
            CASE 
                WHEN to_address IS NULL OR TRIM(to_address) = '' THEN NULL
                ELSE LOWER(TRIM(to_address))
            END AS to_address,
            
            -- Value fields
            CAST(value_wei AS INT64) AS value_wei,
            CAST(value_eth AS FLOAT64) AS value_eth,
            CAST(NULL AS FLOAT64) AS value_usd,  -- USD conversion would come from price data
            
            -- Gas fields
            CAST(gas AS INT64) AS gas_limit,
            CAST(gas_price AS INT64) AS gas_price_wei,
            CAST(gas_used AS INT64) AS gas_used,
            CAST(gas_used AS INT64) * CAST(gas_price AS INT64) AS gas_fee_wei,
            (CAST(gas_used AS FLOAT64) * CAST(gas_price AS FLOAT64)) / 1e18 AS gas_fee_eth,
            
            -- Transaction metadata
            CAST(nonce AS INT64) AS nonce,
            CAST(transaction_index AS INT64) AS transaction_index,
            -- Truncate large input data to first 1000 characters for storage
            CASE 
                WHEN input_data IS NULL OR input_data = '0x' THEN NULL
                ELSE LEFT(input_data, 1000)
            END AS input_data,
            CASE 
                WHEN input_data IS NULL THEN 0
                ELSE LENGTH(input_data)
            END AS input_data_length,
            
            -- Contract fields
            CASE 
                WHEN contract_address IS NULL OR TRIM(contract_address) = '' THEN NULL
                ELSE LOWER(TRIM(contract_address))
            END AS contract_address,
            -- Contract creation: to_address is NULL and contract_address is populated
            (to_address IS NULL OR TRIM(to_address) = '') 
                AND contract_address IS NOT NULL AS is_contract_creation,
            -- Contract interaction: has input data beyond simple transfer
            input_data IS NOT NULL 
                AND input_data != '0x' 
                AND LENGTH(input_data) > 10 AS is_contract_interaction,
            -- Method ID: first 4 bytes of input data (10 chars including 0x)
            CASE 
                WHEN input_data IS NOT NULL AND LENGTH(input_data) >= 10 
                THEN LEFT(input_data, 10)
                ELSE NULL
            END AS method_id,
            
            -- Status fields
            NOT COALESCE(is_error, FALSE) AS is_successful,
            COALESCE(is_error, FALSE) AS is_error,
            
            -- Classification based on transaction characteristics
            CASE
                WHEN (to_address IS NULL OR TRIM(to_address) = '') 
                    AND contract_address IS NOT NULL 
                    THEN 'contract_creation'
                WHEN input_data IS NOT NULL 
                    AND input_data != '0x' 
                    AND LENGTH(input_data) > 10 
                    THEN 'contract_call'
                WHEN value_wei > 0 
                    THEN 'value_transfer'
                ELSE 'other'
            END AS transaction_type,
            
            -- Data quality scoring
            CASE
                WHEN transaction_hash IS NULL THEN 0.0
                WHEN from_address IS NULL THEN 0.5
                WHEN transaction_timestamp IS NULL THEN 0.5
                ELSE 1.0
            END AS data_quality_score,
            
            -- Data quality issues array
            ARRAY_CONCAT(
                CASE WHEN transaction_hash IS NULL THEN ['missing_hash'] ELSE [] END,
                CASE WHEN from_address IS NULL THEN ['missing_from_address'] ELSE [] END,
                CASE WHEN transaction_timestamp IS NULL THEN ['missing_timestamp'] ELSE [] END,
                CASE WHEN value_wei IS NULL THEN ['missing_value'] ELSE [] END,
                CASE WHEN block_number IS NULL THEN ['missing_block'] ELSE [] END
            ) AS data_quality_issues,
            
            -- Metadata
            source,
            ingested_at,
            CURRENT_TIMESTAMP() AS staged_at
            
        FROM deduplicated
        WHERE row_num = 1
            -- Basic data quality filters
            AND transaction_hash IS NOT NULL
            AND block_number IS NOT NULL
    )
    SELECT * FROM cleaned
) AS source
ON target.transaction_id = source.transaction_id
WHEN MATCHED AND target.staged_at < source.staged_at THEN
    UPDATE SET
        transaction_hash = source.transaction_hash,
        block_number = source.block_number,
        block_hash = source.block_hash,
        transaction_timestamp = source.transaction_timestamp,
        transaction_date = source.transaction_date,
        transaction_hour = source.transaction_hour,
        day_of_week = source.day_of_week,
        from_address = source.from_address,
        to_address = source.to_address,
        value_wei = source.value_wei,
        value_eth = source.value_eth,
        value_usd = source.value_usd,
        gas_limit = source.gas_limit,
        gas_price_wei = source.gas_price_wei,
        gas_used = source.gas_used,
        gas_fee_wei = source.gas_fee_wei,
        gas_fee_eth = source.gas_fee_eth,
        nonce = source.nonce,
        transaction_index = source.transaction_index,
        input_data = source.input_data,
        input_data_length = source.input_data_length,
        contract_address = source.contract_address,
        is_contract_creation = source.is_contract_creation,
        is_contract_interaction = source.is_contract_interaction,
        method_id = source.method_id,
        is_successful = source.is_successful,
        is_error = source.is_error,
        transaction_type = source.transaction_type,
        data_quality_score = source.data_quality_score,
        data_quality_issues = source.data_quality_issues,
        source = source.source,
        ingested_at = source.ingested_at,
        staged_at = source.staged_at
WHEN NOT MATCHED THEN
    INSERT ROW;

