{{
    config(
        materialized='incremental',
        schema='analytics',
        partition_by={
            'field': 'transaction_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['from_address', 'to_address', 'transaction_type'],
        incremental_strategy='merge',
        unique_key='transaction_key'
    )
}}

/*
    Transaction fact table
    
    This model creates the central fact table for transaction analytics with:
    - Foreign keys to dimension tables
    - Pre-calculated measures
    - Classification attributes
    
    Incremental load based on staged_at timestamp.
*/

with staged_transactions as (
    select * from {{ ref('stg_blockchain__transactions') }}
    
    {% if is_incremental() %}
    where staged_at > (select max(loaded_at) from {{ this }})
    {% endif %}
),

transformed as (
    select
        -- Primary key
        transaction_id as transaction_key,
        
        -- Natural keys
        transaction_hash,
        block_number,
        
        -- Foreign keys
        cast(format_date('%Y%m%d', transaction_date) as int64) as time_key,
        from_address as from_wallet_key,
        to_address as to_wallet_key,
        
        -- Date/time
        transaction_date,
        transaction_timestamp,
        transaction_hour,
        
        -- Addresses (denormalized for performance)
        from_address,
        to_address,
        
        -- Value measures
        coalesce(value_eth, 0) as value_eth,
        coalesce(value_wei, 0) as value_wei,
        cast(null as float64) as value_usd,  -- Would need price data
        
        -- Gas measures
        gas_limit,
        gas_used,
        cast(gas_price_wei as float64) / 1e9 as gas_price_gwei,
        gas_fee_eth,
        cast(null as float64) as gas_fee_usd,
        case 
            when gas_limit > 0 then cast(gas_used as float64) / gas_limit
            else null
        end as gas_efficiency,
        
        -- Transaction attributes
        transaction_type,
        is_contract_creation,
        is_contract_interaction as is_contract_call,
        coalesce(value_eth, 0) > 0 as is_value_transfer,
        is_successful,
        
        -- Contract details
        method_id,
        contract_address,
        
        -- Calculated measures
        coalesce(value_eth, 0) >= 10 as is_high_value,
        coalesce(value_eth, 0) < 0.001 and coalesce(value_eth, 0) > 0 as is_micro_transaction,
        case
            when value_eth is null or value_eth = 0 then 'zero'
            when value_eth < 0.001 then 'micro'
            when value_eth < 0.1 then 'small'
            when value_eth < 1 then 'medium'
            when value_eth < 10 then 'large'
            when value_eth < 100 then 'very_large'
            else 'whale'
        end as value_tier,
        
        -- Metadata
        source,
        staged_at,
        current_timestamp() as loaded_at
        
    from staged_transactions
)

select * from transformed

