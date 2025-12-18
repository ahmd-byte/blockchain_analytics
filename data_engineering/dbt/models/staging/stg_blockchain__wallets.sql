{{
    config(
        materialized='view',
        schema='staging'
    )
}}

/*
    Staging model for raw wallet data
    
    This model:
    - Deduplicates raw wallet data
    - Normalizes addresses to lowercase
    - Calculates derived metrics
    - Adds wallet classification
    
    Source: blockchain_raw.raw_wallets
*/

with source as (
    select * from {{ source('blockchain_raw', 'raw_wallets') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by wallet_address
            order by ingested_at desc
        ) as row_num
    from source
),

cleaned as (
    select
        -- Primary identifier
        lower(trim(wallet_address)) as wallet_id,
        lower(trim(wallet_address)) as wallet_address,
        
        -- Activity timestamps
        first_seen_timestamp as first_seen_at,
        last_seen_timestamp as last_seen_at,
        date(first_seen_timestamp) as first_seen_date,
        date(last_seen_timestamp) as last_seen_date,
        
        -- Balance information
        cast(balance_wei as int64) as balance_wei,
        cast(balance_eth as float64) as balance_eth,
        
        -- Transaction statistics
        coalesce(total_transactions_in, 0) + coalesce(total_transactions_out, 0) as total_transactions,
        cast(coalesce(total_transactions_in, 0) as int64) as total_transactions_in,
        cast(coalesce(total_transactions_out, 0) as int64) as total_transactions_out,
        
        -- Value statistics
        cast(coalesce(total_value_in_wei, 0) as int64) as total_value_in_wei,
        cast(coalesce(total_value_out_wei, 0) as int64) as total_value_out_wei,
        cast(coalesce(total_value_in_eth, 0) as float64) as total_value_in_eth,
        cast(coalesce(total_value_out_eth, 0) as float64) as total_value_out_eth,
        
        -- Activity metrics
        cast(coalesce(unique_counterparties, 0) as int64) as unique_counterparties,
        
        -- Average transaction value
        case 
            when (coalesce(total_transactions_in, 0) + coalesce(total_transactions_out, 0)) > 0
            then (coalesce(total_value_in_eth, 0) + coalesce(total_value_out_eth, 0)) / 
                 (coalesce(total_transactions_in, 0) + coalesce(total_transactions_out, 0))
            else 0
        end as avg_transaction_value_eth,
        
        -- Activity days
        case 
            when first_seen_timestamp is not null and last_seen_timestamp is not null
            then date_diff(date(last_seen_timestamp), date(first_seen_timestamp), day) + 1
            else 0
        end as activity_days,
        
        -- Transactions per day
        case 
            when first_seen_timestamp is not null 
                and last_seen_timestamp is not null
                and date_diff(date(last_seen_timestamp), date(first_seen_timestamp), day) > 0
            then (coalesce(total_transactions_in, 0) + coalesce(total_transactions_out, 0)) / 
                 nullif(date_diff(date(last_seen_timestamp), date(first_seen_timestamp), day), 0)
            else cast(coalesce(total_transactions_in, 0) + coalesce(total_transactions_out, 0) as float64)
        end as transactions_per_day,
        
        -- Wallet classification
        case
            when coalesce(is_contract, false) then 'contract'
            when coalesce(total_transactions_out, 0) = 0 and coalesce(total_transactions_in, 0) > 0 then 'receive_only'
            when coalesce(total_transactions_in, 0) = 0 and coalesce(total_transactions_out, 0) > 0 then 'send_only'
            when (coalesce(total_transactions_in, 0) + coalesce(total_transactions_out, 0)) > 1000 then 'high_activity'
            when (coalesce(total_transactions_in, 0) + coalesce(total_transactions_out, 0)) > 100 then 'medium_activity'
            when (coalesce(total_transactions_in, 0) + coalesce(total_transactions_out, 0)) > 10 then 'low_activity'
            else 'minimal_activity'
        end as wallet_type,
        
        coalesce(is_contract, false) as is_contract,
        
        -- Metadata
        source,
        ingested_at,
        current_timestamp() as staged_at
        
    from deduplicated
    where row_num = 1
        and wallet_address is not null
        and trim(wallet_address) != ''
)

select * from cleaned

