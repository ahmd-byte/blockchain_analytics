{{
    config(
        materialized='table',
        schema='analytics',
        cluster_by=['wallet_type', 'risk_level']
    )
}}

/*
    Wallet dimension table
    
    This model creates a wallet dimension with:
    - Classification attributes
    - Activity metrics
    - Risk indicators
    - Balance tiers
*/

with staged_wallets as (
    select * from {{ ref('stg_blockchain__wallets') }}
),

-- Calculate additional metrics from transactions
transaction_metrics as (
    select
        from_address as wallet_address,
        count(*) as total_sent,
        sum(value_eth) as total_value_sent,
        avg(value_eth) as avg_sent_value,
        max(value_eth) as max_sent_value
    from {{ ref('stg_blockchain__transactions') }}
    group by from_address
    
    union all
    
    select
        to_address as wallet_address,
        count(*) as total_received,
        sum(value_eth) as total_value_received,
        avg(value_eth) as avg_received_value,
        max(value_eth) as max_received_value
    from {{ ref('stg_blockchain__transactions') }}
    where to_address is not null
    group by to_address
),

aggregated_tx_metrics as (
    select
        wallet_address,
        sum(total_sent) as total_sent,
        sum(total_value_sent) as total_value_sent,
        max(avg_sent_value) as avg_sent_value,
        max(max_sent_value) as max_sent_value
    from transaction_metrics
    group by wallet_address
),

enriched_wallets as (
    select
        -- Primary keys
        w.wallet_address as wallet_key,
        w.wallet_address,
        
        -- Basic attributes
        w.first_seen_date,
        w.last_seen_date,
        date_diff(current_date(), w.first_seen_date, day) as account_age_days,
        
        -- Classification
        w.wallet_type,
        case
            when w.transactions_per_day >= 10 then 'high'
            when w.transactions_per_day >= 1 then 'medium'
            else 'low'
        end as activity_level,
        w.is_contract,
        false as is_exchange,  -- Would need external data
        
        -- Current balance
        w.balance_eth as current_balance_eth,
        case
            when w.balance_eth >= 10000 then 'whale'
            when w.balance_eth >= 1000 then 'large'
            when w.balance_eth >= 10 then 'medium'
            when w.balance_eth >= 0.1 then 'small'
            else 'dust'
        end as balance_tier,
        
        -- Transaction statistics
        w.total_transactions,
        w.total_transactions_out as total_sent,
        w.total_transactions_in as total_received,
        w.unique_counterparties,
        
        -- Value statistics
        w.total_value_out_eth as total_value_sent_eth,
        w.total_value_in_eth as total_value_received_eth,
        w.total_value_in_eth - w.total_value_out_eth as net_flow_eth,
        w.avg_transaction_value_eth as avg_transaction_eth,
        coalesce(t.max_sent_value, 0) as max_transaction_eth,
        
        -- Activity patterns
        w.transactions_per_day as avg_transactions_per_day,
        w.activity_days as days_active,
        case 
            when date_diff(w.last_seen_date, w.first_seen_date, day) > 0
            then cast(w.activity_days as float64) / date_diff(w.last_seen_date, w.first_seen_date, day)
            else 1.0
        end as activity_consistency,
        
        -- Risk indicators (placeholder - would come from ML model)
        cast(null as float64) as fraud_score,
        'unknown' as risk_level,
        false as is_suspicious,
        
        -- Metadata
        w.source,
        current_timestamp() as created_at,
        current_timestamp() as updated_at
        
    from staged_wallets w
    left join aggregated_tx_metrics t on w.wallet_address = t.wallet_address
)

select * from enriched_wallets

