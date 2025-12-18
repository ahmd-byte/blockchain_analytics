{{
    config(
        materialized='incremental',
        schema='analytics',
        partition_by={
            'field': 'metric_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        incremental_strategy='merge',
        unique_key='metric_date'
    )
}}

/*
    Daily aggregated metrics for dashboards
    
    This model pre-aggregates daily metrics for fast dashboard queries:
    - Transaction counts and success rates
    - Value statistics
    - Gas metrics
    - Wallet activity
    - Moving averages and trends
*/

with daily_stats as (
    select
        transaction_date as metric_date,
        
        -- Transaction counts
        count(*) as total_transactions,
        countif(is_successful) as successful_transactions,
        countif(not is_successful) as failed_transactions,
        
        -- Transaction types
        countif(is_value_transfer) as value_transfers,
        countif(is_contract_call) as contract_calls,
        countif(is_contract_creation) as contract_creations,
        
        -- Value metrics
        sum(value_eth) as total_value_eth,
        avg(value_eth) as avg_value_eth,
        approx_quantiles(value_eth, 2)[offset(1)] as median_value_eth,
        max(value_eth) as max_value_eth,
        min(case when value_eth > 0 then value_eth end) as min_value_eth,
        
        -- Value distribution
        countif(value_eth = 0) as zero_value_count,
        countif(value_eth > 0 and value_eth < 0.001) as micro_transaction_count,
        countif(value_eth >= 0.001 and value_eth < 0.1) as small_transaction_count,
        countif(value_eth >= 0.1 and value_eth < 1) as medium_transaction_count,
        countif(value_eth >= 1 and value_eth < 10) as large_transaction_count,
        countif(value_eth >= 10) as whale_transaction_count,
        
        -- Gas metrics
        sum(gas_used) as total_gas_used,
        avg(gas_used) as avg_gas_used,
        avg(gas_price_gwei) as avg_gas_price_gwei,
        sum(gas_fee_eth) as total_gas_fees_eth,
        avg(gas_efficiency) as avg_gas_efficiency,
        
        -- Wallet metrics
        count(distinct from_address) as unique_senders,
        count(distinct to_address) as unique_receivers
        
    from {{ ref('fct_transactions') }}
    
    {% if is_incremental() %}
    where transaction_date >= date_sub(
        (select max(metric_date) from {{ this }}), 
        interval 7 day
    )
    {% endif %}
    
    group by transaction_date
),

unique_wallets as (
    select
        transaction_date as metric_date,
        count(distinct wallet) as unique_wallets
    from (
        select transaction_date, from_address as wallet 
        from {{ ref('fct_transactions') }}
        {% if is_incremental() %}
        where transaction_date >= date_sub(
            (select max(metric_date) from {{ this }}), 
            interval 7 day
        )
        {% endif %}
        
        union distinct
        
        select transaction_date, to_address as wallet 
        from {{ ref('fct_transactions') }}
        where to_address is not null
        {% if is_incremental() %}
        and transaction_date >= date_sub(
            (select max(metric_date) from {{ this }}), 
            interval 7 day
        )
        {% endif %}
    )
    group by transaction_date
),

combined as (
    select
        ds.*,
        uw.unique_wallets,
        ds.total_transactions / 24.0 as transactions_per_hour
    from daily_stats ds
    left join unique_wallets uw on ds.metric_date = uw.metric_date
),

with_moving_avg as (
    select
        *,
        -- Moving averages
        avg(total_transactions) over (
            order by metric_date
            rows between 6 preceding and current row
        ) as tx_count_7d_avg,
        avg(total_transactions) over (
            order by metric_date
            rows between 29 preceding and current row
        ) as tx_count_30d_avg,
        avg(total_value_eth) over (
            order by metric_date
            rows between 6 preceding and current row
        ) as value_7d_avg,
        avg(total_value_eth) over (
            order by metric_date
            rows between 29 preceding and current row
        ) as value_30d_avg,
        
        -- Previous day values for comparison
        lag(total_transactions) over (order by metric_date) as prev_day_tx_count,
        lag(total_value_eth) over (order by metric_date) as prev_day_value,
        lag(unique_wallets) over (order by metric_date) as prev_day_wallets
    from combined
)

select
    metric_date,
    
    -- Transaction counts
    total_transactions,
    successful_transactions,
    failed_transactions,
    safe_divide(successful_transactions, total_transactions) as success_rate,
    
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
    transactions_per_hour,
    
    -- Moving averages
    tx_count_7d_avg,
    tx_count_30d_avg,
    value_7d_avg,
    value_30d_avg,
    
    -- Day-over-day changes
    safe_divide(total_transactions - prev_day_tx_count, prev_day_tx_count) * 100 as tx_count_change_pct,
    safe_divide(total_value_eth - prev_day_value, prev_day_value) * 100 as value_change_pct,
    safe_divide(unique_wallets - prev_day_wallets, prev_day_wallets) * 100 as wallet_change_pct,
    
    -- Metadata
    current_timestamp() as loaded_at

from with_moving_avg

