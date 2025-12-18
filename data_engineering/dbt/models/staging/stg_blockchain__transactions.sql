{{
    config(
        materialized='view',
        schema='staging'
    )
}}

/*
    Staging model for raw transactions
    
    This model:
    - Deduplicates raw transaction data
    - Normalizes addresses to lowercase
    - Converts timestamps to consistent format
    - Adds basic derived fields
    
    Source: blockchain_raw.raw_transactions
*/

with source as (
    select * from {{ source('blockchain_raw', 'raw_transactions') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by transaction_hash
            order by ingested_at desc
        ) as row_num
    from source
),

cleaned as (
    select
        -- Primary identifiers
        concat(transaction_hash, '-', cast(block_number as string)) as transaction_id,
        lower(trim(transaction_hash)) as transaction_hash,
        cast(block_number as int64) as block_number,
        lower(trim(block_hash)) as block_hash,
        
        -- Timestamp transformations
        transaction_timestamp,
        date(transaction_timestamp) as transaction_date,
        extract(hour from transaction_timestamp) as transaction_hour,
        extract(dayofweek from transaction_timestamp) as day_of_week,
        
        -- Address normalization
        lower(trim(from_address)) as from_address,
        case 
            when to_address is null or trim(to_address) = '' then null
            else lower(trim(to_address))
        end as to_address,
        
        -- Value fields
        cast(value_wei as int64) as value_wei,
        cast(value_eth as float64) as value_eth,
        
        -- Gas fields
        cast(gas as int64) as gas_limit,
        cast(gas_price as int64) as gas_price_wei,
        cast(gas_used as int64) as gas_used,
        cast(gas_used as int64) * cast(gas_price as int64) as gas_fee_wei,
        (cast(gas_used as float64) * cast(gas_price as float64)) / 1e18 as gas_fee_eth,
        
        -- Transaction metadata
        cast(nonce as int64) as nonce,
        cast(transaction_index as int64) as transaction_index,
        case 
            when input_data is null or input_data = '0x' then null
            else left(input_data, 1000)
        end as input_data,
        case 
            when input_data is null then 0
            else length(input_data)
        end as input_data_length,
        
        -- Contract fields
        case 
            when contract_address is null or trim(contract_address) = '' then null
            else lower(trim(contract_address))
        end as contract_address,
        
        -- Derived boolean fields
        (to_address is null or trim(to_address) = '') 
            and contract_address is not null as is_contract_creation,
        input_data is not null 
            and input_data != '0x' 
            and length(input_data) > 10 as is_contract_interaction,
        
        -- Method ID extraction
        case 
            when input_data is not null and length(input_data) >= 10 
            then left(input_data, 10)
            else null
        end as method_id,
        
        -- Status fields
        not coalesce(is_error, false) as is_successful,
        coalesce(is_error, false) as is_error,
        
        -- Transaction type classification
        case
            when (to_address is null or trim(to_address) = '') 
                and contract_address is not null 
                then 'contract_creation'
            when input_data is not null 
                and input_data != '0x' 
                and length(input_data) > 10 
                then 'contract_call'
            when value_wei > 0 
                then 'value_transfer'
            else 'other'
        end as transaction_type,
        
        -- Metadata
        source,
        ingested_at,
        current_timestamp() as staged_at
        
    from deduplicated
    where row_num = 1
        and transaction_hash is not null
        and block_number is not null
)

select * from cleaned

