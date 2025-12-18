"""
Blockchain Analytics Daily Pipeline DAG.

This DAG orchestrates the daily data engineering pipeline:
1. Ingest new transactions and wallet data
2. Run staging transformations
3. Build analytics tables
4. Generate daily aggregations
5. Trigger ML model updates (optional)

Schedule: Daily at 2:00 AM UTC
"""

from datetime import datetime, timedelta
from typing import Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
)
from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryTableExistenceSensor,
)
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable


# ============================================================================
# DAG Configuration
# ============================================================================

# Default arguments for all tasks
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-alerts@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Configuration variables (set in Airflow Variables)
PROJECT_ID = Variable.get('gcp_project_id', default_var='your-project-id')
DATASET_RAW = Variable.get('bq_dataset_raw', default_var='blockchain_raw')
DATASET_STAGING = Variable.get('bq_dataset_staging', default_var='blockchain_staging')
DATASET_ANALYTICS = Variable.get('bq_dataset_analytics', default_var='blockchain_analytics')
DBT_PROJECT_DIR = Variable.get('dbt_project_dir', default_var='/opt/airflow/dbt/blockchain_analytics')

# Ingestion configuration
ETHERSCAN_API_KEY = Variable.get('etherscan_api_key', default_var='')
MONITORED_ADDRESSES = Variable.get(
    'monitored_addresses', 
    default_var='[]',
    deserialize_json=True
)


# ============================================================================
# Helper Functions
# ============================================================================

def get_ingestion_params(**context) -> Dict:
    """
    Get parameters for ingestion tasks.
    
    Calculates the date range for incremental ingestion based on
    the execution date.
    """
    execution_date = context['execution_date']
    
    # Ingest data for the previous day
    data_date = execution_date - timedelta(days=1)
    
    return {
        'data_date': data_date.strftime('%Y-%m-%d'),
        'project_id': PROJECT_ID,
        'dataset_raw': DATASET_RAW,
    }


def check_new_data_available(**context) -> str:
    """
    Check if there's new data to process.
    
    Returns the next task to execute based on data availability.
    """
    from google.cloud import bigquery
    
    ti = context['ti']
    params = ti.xcom_pull(task_ids='get_ingestion_params')
    data_date = params['data_date']
    
    client = bigquery.Client(project=PROJECT_ID)
    
    query = f"""
    SELECT COUNT(*) as record_count
    FROM `{PROJECT_ID}.{DATASET_RAW}.raw_transactions`
    WHERE DATE(ingested_at) = '{data_date}'
    """
    
    result = list(client.query(query).result())
    record_count = result[0].record_count if result else 0
    
    if record_count > 0:
        return 'transform_group.run_staging_transactions'
    else:
        return 'skip_processing'


def run_data_quality_checks(**context) -> bool:
    """
    Run data quality checks on staged data.
    
    Returns True if all checks pass.
    """
    from google.cloud import bigquery
    
    client = bigquery.Client(project=PROJECT_ID)
    
    checks = [
        # Check for null transaction hashes
        f"""
        SELECT COUNT(*) as null_count
        FROM `{PROJECT_ID}.{DATASET_STAGING}.stg_transactions`
        WHERE transaction_hash IS NULL
        """,
        
        # Check for invalid timestamps
        f"""
        SELECT COUNT(*) as future_count
        FROM `{PROJECT_ID}.{DATASET_STAGING}.stg_transactions`
        WHERE transaction_timestamp > CURRENT_TIMESTAMP()
        """,
        
        # Check for duplicate transaction IDs
        f"""
        SELECT COUNT(*) - COUNT(DISTINCT transaction_id) as duplicate_count
        FROM `{PROJECT_ID}.{DATASET_STAGING}.stg_transactions`
        """,
    ]
    
    all_passed = True
    for check_query in checks:
        result = list(client.query(check_query).result())
        count = list(result[0].values())[0] if result else 0
        if count > 0:
            all_passed = False
            print(f"Data quality check failed: {check_query[:100]}... Count: {count}")
    
    return all_passed


def send_pipeline_notification(**context) -> None:
    """
    Send notification about pipeline completion.
    """
    ti = context['ti']
    dag_run = context['dag_run']
    
    # Get processing stats
    stats = ti.xcom_pull(task_ids='analytics_group.load_aggregations')
    
    message = f"""
    Blockchain Pipeline Completed
    =============================
    DAG Run: {dag_run.run_id}
    Execution Date: {context['execution_date']}
    Status: {'Success' if dag_run.state == 'success' else dag_run.state}
    
    Processing Stats:
    {stats if stats else 'No stats available'}
    """
    
    print(message)
    # In production, send to Slack/email/PagerDuty


# ============================================================================
# SQL Queries
# ============================================================================

STAGING_TRANSACTIONS_SQL = f"""
-- Transform raw transactions to staging
MERGE INTO `{PROJECT_ID}.{DATASET_STAGING}.stg_transactions` AS target
USING (
    WITH deduplicated AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_hash
                ORDER BY ingested_at DESC
            ) AS row_num
        FROM `{PROJECT_ID}.{DATASET_RAW}.raw_transactions`
        WHERE DATE(ingested_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    )
    SELECT
        CONCAT(transaction_hash, '-', CAST(block_number AS STRING)) AS transaction_id,
        LOWER(TRIM(transaction_hash)) AS transaction_hash,
        CAST(block_number AS INT64) AS block_number,
        LOWER(TRIM(block_hash)) AS block_hash,
        transaction_timestamp,
        DATE(transaction_timestamp) AS transaction_date,
        EXTRACT(HOUR FROM transaction_timestamp) AS transaction_hour,
        LOWER(TRIM(from_address)) AS from_address,
        CASE 
            WHEN to_address IS NULL OR TRIM(to_address) = '' THEN NULL
            ELSE LOWER(TRIM(to_address))
        END AS to_address,
        CAST(value_wei AS INT64) AS value_wei,
        CAST(value_eth AS FLOAT64) AS value_eth,
        CAST(gas AS INT64) AS gas_limit,
        CAST(gas_price AS INT64) AS gas_price_wei,
        CAST(gas_used AS INT64) AS gas_used,
        CASE
            WHEN (to_address IS NULL OR TRIM(to_address) = '') 
                AND contract_address IS NOT NULL 
                THEN 'contract_creation'
            WHEN input_data IS NOT NULL AND input_data != '0x' 
                AND LENGTH(input_data) > 10 
                THEN 'contract_call'
            WHEN value_wei > 0 THEN 'value_transfer'
            ELSE 'other'
        END AS transaction_type,
        NOT COALESCE(is_error, FALSE) AS is_successful,
        source,
        ingested_at,
        CURRENT_TIMESTAMP() AS staged_at
    FROM deduplicated
    WHERE row_num = 1
) AS source
ON target.transaction_id = source.transaction_id
WHEN NOT MATCHED THEN INSERT ROW;
"""

STAGING_WALLETS_SQL = f"""
-- Transform raw wallets to staging
MERGE INTO `{PROJECT_ID}.{DATASET_STAGING}.stg_wallets` AS target
USING (
    WITH deduplicated AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY wallet_address
                ORDER BY ingested_at DESC
            ) AS row_num
        FROM `{PROJECT_ID}.{DATASET_RAW}.raw_wallets`
    )
    SELECT
        LOWER(TRIM(wallet_address)) AS wallet_id,
        LOWER(TRIM(wallet_address)) AS wallet_address,
        first_seen_timestamp AS first_seen_at,
        last_seen_timestamp AS last_seen_at,
        CAST(balance_wei AS INT64) AS balance_wei,
        CAST(balance_eth AS FLOAT64) AS balance_eth,
        COALESCE(total_transactions_in, 0) + COALESCE(total_transactions_out, 0) AS total_transactions,
        CAST(COALESCE(total_transactions_in, 0) AS INT64) AS total_transactions_in,
        CAST(COALESCE(total_transactions_out, 0) AS INT64) AS total_transactions_out,
        CAST(COALESCE(total_value_in_eth, 0) AS FLOAT64) AS total_value_in_eth,
        CAST(COALESCE(total_value_out_eth, 0) AS FLOAT64) AS total_value_out_eth,
        CASE
            WHEN COALESCE(is_contract, FALSE) THEN 'contract'
            WHEN COALESCE(total_transactions_out, 0) = 0 THEN 'receive_only'
            WHEN COALESCE(total_transactions_in, 0) = 0 THEN 'send_only'
            ELSE 'active'
        END AS wallet_type,
        source,
        ingested_at,
        CURRENT_TIMESTAMP() AS staged_at
    FROM deduplicated
    WHERE row_num = 1 AND wallet_address IS NOT NULL
) AS source
ON target.wallet_id = source.wallet_id
WHEN MATCHED THEN UPDATE SET
    balance_wei = source.balance_wei,
    balance_eth = source.balance_eth,
    total_transactions = source.total_transactions,
    staged_at = source.staged_at
WHEN NOT MATCHED THEN INSERT ROW;
"""

FACT_TRANSACTIONS_SQL = f"""
-- Load fact transactions
MERGE INTO `{PROJECT_ID}.{DATASET_ANALYTICS}.fact_transactions` AS target
USING (
    SELECT
        transaction_id AS transaction_key,
        transaction_hash,
        block_number,
        CAST(FORMAT_DATE('%Y%m%d', transaction_date) AS INT64) AS time_key,
        from_address AS from_wallet_key,
        to_address AS to_wallet_key,
        transaction_date,
        transaction_timestamp,
        transaction_hour,
        from_address,
        to_address,
        COALESCE(value_eth, 0) AS value_eth,
        COALESCE(value_wei, 0) AS value_wei,
        gas_limit,
        gas_used,
        transaction_type,
        is_successful,
        source,
        staged_at,
        CURRENT_TIMESTAMP() AS loaded_at
    FROM `{PROJECT_ID}.{DATASET_STAGING}.stg_transactions`
    WHERE staged_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
) AS source
ON target.transaction_key = source.transaction_key
WHEN NOT MATCHED THEN INSERT ROW;
"""

DAILY_AGGREGATIONS_SQL = f"""
-- Update daily aggregations
MERGE INTO `{PROJECT_ID}.{DATASET_ANALYTICS}.agg_daily_metrics` AS target
USING (
    SELECT
        transaction_date AS metric_date,
        COUNT(*) AS total_transactions,
        COUNTIF(is_successful) AS successful_transactions,
        SUM(value_eth) AS total_value_eth,
        AVG(value_eth) AS avg_value_eth,
        MAX(value_eth) AS max_value_eth,
        COUNT(DISTINCT from_address) AS unique_senders,
        COUNT(DISTINCT to_address) AS unique_receivers,
        SUM(gas_used) AS total_gas_used,
        AVG(gas_used) AS avg_gas_used,
        CURRENT_TIMESTAMP() AS loaded_at
    FROM `{PROJECT_ID}.{DATASET_ANALYTICS}.fact_transactions`
    WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    GROUP BY transaction_date
) AS source
ON target.metric_date = source.metric_date
WHEN MATCHED THEN UPDATE SET
    total_transactions = source.total_transactions,
    successful_transactions = source.successful_transactions,
    total_value_eth = source.total_value_eth,
    avg_value_eth = source.avg_value_eth,
    max_value_eth = source.max_value_eth,
    unique_senders = source.unique_senders,
    unique_receivers = source.unique_receivers,
    total_gas_used = source.total_gas_used,
    avg_gas_used = source.avg_gas_used,
    loaded_at = source.loaded_at
WHEN NOT MATCHED THEN INSERT ROW;
"""


# ============================================================================
# DAG Definition
# ============================================================================

with DAG(
    dag_id='blockchain_daily_pipeline',
    default_args=default_args,
    description='Daily blockchain data engineering pipeline',
    schedule_interval='0 2 * * *',  # 2:00 AM UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['blockchain', 'data-engineering', 'daily'],
    doc_md=__doc__,
) as dag:
    
    # ========================================================================
    # Start and End Tasks
    # ========================================================================
    
    start = EmptyOperator(
        task_id='start',
        doc='Pipeline start marker'
    )
    
    end_success = EmptyOperator(
        task_id='end_success',
        doc='Pipeline completed successfully'
    )
    
    skip_processing = EmptyOperator(
        task_id='skip_processing',
        doc='Skip processing - no new data'
    )
    
    # ========================================================================
    # Ingestion Tasks
    # ========================================================================
    
    with TaskGroup(group_id='ingest_group') as ingest_group:
        
        get_params = PythonOperator(
            task_id='get_ingestion_params',
            python_callable=get_ingestion_params,
            doc='Calculate ingestion parameters'
        )
        
        # Run Python ingestion scripts
        ingest_transactions = BashOperator(
            task_id='ingest_transactions',
            bash_command="""
            cd {{ var.value.project_dir }}/data_engineering
            python -m ingestion.ingest_transactions \
                --addresses {{ var.json.monitored_addresses | join(' ') }} \
                --resume
            """,
            doc='Ingest new transaction data from Etherscan'
        )
        
        ingest_wallets = BashOperator(
            task_id='ingest_wallets',
            bash_command="""
            cd {{ var.value.project_dir }}/data_engineering
            python -m ingestion.ingest_wallets --from-transactions --limit 10000
            """,
            doc='Extract and enrich wallet data'
        )
        
        get_params >> ingest_transactions >> ingest_wallets
    
    # ========================================================================
    # Check for New Data
    # ========================================================================
    
    check_new_data = BranchPythonOperator(
        task_id='check_new_data',
        python_callable=check_new_data_available,
        doc='Check if new data is available for processing'
    )
    
    # ========================================================================
    # Transform Tasks
    # ========================================================================
    
    with TaskGroup(group_id='transform_group') as transform_group:
        
        run_staging_transactions = BigQueryInsertJobOperator(
            task_id='run_staging_transactions',
            configuration={
                'query': {
                    'query': STAGING_TRANSACTIONS_SQL,
                    'useLegacySql': False,
                }
            },
            location='US',
            doc='Transform raw transactions to staging'
        )
        
        run_staging_wallets = BigQueryInsertJobOperator(
            task_id='run_staging_wallets',
            configuration={
                'query': {
                    'query': STAGING_WALLETS_SQL,
                    'useLegacySql': False,
                }
            },
            location='US',
            doc='Transform raw wallets to staging'
        )
        
        run_staging_transactions >> run_staging_wallets
    
    # ========================================================================
    # Data Quality Tasks
    # ========================================================================
    
    data_quality_checks = PythonOperator(
        task_id='data_quality_checks',
        python_callable=run_data_quality_checks,
        doc='Run data quality checks on staged data'
    )
    
    # ========================================================================
    # Analytics Tasks
    # ========================================================================
    
    with TaskGroup(group_id='analytics_group') as analytics_group:
        
        load_fact_transactions = BigQueryInsertJobOperator(
            task_id='load_fact_transactions',
            configuration={
                'query': {
                    'query': FACT_TRANSACTIONS_SQL,
                    'useLegacySql': False,
                }
            },
            location='US',
            doc='Load fact_transactions table'
        )
        
        load_aggregations = BigQueryInsertJobOperator(
            task_id='load_aggregations',
            configuration={
                'query': {
                    'query': DAILY_AGGREGATIONS_SQL,
                    'useLegacySql': False,
                }
            },
            location='US',
            doc='Update daily aggregations'
        )
        
        load_fact_transactions >> load_aggregations
    
    # ========================================================================
    # dbt Alternative (Optional)
    # ========================================================================
    
    # Uncomment to use dbt instead of BigQuery operators
    # run_dbt = BashOperator(
    #     task_id='run_dbt',
    #     bash_command=f"""
    #     cd {DBT_PROJECT_DIR}
    #     dbt run --target prod --select staging+ marts+
    #     dbt test --target prod --select staging+ marts+
    #     """,
    #     doc='Run dbt models and tests'
    # )
    
    # ========================================================================
    # Notification Task
    # ========================================================================
    
    send_notification = PythonOperator(
        task_id='send_notification',
        python_callable=send_pipeline_notification,
        trigger_rule='all_done',
        doc='Send pipeline completion notification'
    )
    
    # ========================================================================
    # Task Dependencies
    # ========================================================================
    
    start >> ingest_group >> check_new_data
    
    check_new_data >> transform_group >> data_quality_checks >> analytics_group >> send_notification >> end_success
    check_new_data >> skip_processing >> send_notification


# ============================================================================
# DAG Documentation
# ============================================================================

dag.doc_md = """
## Blockchain Analytics Daily Pipeline

This DAG orchestrates the daily data engineering pipeline for blockchain analytics.

### Pipeline Steps:
1. **Ingestion**: Fetch new transaction and wallet data from Etherscan
2. **Staging**: Clean and normalize raw data
3. **Quality Checks**: Validate data quality
4. **Analytics**: Build fact and dimension tables
5. **Aggregations**: Update daily metrics

### Schedule:
- Runs daily at 2:00 AM UTC
- Single active run at a time
- 2 retries with 5-minute delay

### Configuration:
Set the following Airflow Variables:
- `gcp_project_id`: GCP project ID
- `bq_dataset_raw`: Raw dataset name
- `bq_dataset_staging`: Staging dataset name
- `bq_dataset_analytics`: Analytics dataset name
- `etherscan_api_key`: Etherscan API key
- `monitored_addresses`: JSON array of addresses to monitor

### Monitoring:
- Alerts sent on failure to: data-alerts@example.com
- Check Airflow logs for detailed execution info
"""

