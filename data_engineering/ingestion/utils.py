"""
Utility functions for blockchain data ingestion.

This module provides common utilities used across the ingestion pipeline,
including logging, retry logic, data validation, and BigQuery helpers.
"""

import logging
import time
import hashlib
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional
from functools import wraps

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from .config import CONFIG


# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logger(name: str, level: str = None) -> logging.Logger:
    """
    Set up a logger with consistent formatting.
    
    Args:
        name: Logger name (typically __name__)
        level: Log level (defaults to config setting)
        
    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger(name)
    level = level or CONFIG.log_level
    logger.setLevel(getattr(logging, level.upper()))
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger


# ============================================================================
# RETRY DECORATOR
# ============================================================================

def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    exceptions: tuple = (Exception,)
) -> Callable:
    """
    Decorator for retrying functions with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay between retries (seconds)
        max_delay: Maximum delay between retries (seconds)
        exponential_base: Base for exponential backoff
        exceptions: Tuple of exceptions to catch and retry
        
    Returns:
        Callable: Decorated function with retry logic
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            logger = setup_logger(func.__module__)
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = min(base_delay * (exponential_base ** attempt), max_delay)
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_retries + 1} failed: {e}. "
                            f"Retrying in {delay:.2f}s..."
                        )
                        time.sleep(delay)
                    else:
                        logger.error(f"All {max_retries + 1} attempts failed: {e}")
            
            raise last_exception
        return wrapper
    return decorator


# ============================================================================
# DATA VALIDATION & TRANSFORMATION
# ============================================================================

def normalize_address(address: str) -> str:
    """
    Normalize Ethereum address to lowercase with checksum validation.
    
    Args:
        address: Ethereum address string
        
    Returns:
        str: Normalized lowercase address
    """
    if not address:
        return ""
    return address.lower().strip()


def wei_to_ether(wei_value: int) -> float:
    """
    Convert Wei to Ether.
    
    Args:
        wei_value: Value in Wei
        
    Returns:
        float: Value in Ether
    """
    return wei_value / 1e18


def generate_record_hash(*fields) -> str:
    """
    Generate a unique hash for a record based on its fields.
    Used for deduplication.
    
    Args:
        *fields: Fields to include in the hash
        
    Returns:
        str: SHA256 hash of the concatenated fields
    """
    content = "|".join(str(f) for f in fields)
    return hashlib.sha256(content.encode()).hexdigest()


def unix_to_datetime(timestamp: int) -> datetime:
    """
    Convert Unix timestamp to datetime.
    
    Args:
        timestamp: Unix timestamp (seconds since epoch)
        
    Returns:
        datetime: UTC datetime object
    """
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


def parse_hex_to_int(hex_value: str) -> int:
    """
    Parse hexadecimal string to integer.
    
    Args:
        hex_value: Hexadecimal string (with or without '0x' prefix)
        
    Returns:
        int: Integer value
    """
    if not hex_value:
        return 0
    if isinstance(hex_value, int):
        return hex_value
    return int(hex_value, 16)


# ============================================================================
# BIGQUERY HELPERS
# ============================================================================

class BigQueryHelper:
    """Helper class for BigQuery operations."""
    
    def __init__(self, project_id: str = None):
        """
        Initialize BigQuery helper.
        
        Args:
            project_id: GCP project ID (defaults to config)
        """
        self.project_id = project_id or CONFIG.bigquery.project_id
        self.client = bigquery.Client(project=self.project_id)
        self.logger = setup_logger(__name__)
    
    def ensure_dataset_exists(self, dataset_id: str) -> None:
        """
        Create dataset if it doesn't exist.
        
        Args:
            dataset_id: Dataset ID to create
        """
        dataset_ref = f"{self.project_id}.{dataset_id}"
        try:
            self.client.get_dataset(dataset_ref)
            self.logger.debug(f"Dataset {dataset_ref} already exists")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset)
            self.logger.info(f"Created dataset {dataset_ref}")
    
    def table_exists(self, dataset_id: str, table_id: str) -> bool:
        """
        Check if a table exists.
        
        Args:
            dataset_id: Dataset ID
            table_id: Table ID
            
        Returns:
            bool: True if table exists
        """
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        try:
            self.client.get_table(table_ref)
            return True
        except NotFound:
            return False
    
    def execute_query(self, query: str, params: List = None) -> List[Dict]:
        """
        Execute a BigQuery query and return results.
        
        Args:
            query: SQL query string
            params: Query parameters (optional)
            
        Returns:
            List[Dict]: Query results as list of dictionaries
        """
        job_config = bigquery.QueryJobConfig()
        if params:
            job_config.query_parameters = params
        
        query_job = self.client.query(query, job_config=job_config)
        results = query_job.result()
        
        return [dict(row) for row in results]
    
    def insert_rows(
        self,
        dataset_id: str,
        table_id: str,
        rows: List[Dict],
        schema: List[bigquery.SchemaField] = None
    ) -> int:
        """
        Insert rows into a BigQuery table.
        
        Args:
            dataset_id: Target dataset ID
            table_id: Target table ID
            rows: List of row dictionaries
            schema: Table schema (for table creation)
            
        Returns:
            int: Number of rows inserted
        """
        if not rows:
            return 0
        
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        
        # Create table if schema provided and table doesn't exist
        if schema and not self.table_exists(dataset_id, table_id):
            self.ensure_dataset_exists(dataset_id)
            table = bigquery.Table(table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="ingested_at"
            )
            self.client.create_table(table)
            self.logger.info(f"Created table {table_ref}")
        
        errors = self.client.insert_rows_json(table_ref, rows)
        
        if errors:
            self.logger.error(f"Errors inserting rows: {errors}")
            raise Exception(f"BigQuery insert errors: {errors}")
        
        self.logger.info(f"Inserted {len(rows)} rows into {table_ref}")
        return len(rows)
    
    def get_max_value(
        self,
        dataset_id: str,
        table_id: str,
        column: str,
        default: Any = 0
    ) -> Any:
        """
        Get the maximum value of a column in a table.
        
        Args:
            dataset_id: Dataset ID
            table_id: Table ID
            column: Column name
            default: Default value if table is empty
            
        Returns:
            Any: Maximum value or default
        """
        if not self.table_exists(dataset_id, table_id):
            return default
        
        query = f"""
        SELECT MAX({column}) as max_value
        FROM `{self.project_id}.{dataset_id}.{table_id}`
        """
        
        results = self.execute_query(query)
        if results and results[0].get("max_value") is not None:
            return results[0]["max_value"]
        return default


# ============================================================================
# CHECKPOINT MANAGEMENT
# ============================================================================

class CheckpointManager:
    """Manages ingestion checkpoints for idempotent processing."""
    
    def __init__(self, bq_helper: BigQueryHelper = None):
        """
        Initialize checkpoint manager.
        
        Args:
            bq_helper: BigQuery helper instance
        """
        self.bq = bq_helper or BigQueryHelper()
        self.logger = setup_logger(__name__)
        self.dataset = CONFIG.bigquery.raw_dataset
        self.table = CONFIG.checkpoint_table
    
    def _ensure_checkpoint_table(self) -> None:
        """Create checkpoint table if it doesn't exist."""
        schema = [
            bigquery.SchemaField("pipeline_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("checkpoint_key", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("checkpoint_value", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
        ]
        
        if not self.bq.table_exists(self.dataset, self.table):
            self.bq.ensure_dataset_exists(self.dataset)
            table_ref = f"{self.bq.project_id}.{self.dataset}.{self.table}"
            table = bigquery.Table(table_ref, schema=schema)
            self.bq.client.create_table(table)
            self.logger.info(f"Created checkpoint table {table_ref}")
    
    def get_checkpoint(self, pipeline_name: str, key: str, default: Any = None) -> Any:
        """
        Get a checkpoint value.
        
        Args:
            pipeline_name: Name of the pipeline
            key: Checkpoint key
            default: Default value if not found
            
        Returns:
            Any: Checkpoint value or default
        """
        self._ensure_checkpoint_table()
        
        query = f"""
        SELECT checkpoint_value
        FROM `{self.bq.project_id}.{self.dataset}.{self.table}`
        WHERE pipeline_name = @pipeline_name AND checkpoint_key = @key
        ORDER BY updated_at DESC
        LIMIT 1
        """
        
        params = [
            bigquery.ScalarQueryParameter("pipeline_name", "STRING", pipeline_name),
            bigquery.ScalarQueryParameter("key", "STRING", key),
        ]
        
        results = self.bq.execute_query(query, params)
        if results:
            return results[0]["checkpoint_value"]
        return default
    
    def set_checkpoint(self, pipeline_name: str, key: str, value: Any) -> None:
        """
        Set a checkpoint value.
        
        Args:
            pipeline_name: Name of the pipeline
            key: Checkpoint key
            value: Value to store
        """
        self._ensure_checkpoint_table()
        
        row = {
            "pipeline_name": pipeline_name,
            "checkpoint_key": key,
            "checkpoint_value": str(value),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        
        table_ref = f"{self.bq.project_id}.{self.dataset}.{self.table}"
        self.bq.client.insert_rows_json(table_ref, [row])
        self.logger.debug(f"Set checkpoint {pipeline_name}.{key} = {value}")

