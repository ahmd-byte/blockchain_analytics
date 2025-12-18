"""
BigQuery client for database operations.

This module provides an async-compatible BigQuery client for executing
queries against Google BigQuery with parameterized query support.
"""

import asyncio
from typing import Any, Optional
from concurrent.futures import ThreadPoolExecutor
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
from google.api_core.exceptions import NotFound

from app.core.config import get_settings


class BigQueryClient:
    """
    Async-compatible BigQuery client.
    
    This client wraps the synchronous BigQuery client to provide
    async query execution using a thread pool executor.
    
    Attributes:
        client: The underlying BigQuery client
        project: GCP project ID
        dataset_analytics: Analytics dataset name
        dataset_ml: ML dataset name
        settings: Application settings
    """
    
    _instance: Optional["BigQueryClient"] = None
    _executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=10)
    
    def __new__(cls) -> "BigQueryClient":
        """Singleton pattern to reuse BigQuery client."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize the BigQuery client."""
        if self._initialized:
            return
            
        self.settings = get_settings()
        self.project = self.settings.google_cloud_project
        self.dataset_analytics = self.settings.bigquery_dataset_analytics
        self.dataset_ml = self.settings.bigquery_dataset_ml
        
        try:
            self.client = bigquery.Client(project=self.project)
            self._initialized = True
        except Exception as e:
            # Log the error but allow the app to start (for development)
            print(f"Warning: Could not initialize BigQuery client: {e}")
            self.client = None
            self._initialized = True
    
    def _get_full_table_name(self, table: str, dataset: str = None) -> str:
        """
        Get the fully qualified table name.
        
        Args:
            table: Table name
            dataset: Dataset name (defaults to analytics dataset)
            
        Returns:
            str: Fully qualified table name (project.dataset.table)
        """
        dataset = dataset or self.dataset_analytics
        return f"`{self.project}.{dataset}.{table}`"
    
    def _execute_query_sync(
        self, 
        query: str, 
        parameters: list[bigquery.ScalarQueryParameter] = None
    ) -> list[dict[str, Any]]:
        """
        Execute a query synchronously.
        
        Args:
            query: SQL query string
            parameters: Optional list of query parameters
            
        Returns:
            list[dict]: Query results as list of dictionaries
            
        Raises:
            GoogleCloudError: If query execution fails
        """
        if self.client is None:
            raise RuntimeError("BigQuery client not initialized")
        
        job_config = bigquery.QueryJobConfig()
        if parameters:
            job_config.query_parameters = parameters
        
        query_job = self.client.query(query, job_config=job_config)
        results = query_job.result()
        
        return [dict(row) for row in results]
    
    async def execute_query(
        self, 
        query: str, 
        parameters: list[bigquery.ScalarQueryParameter] = None
    ) -> list[dict[str, Any]]:
        """
        Execute a query asynchronously.
        
        Uses a thread pool executor to run the synchronous BigQuery
        client in a non-blocking manner.
        
        Args:
            query: SQL query string
            parameters: Optional list of query parameters
            
        Returns:
            list[dict]: Query results as list of dictionaries
            
        Raises:
            GoogleCloudError: If query execution fails
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._execute_query_sync,
            query,
            parameters
        )
    
    async def health_check(self) -> dict[str, Any]:
        """
        Perform a health check on the BigQuery connection.
        
        Returns:
            dict: Health check result with status and message
        """
        if self.client is None:
            return {
                "status": "unhealthy",
                "message": "BigQuery client not initialized",
                "project": self.project
            }
        
        try:
            # Simple query to check connection
            query = "SELECT 1 as health_check"
            await self.execute_query(query)
            return {
                "status": "healthy",
                "message": "BigQuery connection successful",
                "project": self.project
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "message": str(e),
                "project": self.project
            }


def get_bigquery_client() -> BigQueryClient:
    """
    Get the BigQuery client instance.
    
    Returns:
        BigQueryClient: Singleton BigQuery client instance
    """
    return BigQueryClient()

