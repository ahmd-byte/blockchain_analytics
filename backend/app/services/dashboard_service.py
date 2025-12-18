"""
Dashboard service for business logic.

This module contains business logic for dashboard-related operations,
including summary statistics and metrics retrieval.
"""

from typing import Any
from datetime import datetime
from google.cloud import bigquery

from app.core.bigquery_client import get_bigquery_client
from app.core.config import get_settings
from app.schemas.dashboard import DashboardSummary


class DashboardService:
    """
    Service class for dashboard operations.
    
    Provides methods for retrieving dashboard statistics
    and metrics from BigQuery.
    """
    
    def __init__(self):
        """Initialize the dashboard service."""
        self.bq_client = get_bigquery_client()
        self.settings = get_settings()
    
    async def get_summary(self) -> DashboardSummary:
        """
        Get dashboard summary statistics.
        
        Retrieves total transactions, total volume, total wallets,
        and suspicious wallet count from BigQuery.
        
        Returns:
            DashboardSummary: Summary statistics object
            
        Raises:
            Exception: If query execution fails
        """
        project = self.settings.google_cloud_project
        raw_dataset = self.settings.bigquery_dataset_raw
        ml_dataset = self.settings.bigquery_dataset_ml
        
        # Query for transaction stats from raw_transactions
        transactions_query = f"""
        SELECT 
            COUNT(*) as total_transactions,
            COALESCE(SUM(CAST(value_eth AS FLOAT64)), 0) as total_volume
        FROM `{project}.{raw_dataset}.{self.settings.table_fact_transactions}`
        """
        
        # Query for wallet stats - count unique addresses from transactions
        wallets_query = f"""
        SELECT COUNT(DISTINCT address) as total_wallets
        FROM (
            SELECT from_address as address FROM `{project}.{raw_dataset}.{self.settings.table_fact_transactions}`
            UNION DISTINCT
            SELECT to_address as address FROM `{project}.{raw_dataset}.{self.settings.table_fact_transactions}` WHERE to_address IS NOT NULL
        )
        """
        
        # Query for suspicious wallet count from ML fraud scores
        # Falls back to high-value transaction heuristic if ML table doesn't exist
        suspicious_query = f"""
        SELECT COUNTIF(fraud_score >= 0.7) as suspicious_count
        FROM `{project}.{ml_dataset}.{self.settings.table_wallet_fraud_scores}`
        """
        
        try:
            # Execute queries
            tx_results = await self.bq_client.execute_query(transactions_query)
            wallet_results = await self.bq_client.execute_query(wallets_query)
            
            # Try ML-based suspicious count, fallback to 0 if ML table doesn't exist
            suspicious_count = 0
            try:
                suspicious_results = await self.bq_client.execute_query(suspicious_query)
                suspicious_data = suspicious_results[0] if suspicious_results else {}
                suspicious_count = int(suspicious_data.get("suspicious_count", 0))
            except Exception:
                # ML table may not exist yet - use fallback
                raw_dataset = self.settings.bigquery_dataset_raw
                fallback_query = f"""
                SELECT COUNT(DISTINCT from_address) as suspicious_count
                FROM `{project}.{raw_dataset}.{self.settings.table_fact_transactions}`
                WHERE CAST(value_eth AS FLOAT64) > 100
                """
                try:
                    fallback_results = await self.bq_client.execute_query(fallback_query)
                    fallback_data = fallback_results[0] if fallback_results else {}
                    suspicious_count = int(fallback_data.get("suspicious_count", 0))
                except Exception:
                    suspicious_count = 0
            
            # Extract values with defaults
            tx_data = tx_results[0] if tx_results else {}
            wallet_data = wallet_results[0] if wallet_results else {}
            
            return DashboardSummary(
                total_transactions=int(tx_data.get("total_transactions", 0)),
                total_volume=float(tx_data.get("total_volume", 0)),
                total_wallets=int(wallet_data.get("total_wallets", 0)),
                suspicious_wallet_count=suspicious_count,
                last_updated=datetime.utcnow()
            )
            
        except Exception as e:
            raise Exception(f"Failed to fetch dashboard summary: {str(e)}")
    
    async def get_summary_mock(self) -> DashboardSummary:
        """
        Get mock dashboard summary for development/testing.
        
        Returns:
            DashboardSummary: Mock summary statistics
        """
        return DashboardSummary(
            total_transactions=1547832,
            total_volume=25847293.45,
            total_wallets=78234,
            suspicious_wallet_count=1342,
            last_updated=datetime.utcnow()
        )


def get_dashboard_service() -> DashboardService:
    """
    Get dashboard service instance.
    
    Returns:
        DashboardService: Dashboard service instance
    """
    return DashboardService()


