"""
Wallet service for business logic.

This module contains business logic for wallet-related operations,
including wallet statistics and transaction history retrieval.
"""

from typing import Optional
from google.cloud import bigquery

from app.core.bigquery_client import get_bigquery_client
from app.core.config import get_settings
from app.schemas.wallet import (
    WalletStats, 
    DailyTransactionVolume, 
    WalletDetailResponse
)


class WalletService:
    """
    Service class for wallet operations.
    
    Provides methods for retrieving wallet statistics
    and transaction history from BigQuery.
    """
    
    def __init__(self):
        """Initialize the wallet service."""
        self.bq_client = get_bigquery_client()
        self.settings = get_settings()
    
    async def get_wallet_details(
        self, 
        wallet_address: str,
        days: int = 30
    ) -> Optional[WalletDetailResponse]:
        """
        Get detailed wallet information including stats and daily volumes.
        
        Args:
            wallet_address: The blockchain wallet address
            days: Number of days of transaction history to include
            
        Returns:
            WalletDetailResponse: Complete wallet details or None if not found
            
        Raises:
            Exception: If query execution fails
        """
        project = self.settings.google_cloud_project
        analytics_dataset = self.settings.bigquery_dataset_analytics
        ml_dataset = self.settings.bigquery_dataset_ml
        
        # Parameterized query for wallet stats - computed from raw_transactions
        stats_query = f"""
        SELECT 
            @wallet_address as wallet_address,
            COUNT(*) as total_transactions,
            COALESCE(SUM(CAST(value_eth AS FLOAT64)), 0) as total_volume,
            MIN(DATE(transaction_timestamp)) as first_transaction_date,
            MAX(DATE(transaction_timestamp)) as last_transaction_date,
            COUNT(DISTINCT CASE 
                WHEN from_address = @wallet_address THEN to_address 
                ELSE from_address 
            END) as unique_counterparties,
            -- Placeholder fraud score based on transaction patterns
            CASE 
                WHEN SUM(CAST(value_eth AS FLOAT64)) > 1000 THEN 0.3
                WHEN COUNT(*) > 100 THEN 0.2
                ELSE 0.1
            END as fraud_score,
            FALSE as is_suspicious
        FROM `{project}.{analytics_dataset}.{self.settings.table_fact_transactions}`
        WHERE from_address = @wallet_address OR to_address = @wallet_address
        """
        
        # Parameterized query for daily volumes - using raw_transactions
        volumes_query = f"""
        SELECT 
            DATE(transaction_timestamp) as date,
            COUNT(*) as transaction_count,
            COALESCE(SUM(CAST(value_eth AS FLOAT64)), 0) as total_value,
            COALESCE(SUM(CASE WHEN to_address = @wallet_address THEN CAST(value_eth AS FLOAT64) ELSE 0 END), 0) as inflow,
            COALESCE(SUM(CASE WHEN from_address = @wallet_address THEN CAST(value_eth AS FLOAT64) ELSE 0 END), 0) as outflow
        FROM `{project}.{analytics_dataset}.{self.settings.table_fact_transactions}`
        WHERE (from_address = @wallet_address OR to_address = @wallet_address)
            AND DATE(transaction_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
        GROUP BY DATE(transaction_timestamp)
        ORDER BY date DESC
        """
        
        # Query parameters
        params = [
            bigquery.ScalarQueryParameter("wallet_address", "STRING", wallet_address),
            bigquery.ScalarQueryParameter("days", "INT64", days)
        ]
        
        try:
            # Execute stats query
            stats_results = await self.bq_client.execute_query(
                stats_query, 
                [bigquery.ScalarQueryParameter("wallet_address", "STRING", wallet_address)]
            )
            
            if not stats_results:
                return None
            
            stats_data = stats_results[0]
            
            # Calculate average transaction value
            total_tx = stats_data.get("total_transactions", 0)
            total_vol = stats_data.get("total_volume", 0)
            avg_value = total_vol / total_tx if total_tx > 0 else 0
            
            wallet_stats = WalletStats(
                wallet_address=stats_data.get("wallet_address", wallet_address),
                total_transactions=int(stats_data.get("total_transactions", 0)),
                total_volume=float(stats_data.get("total_volume", 0)),
                first_transaction_date=stats_data.get("first_transaction_date"),
                last_transaction_date=stats_data.get("last_transaction_date"),
                unique_counterparties=int(stats_data.get("unique_counterparties", 0)),
                average_transaction_value=round(avg_value, 2),
                fraud_score=float(stats_data.get("fraud_score", 0)),
                is_suspicious=bool(stats_data.get("is_suspicious", False))
            )
            
            # Execute volumes query
            volumes_results = await self.bq_client.execute_query(volumes_query, params)
            
            daily_volumes = [
                DailyTransactionVolume(
                    transaction_date=row.get("date"),
                    transaction_count=int(row.get("transaction_count", 0)),
                    total_value=float(row.get("total_value", 0)),
                    inflow=float(row.get("inflow", 0)),
                    outflow=float(row.get("outflow", 0))
                )
                for row in volumes_results
            ]
            
            return WalletDetailResponse(
                stats=wallet_stats,
                daily_volumes=daily_volumes
            )
            
        except Exception as e:
            raise Exception(f"Failed to fetch wallet details: {str(e)}")
    
def get_wallet_service() -> WalletService:
    """
    Get wallet service instance.
    
    Returns:
        WalletService: Wallet service instance
    """
    return WalletService()

