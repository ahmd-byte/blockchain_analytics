"""
Fraud detection service for business logic.

This module contains business logic for fraud detection operations,
including retrieving fraud scores and suspicious wallet data.
"""

from typing import Optional
from datetime import datetime
from google.cloud import bigquery

from app.core.bigquery_client import get_bigquery_client
from app.core.config import get_settings
from app.schemas.fraud import (
    FraudWallet,
    FraudWalletListResponse,
    FraudQueryParams
)


class FraudService:
    """
    Service class for fraud detection operations.
    
    Provides methods for retrieving fraud scores
    and suspicious wallet data from BigQuery.
    """
    
    def __init__(self):
        """Initialize the fraud service."""
        self.bq_client = get_bigquery_client()
        self.settings = get_settings()
    
    def _get_risk_category(self, fraud_score: float) -> str:
        """
        Get risk category based on fraud score.
        
        Args:
            fraud_score: Fraud score (0-1)
            
        Returns:
            str: Risk category (low, medium, high, critical)
        """
        if fraud_score >= 0.9:
            return "critical"
        elif fraud_score >= 0.7:
            return "high"
        elif fraud_score >= 0.4:
            return "medium"
        return "low"
    
    async def get_fraud_wallets(
        self,
        params: FraudQueryParams
    ) -> FraudWalletListResponse:
        """
        Get list of wallets with fraud scores.
        
        Args:
            params: Query parameters for filtering and pagination
            
        Returns:
            FraudWalletListResponse: Paginated list of fraud wallet data
            
        Raises:
            Exception: If query execution fails
        """
        project = self.settings.google_cloud_project
        ml_dataset = self.settings.bigquery_dataset_ml
        
        # Build WHERE clause dynamically
        where_conditions = ["1=1"]
        query_params = []
        
        if params.min_fraud_score is not None:
            where_conditions.append("fs.fraud_score >= @min_fraud_score")
            query_params.append(
                bigquery.ScalarQueryParameter("min_fraud_score", "FLOAT64", params.min_fraud_score)
            )
        
        if params.max_fraud_score is not None:
            where_conditions.append("fs.fraud_score <= @max_fraud_score")
            query_params.append(
                bigquery.ScalarQueryParameter("max_fraud_score", "FLOAT64", params.max_fraud_score)
            )
        
        if params.is_suspicious is not None:
            # Convert is_suspicious filter to fraud_score condition
            # is_suspicious = True means fraud_score >= 0.7
            if params.is_suspicious:
                where_conditions.append("fs.fraud_score >= 0.7")
            else:
                where_conditions.append("fs.fraud_score < 0.7")
        
        if params.min_tx_count is not None:
            where_conditions.append("wf.tx_count >= @min_tx_count")
            query_params.append(
                bigquery.ScalarQueryParameter("min_tx_count", "INT64", params.min_tx_count)
            )
        
        where_clause = " AND ".join(where_conditions)
        
        # Validate sort_by to prevent SQL injection
        allowed_sort_fields = ["fraud_score", "tx_count", "total_value", "wallet_address"]
        sort_by = params.sort_by if params.sort_by in allowed_sort_fields else "fraud_score"
        sort_order = "DESC" if params.sort_order.upper() == "DESC" else "ASC"
        
        # Calculate offset
        offset = (params.page - 1) * params.page_size
        
        # Query from ML-generated fraud scores table
        # Joins with wallet features for tx_count and total_value
        data_query = f"""
        SELECT 
            fs.wallet_address,
            fs.fraud_score,
            fs.risk_category,
            fs.scored_at as last_activity,
            CASE WHEN fs.fraud_score >= 0.7 THEN TRUE ELSE FALSE END as is_suspicious,
            COALESCE(wf.tx_count, 0) as tx_count,
            COALESCE(wf.total_value, 0) as total_value
        FROM `{project}.{ml_dataset}.{self.settings.table_wallet_fraud_scores}` fs
        LEFT JOIN `{project}.{ml_dataset}.{self.settings.table_wallet_features}` wf
            ON fs.wallet_address = wf.wallet_address
        WHERE {where_clause}
        ORDER BY {sort_by} {sort_order}
        LIMIT @page_size
        OFFSET @offset
        """
        
        query_params.extend([
            bigquery.ScalarQueryParameter("page_size", "INT64", params.page_size),
            bigquery.ScalarQueryParameter("offset", "INT64", offset)
        ])
        
        # Count query for total results
        count_query = f"""
        SELECT 
            COUNT(*) as total_count,
            COUNTIF(fraud_score >= 0.7) as suspicious_count
        FROM `{project}.{ml_dataset}.{self.settings.table_wallet_fraud_scores}`
        """
        
        # Remove pagination params for count query
        count_params = [p for p in query_params if p.name not in ("page_size", "offset")]
        
        try:
            # Execute queries
            data_results = await self.bq_client.execute_query(data_query, query_params)
            count_results = await self.bq_client.execute_query(count_query, count_params)
            
            count_data = count_results[0] if count_results else {}
            
            wallets = [
                FraudWallet(
                    wallet_address=row.get("wallet_address", ""),
                    fraud_score=float(row.get("fraud_score", 0)),
                    is_suspicious=bool(row.get("is_suspicious", False)),
                    tx_count=int(row.get("tx_count", 0)),
                    total_value=float(row.get("total_value", 0)),
                    risk_category=row.get("risk_category") or self._get_risk_category(float(row.get("fraud_score", 0))),
                    last_activity=row.get("last_activity"),
                    flagged_reason="ML-detected anomalous behavior" if row.get("is_suspicious") else None
                )
                for row in data_results
            ]
            
            return FraudWalletListResponse(
                wallets=wallets,
                total_count=int(count_data.get("total_count", 0)),
                page=params.page,
                page_size=params.page_size,
                suspicious_count=int(count_data.get("suspicious_count", 0))
            )
            
        except Exception as e:
            # If ML table doesn't exist or query fails, try fallback to raw_wallets
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"ML fraud query failed, using fallback: {str(e)}")
            # Use fallback for any error (table not found, schema mismatch, etc.)
            return await self._get_fraud_wallets_fallback(params)
    
    async def _get_fraud_wallets_fallback(
        self,
        params: FraudQueryParams
    ) -> FraudWalletListResponse:
        """
        Fallback method when ML tables don't exist.
        Uses raw_wallets with heuristic-based fraud scoring.
        """
        project = self.settings.google_cloud_project
        raw_dataset = self.settings.bigquery_dataset_raw
        
        offset = (params.page - 1) * params.page_size
        
        # Fallback query using raw_wallets
        data_query = f"""
        SELECT 
            wallet_address,
            (total_transactions_in + total_transactions_out) as tx_count,
            CAST(total_value_in_eth AS FLOAT64) + CAST(total_value_out_eth AS FLOAT64) as total_value,
            last_seen_timestamp as last_activity,
            CASE 
                WHEN (CAST(total_value_in_eth AS FLOAT64) + CAST(total_value_out_eth AS FLOAT64)) > 1000 THEN 0.8
                WHEN (total_transactions_in + total_transactions_out) > 500 THEN 0.6
                WHEN (CAST(total_value_in_eth AS FLOAT64) + CAST(total_value_out_eth AS FLOAT64)) > 100 THEN 0.4
                ELSE 0.2
            END as fraud_score
        FROM `{project}.{raw_dataset}.raw_wallets`
        ORDER BY fraud_score DESC
        LIMIT @page_size
        OFFSET @offset
        """
        
        count_query = f"""
        SELECT 
            COUNT(*) as total_count,
            COUNTIF((CAST(total_value_in_eth AS FLOAT64) + CAST(total_value_out_eth AS FLOAT64)) > 1000) as suspicious_count
        FROM `{project}.{raw_dataset}.raw_wallets`
        """
        
        query_params = [
            bigquery.ScalarQueryParameter("page_size", "INT64", params.page_size),
            bigquery.ScalarQueryParameter("offset", "INT64", offset)
        ]
        
        data_results = await self.bq_client.execute_query(data_query, query_params)
        count_results = await self.bq_client.execute_query(count_query)
        
        count_data = count_results[0] if count_results else {}
        
        wallets = [
            FraudWallet(
                wallet_address=row.get("wallet_address", ""),
                fraud_score=float(row.get("fraud_score", 0)),
                is_suspicious=float(row.get("fraud_score", 0)) >= 0.7,
                tx_count=int(row.get("tx_count", 0)),
                total_value=float(row.get("total_value", 0)),
                risk_category=self._get_risk_category(float(row.get("fraud_score", 0))),
                last_activity=row.get("last_activity"),
                flagged_reason="Heuristic-based detection (ML model not yet run)" if float(row.get("fraud_score", 0)) >= 0.7 else None
            )
            for row in data_results
        ]
        
        return FraudWalletListResponse(
            wallets=wallets,
            total_count=int(count_data.get("total_count", 0)),
            page=params.page,
            page_size=params.page_size,
            suspicious_count=int(count_data.get("suspicious_count", 0))
        )
    
def get_fraud_service() -> FraudService:
    """
    Get fraud service instance.
    
    Returns:
        FraudService: Fraud service instance
    """
    return FraudService()


