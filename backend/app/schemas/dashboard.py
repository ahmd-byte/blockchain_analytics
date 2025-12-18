"""
Dashboard schemas for API responses.

This module defines Pydantic models for the dashboard endpoints,
including summary statistics and metrics.
"""

from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class DashboardSummary(BaseModel):
    """
    Dashboard summary statistics.
    
    Attributes:
        total_transactions: Total number of transactions
        total_volume: Total transaction volume in native currency
        total_wallets: Total number of unique wallets
        suspicious_wallet_count: Number of wallets flagged as suspicious
        last_updated: Timestamp of last data update
    """
    
    total_transactions: int = Field(
        ..., 
        description="Total number of blockchain transactions",
        ge=0
    )
    total_volume: float = Field(
        ..., 
        description="Total transaction volume",
        ge=0
    )
    total_wallets: int = Field(
        ..., 
        description="Total number of unique wallet addresses",
        ge=0
    )
    suspicious_wallet_count: int = Field(
        ..., 
        description="Number of wallets flagged as suspicious",
        ge=0
    )
    last_updated: Optional[datetime] = Field(
        default=None,
        description="Timestamp of last data refresh"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "total_transactions": 1500000,
                "total_volume": 25000000.50,
                "total_wallets": 75000,
                "suspicious_wallet_count": 1250,
                "last_updated": "2024-01-15T10:30:00Z"
            }
        }


class DashboardMetric(BaseModel):
    """
    Single dashboard metric with optional comparison.
    
    Attributes:
        metric_name: Name of the metric
        value: Current value
        previous_value: Previous period value for comparison
        change_percentage: Percentage change from previous period
    """
    
    metric_name: str = Field(..., description="Name of the metric")
    value: float = Field(..., description="Current metric value")
    previous_value: Optional[float] = Field(
        default=None, 
        description="Previous period value"
    )
    change_percentage: Optional[float] = Field(
        default=None, 
        description="Percentage change from previous period"
    )

