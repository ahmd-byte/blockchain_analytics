"""
Wallet schemas for API responses.

This module defines Pydantic models for wallet-related endpoints,
including wallet details and transaction history.
"""

from pydantic import BaseModel, Field
from typing import Optional
import datetime


class DailyTransactionVolume(BaseModel):
    """
    Daily transaction volume for a wallet.
    
    Attributes:
        transaction_date: Transaction date
        transaction_count: Number of transactions on that date
        total_value: Total value transacted on that date
        inflow: Total incoming value
        outflow: Total outgoing value
    """
    
    transaction_date: datetime.date = Field(..., description="Transaction date", serialization_alias="date")
    transaction_count: int = Field(
        ..., 
        description="Number of transactions",
        ge=0
    )
    total_value: float = Field(
        ..., 
        description="Total value transacted",
        ge=0
    )
    inflow: Optional[float] = Field(
        default=0, 
        description="Total incoming value",
        ge=0
    )
    outflow: Optional[float] = Field(
        default=0, 
        description="Total outgoing value",
        ge=0
    )
    
    model_config = {"populate_by_name": True}


class WalletStats(BaseModel):
    """
    Wallet statistics summary.
    
    Attributes:
        wallet_address: The wallet's blockchain address
        total_transactions: Total number of transactions
        total_volume: Total transaction volume
        first_transaction_date: Date of first transaction
        last_transaction_date: Date of most recent transaction
        unique_counterparties: Number of unique wallets interacted with
        average_transaction_value: Average value per transaction
        fraud_score: ML-generated fraud risk score (0-1)
        is_suspicious: Whether wallet is flagged as suspicious
    """
    
    wallet_address: str = Field(
        ..., 
        description="Wallet blockchain address",
        min_length=1
    )
    total_transactions: int = Field(
        ..., 
        description="Total number of transactions",
        ge=0
    )
    total_volume: float = Field(
        ..., 
        description="Total transaction volume",
        ge=0
    )
    first_transaction_date: Optional[datetime.date] = Field(
        default=None, 
        description="Date of first transaction"
    )
    last_transaction_date: Optional[datetime.date] = Field(
        default=None, 
        description="Date of most recent transaction"
    )
    unique_counterparties: Optional[int] = Field(
        default=0, 
        description="Number of unique wallets interacted with",
        ge=0
    )
    average_transaction_value: Optional[float] = Field(
        default=0, 
        description="Average transaction value",
        ge=0
    )
    fraud_score: Optional[float] = Field(
        default=None, 
        description="Fraud risk score (0-1)",
        ge=0,
        le=1
    )
    is_suspicious: Optional[bool] = Field(
        default=False, 
        description="Whether wallet is flagged as suspicious"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "wallet_address": "0x742d35Cc6634C0532925a3b844Bc9e7595f2bD5e",
                "total_transactions": 1500,
                "total_volume": 250000.75,
                "first_transaction_date": "2023-01-15",
                "last_transaction_date": "2024-01-15",
                "unique_counterparties": 350,
                "average_transaction_value": 166.67,
                "fraud_score": 0.15,
                "is_suspicious": False
            }
        }


class WalletDetailResponse(BaseModel):
    """
    Complete wallet detail response including stats and daily volumes.
    
    Attributes:
        stats: Wallet statistics summary
        daily_volumes: List of daily transaction volumes
    """
    
    stats: WalletStats = Field(..., description="Wallet statistics")
    daily_volumes: list[DailyTransactionVolume] = Field(
        default_factory=list, 
        description="Daily transaction volume history"
    )

