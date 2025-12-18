"""
Fraud detection schemas for API responses.

This module defines Pydantic models for fraud detection endpoints,
including fraud scores and suspicious wallet data.
"""

from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class FraudWallet(BaseModel):
    """
    Wallet fraud information.
    
    Attributes:
        wallet_address: The wallet's blockchain address
        fraud_score: ML-generated fraud risk score (0-1)
        is_suspicious: Whether wallet is flagged as suspicious
        tx_count: Total transaction count
        total_value: Total value transacted
        risk_category: Risk classification (low, medium, high, critical)
        last_activity: Last transaction timestamp
        flagged_reason: Reason for flagging (if suspicious)
    """
    
    wallet_address: str = Field(
        ..., 
        description="Wallet blockchain address"
    )
    fraud_score: float = Field(
        ..., 
        description="Fraud risk score (0-1)",
        ge=0,
        le=1
    )
    is_suspicious: bool = Field(
        ..., 
        description="Whether wallet is flagged as suspicious"
    )
    tx_count: int = Field(
        ..., 
        description="Total transaction count",
        ge=0
    )
    total_value: float = Field(
        ..., 
        description="Total value transacted",
        ge=0
    )
    risk_category: Optional[str] = Field(
        default="low",
        description="Risk classification: low, medium, high, critical"
    )
    last_activity: Optional[datetime] = Field(
        default=None,
        description="Last transaction timestamp"
    )
    flagged_reason: Optional[str] = Field(
        default=None,
        description="Reason for flagging if suspicious"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "wallet_address": "0x742d35Cc6634C0532925a3b844Bc9e7595f2bD5e",
                "fraud_score": 0.85,
                "is_suspicious": True,
                "tx_count": 5000,
                "total_value": 1500000.00,
                "risk_category": "high",
                "last_activity": "2024-01-15T14:30:00Z",
                "flagged_reason": "Unusual transaction patterns detected"
            }
        }


class FraudWalletListResponse(BaseModel):
    """
    Response for fraud wallet list endpoint.
    
    Attributes:
        wallets: List of fraud wallet information
        total_count: Total number of results
        page: Current page number
        page_size: Number of results per page
        suspicious_count: Number of suspicious wallets in results
    """
    
    wallets: list[FraudWallet] = Field(
        ..., 
        description="List of wallet fraud information"
    )
    total_count: int = Field(
        ..., 
        description="Total number of results",
        ge=0
    )
    page: int = Field(
        default=1, 
        description="Current page number",
        ge=1
    )
    page_size: int = Field(
        default=50, 
        description="Results per page",
        ge=1,
        le=1000
    )
    suspicious_count: Optional[int] = Field(
        default=0,
        description="Number of suspicious wallets in results"
    )


class FraudQueryParams(BaseModel):
    """
    Query parameters for fraud wallet endpoint.
    
    Attributes:
        min_fraud_score: Minimum fraud score filter
        max_fraud_score: Maximum fraud score filter
        is_suspicious: Filter by suspicious flag
        min_tx_count: Minimum transaction count filter
        sort_by: Sort field
        sort_order: Sort direction (asc/desc)
        page: Page number
        page_size: Results per page
    """
    
    min_fraud_score: Optional[float] = Field(
        default=None,
        description="Minimum fraud score filter",
        ge=0,
        le=1
    )
    max_fraud_score: Optional[float] = Field(
        default=None,
        description="Maximum fraud score filter",
        ge=0,
        le=1
    )
    is_suspicious: Optional[bool] = Field(
        default=None,
        description="Filter by suspicious flag"
    )
    min_tx_count: Optional[int] = Field(
        default=None,
        description="Minimum transaction count",
        ge=0
    )
    sort_by: str = Field(
        default="fraud_score",
        description="Sort field"
    )
    sort_order: str = Field(
        default="desc",
        description="Sort direction: asc or desc"
    )
    page: int = Field(
        default=1,
        description="Page number",
        ge=1
    )
    page_size: int = Field(
        default=50,
        description="Results per page",
        ge=1,
        le=1000
    )


