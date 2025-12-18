"""
Fraud detection API routes.

This module defines API endpoints for fraud detection operations,
including retrieving fraud scores and suspicious wallet data.
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Optional

from app.schemas.fraud import (
    FraudWalletListResponse,
    FraudQueryParams
)
from app.schemas.health import ErrorResponse
from app.services.fraud_service import FraudService, get_fraud_service
from app.core.config import get_settings, Settings


router = APIRouter(
    prefix="/fraud",
    tags=["Fraud Detection"],
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"}
    }
)


@router.get(
    "/wallets",
    response_model=FraudWalletListResponse,
    summary="Get Fraud Wallets",
    description="Retrieve a paginated list of wallets with fraud scores "
                "and suspicious flags. Supports filtering and sorting.",
    responses={
        200: {
            "description": "Paginated list of fraud wallet data",
            "model": FraudWalletListResponse
        }
    }
)
async def get_fraud_wallets(
    min_fraud_score: Optional[float] = Query(
        default=None,
        description="Minimum fraud score filter (0-1)",
        ge=0,
        le=1
    ),
    max_fraud_score: Optional[float] = Query(
        default=None,
        description="Maximum fraud score filter (0-1)",
        ge=0,
        le=1
    ),
    is_suspicious: Optional[bool] = Query(
        default=None,
        description="Filter by suspicious flag"
    ),
    min_tx_count: Optional[int] = Query(
        default=None,
        description="Minimum transaction count filter",
        ge=0
    ),
    sort_by: str = Query(
        default="fraud_score",
        description="Sort field: fraud_score, tx_count, total_value, wallet_address"
    ),
    sort_order: str = Query(
        default="desc",
        description="Sort direction: asc or desc"
    ),
    page: int = Query(
        default=1,
        description="Page number",
        ge=1
    ),
    page_size: int = Query(
        default=50,
        description="Results per page",
        ge=1,
        le=1000
    ),
    use_mock: Optional[bool] = Query(
        default=False,
        description="Use mock data for development/testing"
    ),
    service: FraudService = Depends(get_fraud_service),
    settings: Settings = Depends(get_settings)
) -> FraudWalletListResponse:
    """
    Get list of wallets with fraud scores.
    
    This endpoint returns a paginated list of wallets with their
    associated fraud detection data, including:
    
    **For each wallet:**
    - wallet_address: Blockchain address
    - fraud_score: ML-generated risk score (0-1)
    - is_suspicious: Boolean flag for suspicious activity
    - tx_count: Total transaction count
    - total_value: Total value transacted
    - risk_category: Derived risk level (low, medium, high, critical)
    - last_activity: Last transaction timestamp
    - flagged_reason: Reason for flagging (if suspicious)
    
    **Pagination info:**
    - total_count: Total matching results
    - page: Current page
    - page_size: Results per page
    - suspicious_count: Number of suspicious wallets in results
    
    **Filtering options:**
    - min_fraud_score/max_fraud_score: Score range filter
    - is_suspicious: Filter by suspicious flag
    - min_tx_count: Minimum transaction count
    
    **Sorting options:**
    - sort_by: fraud_score, tx_count, total_value, wallet_address
    - sort_order: asc or desc
    
    Args:
        min_fraud_score: Minimum fraud score (0-1)
        max_fraud_score: Maximum fraud score (0-1)
        is_suspicious: Filter by suspicious flag
        min_tx_count: Minimum transaction count
        sort_by: Sort field
        sort_order: Sort direction
        page: Page number (1-based)
        page_size: Results per page (1-1000)
        use_mock: If True, return mock data
        service: Fraud service dependency
        settings: Application settings dependency
        
    Returns:
        FraudWalletListResponse: Paginated list of fraud wallet data
        
    Raises:
        HTTPException: 500 on query error
    """
    try:
        # Build query params
        params = FraudQueryParams(
            min_fraud_score=min_fraud_score,
            max_fraud_score=max_fraud_score,
            is_suspicious=is_suspicious,
            min_tx_count=min_tx_count,
            sort_by=sort_by,
            sort_order=sort_order,
            page=page,
            page_size=page_size
        )
        
        if use_mock or settings.debug:
            return await service.get_fraud_wallets_mock(params)
        
        return await service.get_fraud_wallets(params)
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch fraud wallets: {str(e)}"
        )


