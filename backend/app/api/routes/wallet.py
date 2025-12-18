"""
Wallet API routes.

This module defines API endpoints for wallet-related operations,
including wallet statistics and transaction history.
"""

from fastapi import APIRouter, HTTPException, Depends, Path, Query
from typing import Optional

from app.schemas.wallet import WalletDetailResponse
from app.schemas.health import ErrorResponse
from app.services.wallet_service import WalletService, get_wallet_service
from app.core.config import get_settings, Settings


router = APIRouter(
    prefix="/wallet",
    tags=["Wallet"],
    responses={
        404: {"model": ErrorResponse, "description": "Wallet not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    }
)


@router.get(
    "/{wallet_address}",
    response_model=WalletDetailResponse,
    summary="Get Wallet Details",
    description="Retrieve detailed information about a specific wallet, "
                "including statistics and daily transaction volumes.",
    responses={
        200: {
            "description": "Wallet details with stats and daily volumes",
            "model": WalletDetailResponse
        }
    }
)
async def get_wallet_details(
    wallet_address: str = Path(
        ...,
        description="The blockchain wallet address",
        min_length=1,
        example="0x742d35Cc6634C0532925a3b844Bc9e7595f2bD5e"
    ),
    days: int = Query(
        default=30,
        description="Number of days of transaction history to include",
        ge=1,
        le=365
    ),
    use_mock: Optional[bool] = Query(
        default=False,
        description="Use mock data for development/testing"
    ),
    service: WalletService = Depends(get_wallet_service),
    settings: Settings = Depends(get_settings)
) -> WalletDetailResponse:
    """
    Get detailed wallet information.
    
    This endpoint returns comprehensive information about a specific
    wallet address, including:
    
    **Wallet Statistics:**
    - Total transactions and volume
    - First and last transaction dates
    - Number of unique counterparties
    - Average transaction value
    - Fraud score and suspicious flag
    
    **Daily Transaction Volume (for specified days):**
    - Date
    - Transaction count
    - Total value
    - Inflow and outflow amounts
    
    Args:
        wallet_address: The blockchain wallet address to look up
        days: Number of days of history to include (1-365)
        use_mock: If True, return mock data instead of querying BigQuery
        service: Wallet service dependency
        settings: Application settings dependency
        
    Returns:
        WalletDetailResponse: Complete wallet details with stats and volumes
        
    Raises:
        HTTPException: 404 if wallet not found, 500 on query error
    """
    try:
        if use_mock or settings.debug:
            return await service.get_wallet_details_mock(wallet_address, days)
        
        result = await service.get_wallet_details(wallet_address, days)
        
        if result is None:
            raise HTTPException(
                status_code=404,
                detail=f"Wallet not found: {wallet_address}"
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch wallet details: {str(e)}"
        )

