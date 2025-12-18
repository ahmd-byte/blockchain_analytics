"""
Dashboard API routes.

This module defines API endpoints for dashboard statistics
and metrics.
"""

from fastapi import APIRouter, HTTPException, Depends

from app.schemas.dashboard import DashboardSummary
from app.schemas.health import ErrorResponse
from app.services.dashboard_service import DashboardService, get_dashboard_service


router = APIRouter(
    prefix="/dashboard",
    tags=["Dashboard"],
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"}
    }
)


@router.get(
    "/summary",
    response_model=DashboardSummary,
    summary="Get Dashboard Summary",
    description="Retrieve summary statistics including total transactions, "
                "volume, wallets, and suspicious wallet count.",
    responses={
        200: {
            "description": "Dashboard summary statistics",
            "model": DashboardSummary
        }
    }
)
async def get_dashboard_summary(
    service: DashboardService = Depends(get_dashboard_service)
) -> DashboardSummary:
    """
    Get dashboard summary statistics.
    
    This endpoint returns aggregate statistics for the blockchain
    analytics dashboard, including:
    
    - **total_transactions**: Total number of blockchain transactions
    - **total_volume**: Total transaction volume in native currency
    - **total_wallets**: Total number of unique wallet addresses
    - **suspicious_wallet_count**: Number of wallets flagged as suspicious
    - **last_updated**: Timestamp of last data refresh
    
    Args:
        service: Dashboard service dependency
        
    Returns:
        DashboardSummary: Dashboard summary statistics
        
    Raises:
        HTTPException: 500 error if query execution fails
    """
    try:
        return await service.get_summary()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch dashboard summary: {str(e)}"
        )


