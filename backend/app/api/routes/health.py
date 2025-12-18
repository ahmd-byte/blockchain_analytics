"""
Health check API routes.

This module defines API endpoints for health checks
and system status monitoring.
"""

from fastapi import APIRouter, Depends
from datetime import datetime

from app.schemas.health import HealthCheckResponse
from app.core.config import get_settings, Settings
from app.core.bigquery_client import get_bigquery_client, BigQueryClient


router = APIRouter(
    tags=["Health"]
)


@router.get(
    "/health",
    response_model=HealthCheckResponse,
    summary="Health Check",
    description="Check the health status of the API and its dependencies.",
    responses={
        200: {
            "description": "Health check response",
            "model": HealthCheckResponse
        }
    }
)
async def health_check(
    settings: Settings = Depends(get_settings),
    bq_client: BigQueryClient = Depends(get_bigquery_client)
) -> HealthCheckResponse:
    """
    Perform a health check on the API.
    
    This endpoint checks the status of the API and its
    dependencies, including:
    
    - API server status
    - BigQuery connection status
    - Application version
    
    Use this endpoint for:
    - Load balancer health checks
    - Monitoring and alerting
    - Debugging connectivity issues
    
    Args:
        settings: Application settings dependency
        bq_client: BigQuery client dependency
        
    Returns:
        HealthCheckResponse: Health check result with status details
    """
    # Check BigQuery connection
    bq_health = await bq_client.health_check()
    
    # Determine overall status
    overall_status = "healthy" if bq_health.get("status") == "healthy" else "degraded"
    
    return HealthCheckResponse(
        status=overall_status,
        timestamp=datetime.utcnow(),
        version=settings.app_version,
        bigquery_status=bq_health.get("status"),
        bigquery_message=bq_health.get("message")
    )


@router.get(
    "/",
    summary="Root Endpoint",
    description="API root endpoint with basic information.",
    responses={
        200: {
            "description": "API information",
            "content": {
                "application/json": {
                    "example": {
                        "name": "Blockchain Analytics API",
                        "version": "1.0.0",
                        "docs": "/docs"
                    }
                }
            }
        }
    }
)
async def root(
    settings: Settings = Depends(get_settings)
) -> dict:
    """
    Root endpoint returning basic API information.
    
    Args:
        settings: Application settings dependency
        
    Returns:
        dict: Basic API information
    """
    return {
        "name": settings.app_name,
        "version": settings.app_version,
        "docs": "/docs",
        "health": "/health"
    }

