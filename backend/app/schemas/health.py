"""
Health check schemas for API responses.

This module defines Pydantic models for health check endpoints.
"""

from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class HealthCheckResponse(BaseModel):
    """
    Health check response model.
    
    Attributes:
        status: Overall health status
        timestamp: Current server timestamp
        version: Application version
        bigquery_status: BigQuery connection status
        bigquery_message: BigQuery connection message
    """
    
    status: str = Field(
        ..., 
        description="Overall health status: healthy or unhealthy"
    )
    timestamp: datetime = Field(
        ..., 
        description="Current server timestamp"
    )
    version: str = Field(
        ..., 
        description="Application version"
    )
    bigquery_status: Optional[str] = Field(
        default=None,
        description="BigQuery connection status"
    )
    bigquery_message: Optional[str] = Field(
        default=None,
        description="BigQuery connection message or error"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "status": "healthy",
                "timestamp": "2024-01-15T10:30:00Z",
                "version": "1.0.0",
                "bigquery_status": "healthy",
                "bigquery_message": "BigQuery connection successful"
            }
        }


class ErrorResponse(BaseModel):
    """
    Standard error response model.
    
    Attributes:
        error: Error type/code
        message: Human-readable error message
        detail: Additional error details
        timestamp: Error timestamp
    """
    
    error: str = Field(..., description="Error type or code")
    message: str = Field(..., description="Human-readable error message")
    detail: Optional[str] = Field(
        default=None, 
        description="Additional error details"
    )
    timestamp: datetime = Field(
        default_factory=datetime.utcnow,
        description="Error timestamp"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "error": "NOT_FOUND",
                "message": "Wallet not found",
                "detail": "No wallet found with address 0x...",
                "timestamp": "2024-01-15T10:30:00Z"
            }
        }


