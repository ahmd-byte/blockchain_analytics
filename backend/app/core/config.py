"""
Application configuration settings.

This module defines all configuration settings for the backend application,
including BigQuery connection parameters and API settings.
"""

import os
from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    
    Attributes:
        app_name: Name of the application
        app_version: Current version of the application
        debug: Enable debug mode
        google_cloud_project: GCP project ID for BigQuery
        bigquery_dataset_analytics: BigQuery dataset for blockchain analytics
        bigquery_dataset_ml: BigQuery dataset for ML models
        google_application_credentials: Path to GCP service account JSON
        cors_origins: List of allowed CORS origins
        api_prefix: API route prefix
    """
    
    # Application settings
    app_name: str = "Blockchain Analytics API"
    app_version: str = "1.0.0"
    debug: bool = False
    
    # BigQuery settings
    google_cloud_project: str = os.getenv("GOOGLE_CLOUD_PROJECT", "your-gcp-project-id")
    bigquery_dataset_analytics: str = "blockchain_analytics"
    bigquery_dataset_ml: str = "blockchain_ml"
    google_application_credentials: str = os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS", 
        ""
    )
    
    # API settings
    cors_origins: list[str] = [
        "http://localhost:3000",
        "http://localhost:5173",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:5173",
    ]
    api_prefix: str = "/api"
    
    # Table names
    table_fact_transactions: str = "fact_transactions"
    table_dim_wallet: str = "dim_wallet"
    table_wallet_fraud_scores: str = "wallet_fraud_scores"
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached application settings.
    
    Returns:
        Settings: Application configuration instance
    """
    return Settings()

