"""
Configuration for blockchain data ingestion.

This module contains all configuration settings for the ingestion pipeline,
including API keys, BigQuery settings, and data source configurations.
"""

import os
from dataclasses import dataclass, field
from typing import Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


@dataclass
class EtherscanConfig:
    """Etherscan API configuration."""
    api_key: str = field(default_factory=lambda: os.getenv("ETHERSCAN_API_KEY", ""))
    base_url: str = "https://api.etherscan.io/api"
    rate_limit: int = 5  # requests per second
    max_retries: int = 3
    timeout: int = 30


@dataclass
class Web3Config:
    """Web3 provider configuration."""
    provider_url: str = field(default_factory=lambda: os.getenv("WEB3_PROVIDER_URL", ""))
    timeout: int = 30
    max_retries: int = 3


@dataclass
class BigQueryConfig:
    """BigQuery configuration."""
    project_id: str = field(default_factory=lambda: os.getenv("GOOGLE_CLOUD_PROJECT", ""))
    credentials_path: str = field(default_factory=lambda: os.getenv("GOOGLE_APPLICATION_CREDENTIALS", ""))
    
    # Dataset names
    raw_dataset: str = "blockchain_raw"
    staging_dataset: str = "blockchain_staging"
    analytics_dataset: str = "blockchain_analytics"
    ml_dataset: str = "blockchain_ml"
    
    # Table names
    raw_transactions_table: str = "raw_transactions"
    raw_blocks_table: str = "raw_blocks"
    raw_wallets_table: str = "raw_wallets"
    
    # Write settings
    write_disposition: str = "WRITE_APPEND"
    batch_size: int = 10000


@dataclass
class IngestionConfig:
    """Main ingestion configuration."""
    etherscan: EtherscanConfig = field(default_factory=EtherscanConfig)
    web3: Web3Config = field(default_factory=Web3Config)
    bigquery: BigQueryConfig = field(default_factory=BigQueryConfig)
    
    # Ingestion settings
    start_block: int = 0
    end_block: Optional[int] = None
    batch_size: int = 1000
    checkpoint_enabled: bool = True
    checkpoint_table: str = "ingestion_checkpoints"
    
    # Logging
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))


def get_config() -> IngestionConfig:
    """
    Get the ingestion configuration.
    
    Returns:
        IngestionConfig: Configuration instance with all settings.
    """
    return IngestionConfig()


# Singleton config instance
CONFIG = get_config()

