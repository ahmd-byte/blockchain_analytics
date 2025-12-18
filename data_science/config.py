"""
Configuration for Data Science Pipeline.

This module contains all configuration settings for the ML pipeline,
including BigQuery settings, model hyperparameters, and feature definitions.
"""

import os
from dataclasses import dataclass, field
from typing import List, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


@dataclass
class BigQueryConfig:
    """BigQuery configuration for ML pipeline."""
    project_id: str = field(default_factory=lambda: os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT") or "blockchain-481614")
    credentials_path: str = field(default_factory=lambda: os.getenv("GOOGLE_APPLICATION_CREDENTIALS", ""))
    
    # Source datasets
    raw_dataset: str = "blockchain_raw"
    analytics_dataset: str = "blockchain_analytics"
    
    # ML dataset (output)
    ml_dataset: str = "blockchain_ml"
    
    # Source tables
    raw_transactions_table: str = "raw_transactions"
    raw_wallets_table: str = "raw_wallets"
    
    # Output tables
    wallet_features_table: str = "wallet_features"
    wallet_fraud_scores_table: str = "wallet_fraud_scores"
    model_metadata_table: str = "model_metadata"


@dataclass
class FeatureConfig:
    """Feature engineering configuration."""
    # Time windows for temporal features (in days)
    time_windows: List[int] = field(default_factory=lambda: [7, 30, 90])
    
    # Minimum transactions to include a wallet
    min_transactions: int = 2
    
    # Feature scaling method
    scaling_method: str = "standard"  # 'standard', 'minmax', 'robust'
    
    # Features to compute
    basic_features: List[str] = field(default_factory=lambda: [
        "tx_count",
        "tx_count_in",
        "tx_count_out",
        "total_value",
        "total_value_in",
        "total_value_out",
        "avg_value",
        "std_value",
        "min_value",
        "max_value",
        "unique_counterparties",
        "in_out_ratio",
        "net_flow",
    ])
    
    temporal_features: List[str] = field(default_factory=lambda: [
        "tx_frequency",
        "avg_time_between_tx",
        "activity_days",
        "first_to_last_days",
        "tx_count_7d",
        "tx_count_30d",
        "value_7d",
        "value_30d",
    ])
    
    behavioral_features: List[str] = field(default_factory=lambda: [
        "avg_counterparty_value",
        "counterparty_concentration",
        "self_transactions",
        "round_value_ratio",
        "high_value_tx_ratio",
    ])


@dataclass
class ModelConfig:
    """ML model configuration."""
    # Random seed for reproducibility
    random_seed: int = 42
    
    # Train/test split
    test_size: float = 0.2
    
    # Isolation Forest parameters
    isolation_forest: dict = field(default_factory=lambda: {
        "n_estimators": 100,
        "max_samples": "auto",
        "contamination": 0.1,  # Expected proportion of outliers
        "max_features": 1.0,
        "bootstrap": False,
        "n_jobs": -1,
        "random_state": 42,
    })
    
    # DBSCAN parameters
    dbscan: dict = field(default_factory=lambda: {
        "eps": 0.5,
        "min_samples": 5,
        "metric": "euclidean",
        "n_jobs": -1,
    })
    
    # Local Outlier Factor parameters
    lof: dict = field(default_factory=lambda: {
        "n_neighbors": 20,
        "contamination": 0.1,
        "novelty": False,
        "n_jobs": -1,
    })
    
    # Fraud score thresholds
    high_risk_threshold: float = 0.7
    medium_risk_threshold: float = 0.4
    
    # Model selection
    primary_model: str = "isolation_forest"  # 'isolation_forest', 'dbscan', 'lof', 'ensemble'


@dataclass
class PipelineConfig:
    """Main pipeline configuration."""
    bigquery: BigQueryConfig = field(default_factory=BigQueryConfig)
    features: FeatureConfig = field(default_factory=FeatureConfig)
    model: ModelConfig = field(default_factory=ModelConfig)
    
    # Pipeline settings
    batch_size: int = 10000
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    
    # Output settings
    save_model: bool = True
    model_path: str = "models/"


def get_config() -> PipelineConfig:
    """
    Get the pipeline configuration.
    
    Returns:
        PipelineConfig: Configuration instance with all settings.
    """
    return PipelineConfig()


# Singleton config instance
CONFIG = get_config()

