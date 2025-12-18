"""
Utility functions for Data Science Pipeline.

This module provides common utilities used across the ML pipeline,
including logging, BigQuery helpers, and data processing functions.
"""

import logging
import json
import pickle
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from .config import CONFIG


# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logger(name: str, level: str = None) -> logging.Logger:
    """
    Set up a logger with consistent formatting.
    
    Args:
        name: Logger name (typically __name__)
        level: Log level (defaults to config setting)
        
    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger(name)
    level = level or CONFIG.log_level
    logger.setLevel(getattr(logging, level.upper()))
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger


# ============================================================================
# BIGQUERY HELPERS
# ============================================================================

class BigQueryMLHelper:
    """Helper class for BigQuery ML operations."""
    
    def __init__(self, project_id: str = None):
        """
        Initialize BigQuery helper.
        
        Args:
            project_id: GCP project ID (defaults to config)
        """
        import os
        # Try multiple sources for project ID
        self.project_id = (
            project_id 
            or CONFIG.bigquery.project_id 
            or os.getenv("GOOGLE_CLOUD_PROJECT") 
            or os.getenv("GCP_PROJECT")
            or ""
        )
        
        if not self.project_id:
            raise ValueError("No GCP project ID provided. Set GOOGLE_CLOUD_PROJECT environment variable.")
        
        self.client = bigquery.Client(project=self.project_id)
        self.logger = setup_logger(__name__)
        self.logger.info(f"Initialized BigQuery client for project: {self.project_id}")
    
    def ensure_dataset_exists(self, dataset_id: str) -> None:
        """Create dataset if it doesn't exist."""
        dataset_ref = f"{self.project_id}.{dataset_id}"
        try:
            self.client.get_dataset(dataset_ref)
            self.logger.debug(f"Dataset {dataset_ref} already exists")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset)
            self.logger.info(f"Created dataset {dataset_ref}")
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute a BigQuery query and return results as DataFrame.
        
        Args:
            query: SQL query string
            
        Returns:
            pd.DataFrame: Query results
        """
        self.logger.debug(f"Executing query: {query[:200]}...")
        return self.client.query(query).to_dataframe()
    
    def load_dataframe_to_table(
        self,
        df: pd.DataFrame,
        dataset_id: str,
        table_id: str,
        write_disposition: str = "WRITE_TRUNCATE"
    ) -> int:
        """
        Load a DataFrame to a BigQuery table.
        
        Args:
            df: DataFrame to load
            dataset_id: Target dataset ID
            table_id: Target table ID
            write_disposition: Write behavior (WRITE_TRUNCATE, WRITE_APPEND)
            
        Returns:
            int: Number of rows loaded
        """
        self.ensure_dataset_exists(dataset_id)
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
        )
        
        job = self.client.load_table_from_dataframe(
            df,
            table_ref,
            job_config=job_config
        )
        job.result()  # Wait for completion
        
        self.logger.info(f"Loaded {len(df)} rows to {table_ref}")
        return len(df)
    
    def table_exists(self, dataset_id: str, table_id: str) -> bool:
        """Check if a table exists."""
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        try:
            self.client.get_table(table_ref)
            return True
        except NotFound:
            return False


# ============================================================================
# DATA PROCESSING UTILITIES
# ============================================================================

def compute_statistics(series: pd.Series) -> Dict[str, float]:
    """
    Compute comprehensive statistics for a numeric series.
    
    Args:
        series: Pandas series of numeric values
        
    Returns:
        Dict: Statistics including mean, std, min, max, percentiles
    """
    return {
        "count": len(series),
        "mean": series.mean(),
        "std": series.std(),
        "min": series.min(),
        "max": series.max(),
        "median": series.median(),
        "q25": series.quantile(0.25),
        "q75": series.quantile(0.75),
        "skew": series.skew(),
        "kurtosis": series.kurtosis(),
    }


def normalize_features(
    df: pd.DataFrame,
    method: str = "standard",
    exclude_cols: List[str] = None
) -> Tuple[pd.DataFrame, Dict]:
    """
    Normalize features using specified method.
    
    Args:
        df: DataFrame with features to normalize
        method: Normalization method ('standard', 'minmax', 'robust')
        exclude_cols: Columns to exclude from normalization
        
    Returns:
        Tuple: (normalized DataFrame, scaler parameters)
    """
    from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler
    
    exclude_cols = exclude_cols or []
    feature_cols = [c for c in df.columns if c not in exclude_cols]
    
    if method == "standard":
        scaler = StandardScaler()
    elif method == "minmax":
        scaler = MinMaxScaler()
    elif method == "robust":
        scaler = RobustScaler()
    else:
        raise ValueError(f"Unknown scaling method: {method}")
    
    df_normalized = df.copy()
    df_normalized[feature_cols] = scaler.fit_transform(df[feature_cols])
    
    scaler_params = {
        "method": method,
        "feature_cols": feature_cols,
        "params": {
            "mean": scaler.mean_.tolist() if hasattr(scaler, 'mean_') else None,
            "scale": scaler.scale_.tolist() if hasattr(scaler, 'scale_') else None,
            "var": scaler.var_.tolist() if hasattr(scaler, 'var_') else None,
        }
    }
    
    return df_normalized, scaler_params


def handle_missing_values(
    df: pd.DataFrame,
    strategy: str = "median"
) -> pd.DataFrame:
    """
    Handle missing values in DataFrame.
    
    Args:
        df: DataFrame with potential missing values
        strategy: Imputation strategy ('mean', 'median', 'zero')
        
    Returns:
        pd.DataFrame: DataFrame with imputed values
    """
    df_clean = df.copy()
    
    for col in df_clean.select_dtypes(include=[np.number]).columns:
        if df_clean[col].isna().any():
            if strategy == "mean":
                fill_value = df_clean[col].mean()
            elif strategy == "median":
                fill_value = df_clean[col].median()
            elif strategy == "zero":
                fill_value = 0
            else:
                fill_value = df_clean[col].median()
            
            df_clean[col] = df_clean[col].fillna(fill_value)
    
    return df_clean


def remove_outliers(
    df: pd.DataFrame,
    columns: List[str],
    method: str = "iqr",
    threshold: float = 3.0
) -> pd.DataFrame:
    """
    Remove extreme outliers from DataFrame.
    
    Args:
        df: DataFrame with potential outliers
        columns: Columns to check for outliers
        method: Detection method ('iqr', 'zscore')
        threshold: Threshold for outlier detection
        
    Returns:
        pd.DataFrame: DataFrame with outliers removed
    """
    mask = pd.Series([True] * len(df))
    
    for col in columns:
        if method == "iqr":
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower = Q1 - threshold * IQR
            upper = Q3 + threshold * IQR
            mask &= (df[col] >= lower) & (df[col] <= upper)
        elif method == "zscore":
            mean = df[col].mean()
            std = df[col].std()
            mask &= abs(df[col] - mean) <= threshold * std
    
    return df[mask].copy()


# ============================================================================
# MODEL PERSISTENCE
# ============================================================================

def save_model(model: Any, path: str, metadata: Dict = None) -> str:
    """
    Save a trained model to disk.
    
    Args:
        model: Trained model object
        path: Directory to save model
        metadata: Optional metadata to save with model
        
    Returns:
        str: Path to saved model
    """
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_file = path / f"fraud_model_{timestamp}.pkl"
    
    with open(model_file, 'wb') as f:
        pickle.dump(model, f)
    
    if metadata:
        metadata_file = path / f"fraud_model_{timestamp}_metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
    
    return str(model_file)


def load_model(path: str) -> Any:
    """
    Load a trained model from disk.
    
    Args:
        path: Path to model file
        
    Returns:
        Any: Loaded model object
    """
    with open(path, 'rb') as f:
        return pickle.load(f)


# ============================================================================
# RISK SCORING UTILITIES
# ============================================================================

def calculate_risk_category(fraud_score: float) -> str:
    """
    Calculate risk category from fraud score.
    
    Args:
        fraud_score: Fraud score (0-1)
        
    Returns:
        str: Risk category ('low', 'medium', 'high', 'critical')
    """
    if fraud_score >= 0.9:
        return "critical"
    elif fraud_score >= CONFIG.model.high_risk_threshold:
        return "high"
    elif fraud_score >= CONFIG.model.medium_risk_threshold:
        return "medium"
    return "low"


def anomaly_score_to_probability(scores: np.ndarray) -> np.ndarray:
    """
    Convert anomaly scores to probability-like scores (0-1).
    
    Isolation Forest returns negative scores where more negative = more anomalous.
    This function converts them to 0-1 range where higher = more suspicious.
    
    Args:
        scores: Raw anomaly scores from model
        
    Returns:
        np.ndarray: Normalized scores between 0 and 1
    """
    # Isolation Forest scores: negative = anomaly, positive = normal
    # Convert to 0-1 where 1 = most anomalous
    min_score = scores.min()
    max_score = scores.max()
    
    if max_score == min_score:
        return np.zeros_like(scores)
    
    # Invert and normalize: more negative becomes closer to 1
    normalized = (max_score - scores) / (max_score - min_score)
    
    return np.clip(normalized, 0, 1)

