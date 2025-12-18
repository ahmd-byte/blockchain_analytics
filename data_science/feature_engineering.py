"""
Feature Engineering Pipeline for Blockchain Fraud Detection.

This module computes wallet-level features from blockchain transaction data
stored in BigQuery. Features are designed to capture patterns indicative
of suspicious or fraudulent behavior.

Features computed:
- Basic: transaction counts, values, averages
- Behavioral: counterparty patterns, transaction habits
- Temporal: activity over time windows
- Network: graph-based features

Output:
- Features saved to blockchain_ml.wallet_features table
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .config import CONFIG, BigQueryConfig
from .utils import (
    BigQueryMLHelper,
    setup_logger,
    handle_missing_values,
    compute_statistics,
)


class FeatureEngineer:
    """
    Feature engineering pipeline for wallet fraud detection.
    
    This class extracts, transforms, and loads wallet-level features
    from raw blockchain transaction data in BigQuery.
    
    Attributes:
        bq: BigQuery helper instance
        logger: Logger instance
        config: Feature configuration
    
    Example:
        >>> engineer = FeatureEngineer()
        >>> features_df = engineer.compute_all_features()
        >>> engineer.save_features(features_df)
    """
    
    def __init__(self, config: BigQueryConfig = None):
        """
        Initialize the feature engineering pipeline.
        
        Args:
            config: BigQuery configuration (defaults to CONFIG.bigquery)
        """
        self.config = config or CONFIG.bigquery
        self.bq = BigQueryMLHelper(self.config.project_id)
        self.logger = setup_logger(__name__)
        self.feature_config = CONFIG.features
    
    # ========================================================================
    # BIGQUERY SQL QUERIES FOR FEATURE EXTRACTION
    # ========================================================================
    
    def _get_basic_features_query(self) -> str:
        """
        Generate SQL query for basic wallet features.
        
        Returns:
            str: SQL query for basic features
        """
        return f"""
        WITH wallet_transactions AS (
            -- Get all transactions with wallet as sender
            SELECT
                from_address AS wallet_address,
                value_eth,
                gas_price,
                gas_used,
                to_address AS counterparty,
                transaction_timestamp AS block_timestamp,
                'out' AS direction
            FROM `{self.config.project_id}.{self.config.raw_dataset}.{self.config.raw_transactions_table}`
            WHERE from_address IS NOT NULL
            
            UNION ALL
            
            -- Get all transactions with wallet as receiver
            SELECT
                to_address AS wallet_address,
                value_eth,
                gas_price,
                gas_used,
                from_address AS counterparty,
                transaction_timestamp AS block_timestamp,
                'in' AS direction
            FROM `{self.config.project_id}.{self.config.raw_dataset}.{self.config.raw_transactions_table}`
            WHERE to_address IS NOT NULL
        ),
        
        basic_features AS (
            SELECT
                wallet_address,
                
                -- Transaction counts
                COUNT(*) AS tx_count,
                COUNTIF(direction = 'in') AS tx_count_in,
                COUNTIF(direction = 'out') AS tx_count_out,
                
                -- Value statistics
                SUM(value_eth) AS total_value,
                SUM(CASE WHEN direction = 'in' THEN value_eth ELSE 0 END) AS total_value_in,
                SUM(CASE WHEN direction = 'out' THEN value_eth ELSE 0 END) AS total_value_out,
                AVG(value_eth) AS avg_value,
                STDDEV(value_eth) AS std_value,
                MIN(value_eth) AS min_value,
                MAX(value_eth) AS max_value,
                
                -- Counterparty analysis
                COUNT(DISTINCT counterparty) AS unique_counterparties,
                
                -- Gas analysis
                AVG(gas_used) AS avg_gas_used,
                AVG(gas_price) AS avg_gas_price,
                
                -- Time range
                MIN(block_timestamp) AS first_tx_time,
                MAX(block_timestamp) AS last_tx_time,
                TIMESTAMP_DIFF(MAX(block_timestamp), MIN(block_timestamp), DAY) AS activity_span_days,
                COUNT(DISTINCT DATE(block_timestamp)) AS active_days
                
            FROM wallet_transactions
            GROUP BY wallet_address
            HAVING COUNT(*) >= {self.feature_config.min_transactions}
        )
        
        SELECT
            wallet_address,
            tx_count,
            tx_count_in,
            tx_count_out,
            total_value,
            total_value_in,
            total_value_out,
            avg_value,
            COALESCE(std_value, 0) AS std_value,
            min_value,
            max_value,
            unique_counterparties,
            avg_gas_used,
            avg_gas_price,
            first_tx_time,
            last_tx_time,
            activity_span_days,
            active_days,
            
            -- Derived features
            SAFE_DIVIDE(tx_count_in, tx_count_out) AS in_out_ratio,
            total_value_in - total_value_out AS net_flow,
            SAFE_DIVIDE(tx_count, GREATEST(active_days, 1)) AS tx_per_active_day,
            SAFE_DIVIDE(total_value, tx_count) AS value_per_tx
            
        FROM basic_features
        """
    
    def _get_behavioral_features_query(self) -> str:
        """
        Generate SQL query for behavioral wallet features.
        
        Returns:
            str: SQL query for behavioral features
        """
        return f"""
        WITH wallet_transactions AS (
            SELECT
                from_address AS wallet_address,
                value_eth,
                to_address AS counterparty,
                transaction_timestamp AS block_timestamp,
                'out' AS direction
            FROM `{self.config.project_id}.{self.config.raw_dataset}.{self.config.raw_transactions_table}`
            WHERE from_address IS NOT NULL
            
            UNION ALL
            
            SELECT
                to_address AS wallet_address,
                value_eth,
                from_address AS counterparty,
                transaction_timestamp AS block_timestamp,
                'in' AS direction
            FROM `{self.config.project_id}.{self.config.raw_dataset}.{self.config.raw_transactions_table}`
            WHERE to_address IS NOT NULL
        ),
        
        counterparty_stats AS (
            SELECT
                wallet_address,
                counterparty,
                COUNT(*) AS tx_with_counterparty,
                SUM(value_eth) AS value_with_counterparty
            FROM wallet_transactions
            GROUP BY wallet_address, counterparty
        ),
        
        counterparty_features AS (
            SELECT
                wallet_address,
                AVG(value_with_counterparty) AS avg_counterparty_value,
                MAX(tx_with_counterparty) / SUM(tx_with_counterparty) AS counterparty_concentration,
                COUNTIF(wallet_address = counterparty) AS self_transactions
            FROM counterparty_stats
            GROUP BY wallet_address
        ),
        
        value_patterns AS (
            SELECT
                wallet_address,
                -- Round value analysis (suspicious if many round numbers)
                SAFE_DIVIDE(
                    COUNTIF(MOD(CAST(value_eth * 1000 AS INT64), 1000) = 0),
                    COUNT(*)
                ) AS round_value_ratio,
                -- High value transaction ratio
                SAFE_DIVIDE(
                    COUNTIF(value_eth > 10),  -- Transactions > 10 ETH
                    COUNT(*)
                ) AS high_value_tx_ratio,
                -- Zero value transaction ratio
                SAFE_DIVIDE(
                    COUNTIF(value_eth = 0),
                    COUNT(*)
                ) AS zero_value_tx_ratio
            FROM wallet_transactions
            GROUP BY wallet_address
            HAVING COUNT(*) >= {self.feature_config.min_transactions}
        )
        
        SELECT
            cf.wallet_address,
            COALESCE(cf.avg_counterparty_value, 0) AS avg_counterparty_value,
            COALESCE(cf.counterparty_concentration, 0) AS counterparty_concentration,
            COALESCE(cf.self_transactions, 0) AS self_transactions,
            COALESCE(vp.round_value_ratio, 0) AS round_value_ratio,
            COALESCE(vp.high_value_tx_ratio, 0) AS high_value_tx_ratio,
            COALESCE(vp.zero_value_tx_ratio, 0) AS zero_value_tx_ratio
        FROM counterparty_features cf
        LEFT JOIN value_patterns vp ON cf.wallet_address = vp.wallet_address
        """
    
    def _get_temporal_features_query(self) -> str:
        """
        Generate SQL query for temporal wallet features.
        
        Returns:
            str: SQL query for temporal features
        """
        return f"""
        WITH wallet_transactions AS (
            SELECT
                from_address AS wallet_address,
                value_eth,
                transaction_timestamp AS block_timestamp,
                'out' AS direction
            FROM `{self.config.project_id}.{self.config.raw_dataset}.{self.config.raw_transactions_table}`
            WHERE from_address IS NOT NULL
            
            UNION ALL
            
            SELECT
                to_address AS wallet_address,
                value_eth,
                transaction_timestamp AS block_timestamp,
                'in' AS direction
            FROM `{self.config.project_id}.{self.config.raw_dataset}.{self.config.raw_transactions_table}`
            WHERE to_address IS NOT NULL
        ),
        
        -- Get current timestamp for recency calculations
        current_time AS (
            SELECT MAX(block_timestamp) AS max_time
            FROM wallet_transactions
        ),
        
        temporal_features AS (
            SELECT
                wt.wallet_address,
                
                -- Transaction frequency
                COUNT(*) / GREATEST(
                    TIMESTAMP_DIFF(MAX(wt.block_timestamp), MIN(wt.block_timestamp), HOUR), 
                    1
                ) AS tx_frequency_per_hour,
                
                -- Average time between transactions (in hours)
                SAFE_DIVIDE(
                    TIMESTAMP_DIFF(MAX(wt.block_timestamp), MIN(wt.block_timestamp), HOUR),
                    GREATEST(COUNT(*) - 1, 1)
                ) AS avg_hours_between_tx,
                
                -- Recent activity (last 7 days from max timestamp)
                COUNTIF(wt.block_timestamp >= TIMESTAMP_SUB(ct.max_time, INTERVAL 7 DAY)) AS tx_count_7d,
                SUM(CASE WHEN wt.block_timestamp >= TIMESTAMP_SUB(ct.max_time, INTERVAL 7 DAY) THEN wt.value_eth ELSE 0 END) AS value_7d,
                
                -- Activity in last 30 days
                COUNTIF(wt.block_timestamp >= TIMESTAMP_SUB(ct.max_time, INTERVAL 30 DAY)) AS tx_count_30d,
                SUM(CASE WHEN wt.block_timestamp >= TIMESTAMP_SUB(ct.max_time, INTERVAL 30 DAY) THEN wt.value_eth ELSE 0 END) AS value_30d,
                
                -- Hour of day distribution (entropy-like)
                COUNT(DISTINCT EXTRACT(HOUR FROM wt.block_timestamp)) AS unique_hours_active,
                
                -- Day of week distribution
                COUNT(DISTINCT EXTRACT(DAYOFWEEK FROM wt.block_timestamp)) AS unique_days_of_week_active,
                
                -- Weekend activity ratio
                SAFE_DIVIDE(
                    COUNTIF(EXTRACT(DAYOFWEEK FROM wt.block_timestamp) IN (1, 7)),
                    COUNT(*)
                ) AS weekend_tx_ratio,
                
                -- Night activity ratio (0-6 hours)
                SAFE_DIVIDE(
                    COUNTIF(EXTRACT(HOUR FROM wt.block_timestamp) BETWEEN 0 AND 6),
                    COUNT(*)
                ) AS night_tx_ratio
                
            FROM wallet_transactions wt
            CROSS JOIN current_time ct
            GROUP BY wt.wallet_address
            HAVING COUNT(*) >= {self.feature_config.min_transactions}
        )
        
        SELECT * FROM temporal_features
        """
    
    # ========================================================================
    # FEATURE COMPUTATION METHODS
    # ========================================================================
    
    def compute_basic_features(self) -> pd.DataFrame:
        """
        Compute basic wallet features from transactions.
        
        Returns:
            pd.DataFrame: DataFrame with basic features per wallet
        """
        self.logger.info("Computing basic wallet features...")
        query = self._get_basic_features_query()
        df = self.bq.execute_query(query)
        self.logger.info(f"Computed basic features for {len(df)} wallets")
        return df
    
    def compute_behavioral_features(self) -> pd.DataFrame:
        """
        Compute behavioral wallet features.
        
        Returns:
            pd.DataFrame: DataFrame with behavioral features per wallet
        """
        self.logger.info("Computing behavioral wallet features...")
        query = self._get_behavioral_features_query()
        df = self.bq.execute_query(query)
        self.logger.info(f"Computed behavioral features for {len(df)} wallets")
        return df
    
    def compute_temporal_features(self) -> pd.DataFrame:
        """
        Compute temporal wallet features.
        
        Returns:
            pd.DataFrame: DataFrame with temporal features per wallet
        """
        self.logger.info("Computing temporal wallet features...")
        query = self._get_temporal_features_query()
        df = self.bq.execute_query(query)
        self.logger.info(f"Computed temporal features for {len(df)} wallets")
        return df
    
    def compute_all_features(self) -> pd.DataFrame:
        """
        Compute all wallet features and merge them.
        
        This is the main entry point for feature computation.
        It computes basic, behavioral, and temporal features,
        then merges them into a single feature DataFrame.
        
        Returns:
            pd.DataFrame: Complete feature DataFrame per wallet
        """
        self.logger.info("=" * 60)
        self.logger.info("Starting feature engineering pipeline")
        self.logger.info("=" * 60)
        
        # Compute each feature set
        basic_df = self.compute_basic_features()
        behavioral_df = self.compute_behavioral_features()
        temporal_df = self.compute_temporal_features()
        
        # Merge all features
        self.logger.info("Merging feature sets...")
        features_df = basic_df.merge(
            behavioral_df,
            on="wallet_address",
            how="left"
        ).merge(
            temporal_df,
            on="wallet_address",
            how="left"
        )
        
        # Handle missing values
        features_df = handle_missing_values(features_df, strategy="median")
        
        # Add metadata
        features_df["feature_timestamp"] = datetime.now(timezone.utc)
        features_df["feature_version"] = "1.0.0"
        
        self.logger.info(f"Total features computed: {len(features_df.columns) - 3}")  # Exclude metadata
        self.logger.info(f"Total wallets processed: {len(features_df)}")
        self.logger.info("Feature engineering completed")
        
        return features_df
    
    def get_feature_columns(self) -> List[str]:
        """
        Get list of feature column names (excluding metadata).
        
        Returns:
            List[str]: Feature column names
        """
        return [
            # Basic features
            "tx_count", "tx_count_in", "tx_count_out",
            "total_value", "total_value_in", "total_value_out",
            "avg_value", "std_value", "min_value", "max_value",
            "unique_counterparties", "avg_gas_used", "avg_gas_price",
            "activity_span_days", "active_days",
            "in_out_ratio", "net_flow", "tx_per_active_day", "value_per_tx",
            # Behavioral features
            "avg_counterparty_value", "counterparty_concentration",
            "self_transactions", "round_value_ratio",
            "high_value_tx_ratio", "zero_value_tx_ratio",
            # Temporal features
            "tx_frequency_per_hour", "avg_hours_between_tx",
            "tx_count_7d", "value_7d", "tx_count_30d", "value_30d",
            "unique_hours_active", "unique_days_of_week_active",
            "weekend_tx_ratio", "night_tx_ratio",
        ]
    
    # ========================================================================
    # SAVE FEATURES TO BIGQUERY
    # ========================================================================
    
    def save_features(
        self,
        features_df: pd.DataFrame,
        append: bool = False
    ) -> int:
        """
        Save computed features to BigQuery.
        
        Args:
            features_df: DataFrame with computed features
            append: If True, append to existing table; else replace
            
        Returns:
            int: Number of rows saved
        """
        self.logger.info(f"Saving features to {self.config.ml_dataset}.{self.config.wallet_features_table}")
        
        write_disposition = "WRITE_APPEND" if append else "WRITE_TRUNCATE"
        
        rows_saved = self.bq.load_dataframe_to_table(
            features_df,
            self.config.ml_dataset,
            self.config.wallet_features_table,
            write_disposition=write_disposition
        )
        
        self.logger.info(f"Successfully saved {rows_saved} feature rows")
        return rows_saved
    
    def load_features(self) -> pd.DataFrame:
        """
        Load features from BigQuery for model training.
        
        Returns:
            pd.DataFrame: Feature DataFrame
        """
        query = f"""
        SELECT *
        FROM `{self.config.project_id}.{self.config.ml_dataset}.{self.config.wallet_features_table}`
        ORDER BY feature_timestamp DESC
        """
        return self.bq.execute_query(query)
    
    def get_feature_statistics(self, features_df: pd.DataFrame) -> Dict:
        """
        Compute summary statistics for features.
        
        Args:
            features_df: Feature DataFrame
            
        Returns:
            Dict: Statistics for each feature
        """
        feature_cols = self.get_feature_columns()
        stats = {}
        
        for col in feature_cols:
            if col in features_df.columns:
                stats[col] = compute_statistics(features_df[col])
        
        return stats


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def run_feature_engineering() -> pd.DataFrame:
    """
    Run the complete feature engineering pipeline.
    
    This is the main entry point for computing and saving wallet features.
    
    Returns:
        pd.DataFrame: Computed features
    """
    logger = setup_logger(__name__)
    logger.info("Initializing feature engineering pipeline...")
    
    # Initialize engineer
    engineer = FeatureEngineer()
    
    # Compute features
    features_df = engineer.compute_all_features()
    
    # Save to BigQuery
    engineer.save_features(features_df)
    
    # Log statistics
    stats = engineer.get_feature_statistics(features_df)
    logger.info(f"Feature statistics computed for {len(stats)} features")
    
    return features_df


if __name__ == "__main__":
    # Run as standalone script
    features = run_feature_engineering()
    print(f"\nFeature engineering complete!")
    print(f"Total wallets: {len(features)}")
    print(f"Total features: {len(features.columns)}")

