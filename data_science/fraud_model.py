"""
Fraud Detection Model for Blockchain Wallets.

This module implements unsupervised machine learning models for detecting
suspicious and fraudulent wallet activity based on computed features.

Models implemented:
- Isolation Forest: Primary anomaly detection model
- Local Outlier Factor (LOF): Density-based outlier detection
- DBSCAN: Clustering-based anomaly detection
- Ensemble: Combines multiple models for robust detection

Output:
- Fraud scores saved to blockchain_ml.wallet_fraud_scores table
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn.cluster import DBSCAN
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score

from .config import CONFIG, ModelConfig
from .utils import (
    BigQueryMLHelper,
    setup_logger,
    normalize_features,
    handle_missing_values,
    save_model,
    anomaly_score_to_probability,
    calculate_risk_category,
)
from .feature_engineering import FeatureEngineer


class FraudDetector:
    """
    Fraud detection model for blockchain wallets.
    
    This class trains unsupervised ML models on wallet features
    and generates fraud scores for each wallet.
    
    Attributes:
        model: Primary trained model
        scaler: Feature scaler
        feature_engineer: Feature engineering instance
        logger: Logger instance
    
    Example:
        >>> detector = FraudDetector()
        >>> detector.fit(features_df)
        >>> scores_df = detector.predict(features_df)
        >>> detector.save_scores(scores_df)
    """
    
    def __init__(self, config: ModelConfig = None):
        """
        Initialize the fraud detector.
        
        Args:
            config: Model configuration (defaults to CONFIG.model)
        """
        self.config = config or CONFIG.model
        self.bq_config = CONFIG.bigquery
        self.bq = BigQueryMLHelper(self.bq_config.project_id)
        self.logger = setup_logger(__name__)
        self.feature_engineer = FeatureEngineer()
        
        # Models
        self.isolation_forest = None
        self.lof = None
        self.dbscan = None
        
        # Scaler
        self.scaler = StandardScaler()
        
        # Feature columns used for training
        self.feature_columns = []
        
        # Model metadata
        self.training_metadata = {}
    
    # ========================================================================
    # MODEL INITIALIZATION
    # ========================================================================
    
    def _init_isolation_forest(self) -> IsolationForest:
        """Initialize Isolation Forest model."""
        return IsolationForest(
            n_estimators=self.config.isolation_forest["n_estimators"],
            max_samples=self.config.isolation_forest["max_samples"],
            contamination=self.config.isolation_forest["contamination"],
            max_features=self.config.isolation_forest["max_features"],
            bootstrap=self.config.isolation_forest["bootstrap"],
            n_jobs=self.config.isolation_forest["n_jobs"],
            random_state=self.config.random_seed,
        )
    
    def _init_lof(self) -> LocalOutlierFactor:
        """Initialize Local Outlier Factor model."""
        return LocalOutlierFactor(
            n_neighbors=self.config.lof["n_neighbors"],
            contamination=self.config.lof["contamination"],
            novelty=self.config.lof["novelty"],
            n_jobs=self.config.lof["n_jobs"],
        )
    
    def _init_dbscan(self) -> DBSCAN:
        """Initialize DBSCAN model."""
        return DBSCAN(
            eps=self.config.dbscan["eps"],
            min_samples=self.config.dbscan["min_samples"],
            metric=self.config.dbscan["metric"],
            n_jobs=self.config.dbscan["n_jobs"],
        )
    
    # ========================================================================
    # DATA PREPARATION
    # ========================================================================
    
    def prepare_features(
        self,
        features_df: pd.DataFrame,
        fit_scaler: bool = True
    ) -> Tuple[np.ndarray, List[str]]:
        """
        Prepare features for model training/prediction.
        
        Args:
            features_df: DataFrame with wallet features
            fit_scaler: If True, fit the scaler; else use existing
            
        Returns:
            Tuple: (scaled feature array, feature column names)
        """
        # Get feature columns
        feature_cols = self.feature_engineer.get_feature_columns()
        available_cols = [c for c in feature_cols if c in features_df.columns]
        
        self.logger.info(f"Using {len(available_cols)} features for modeling")
        
        # Handle missing values
        X = features_df[available_cols].copy()
        X = X.fillna(X.median())
        
        # Replace infinities
        X = X.replace([np.inf, -np.inf], np.nan)
        X = X.fillna(0)
        
        # Scale features
        if fit_scaler:
            X_scaled = self.scaler.fit_transform(X)
            self.feature_columns = available_cols
        else:
            X_scaled = self.scaler.transform(X)
        
        return X_scaled, available_cols
    
    # ========================================================================
    # MODEL TRAINING
    # ========================================================================
    
    def fit(
        self,
        features_df: pd.DataFrame,
        model_type: str = None
    ) -> Dict:
        """
        Train the fraud detection model.
        
        Args:
            features_df: DataFrame with wallet features
            model_type: Model to train ('isolation_forest', 'lof', 'dbscan', 'ensemble')
            
        Returns:
            Dict: Training metadata and metrics
        """
        model_type = model_type or self.config.primary_model
        self.logger.info("=" * 60)
        self.logger.info(f"Training fraud detection model: {model_type}")
        self.logger.info("=" * 60)
        
        # Prepare features
        X, feature_cols = self.prepare_features(features_df, fit_scaler=True)
        
        self.logger.info(f"Training data shape: {X.shape}")
        
        # Train/test split for evaluation
        X_train, X_test = train_test_split(
            X,
            test_size=self.config.test_size,
            random_state=self.config.random_seed
        )
        
        self.logger.info(f"Train set: {X_train.shape[0]}, Test set: {X_test.shape[0]}")
        
        training_start = datetime.now(timezone.utc)
        
        if model_type == "isolation_forest" or model_type == "ensemble":
            self.logger.info("Training Isolation Forest...")
            self.isolation_forest = self._init_isolation_forest()
            self.isolation_forest.fit(X_train)
        
        if model_type == "lof" or model_type == "ensemble":
            self.logger.info("Training Local Outlier Factor...")
            self.lof = self._init_lof()
            # LOF needs to be fit on the entire data for prediction
            self.lof.fit(X)
        
        if model_type == "dbscan" or model_type == "ensemble":
            self.logger.info("Training DBSCAN...")
            self.dbscan = self._init_dbscan()
            self.dbscan.fit(X_train)
        
        training_end = datetime.now(timezone.utc)
        
        # Store metadata
        self.training_metadata = {
            "model_type": model_type,
            "training_samples": len(features_df),
            "features_used": feature_cols,
            "training_start": training_start.isoformat(),
            "training_end": training_end.isoformat(),
            "training_duration_seconds": (training_end - training_start).total_seconds(),
            "config": {
                "random_seed": self.config.random_seed,
                "test_size": self.config.test_size,
                "contamination": self.config.isolation_forest["contamination"],
            }
        }
        
        # Evaluate on test set
        metrics = self._evaluate(X_test)
        self.training_metadata["evaluation_metrics"] = metrics
        
        self.logger.info(f"Training completed in {self.training_metadata['training_duration_seconds']:.2f}s")
        
        return self.training_metadata
    
    def _evaluate(self, X_test: np.ndarray) -> Dict:
        """
        Evaluate model performance on test set.
        
        Args:
            X_test: Test feature array
            
        Returns:
            Dict: Evaluation metrics
        """
        metrics = {}
        
        if self.isolation_forest is not None:
            # Get predictions
            predictions = self.isolation_forest.predict(X_test)
            anomalies = (predictions == -1).sum()
            anomaly_ratio = anomalies / len(X_test)
            
            metrics["isolation_forest"] = {
                "test_samples": len(X_test),
                "anomalies_detected": int(anomalies),
                "anomaly_ratio": float(anomaly_ratio),
            }
            
            self.logger.info(f"Isolation Forest - Anomaly ratio: {anomaly_ratio:.2%}")
        
        if self.dbscan is not None:
            # DBSCAN labels: -1 = noise (outlier)
            labels = self.dbscan.fit_predict(X_test)
            n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
            noise_ratio = (labels == -1).sum() / len(labels)
            
            metrics["dbscan"] = {
                "n_clusters": n_clusters,
                "noise_ratio": float(noise_ratio),
            }
            
            # Compute silhouette score if we have clusters
            if n_clusters > 1:
                non_noise = labels != -1
                if non_noise.sum() > 0:
                    score = silhouette_score(X_test[non_noise], labels[non_noise])
                    metrics["dbscan"]["silhouette_score"] = float(score)
            
            self.logger.info(f"DBSCAN - Clusters: {n_clusters}, Noise ratio: {noise_ratio:.2%}")
        
        return metrics
    
    # ========================================================================
    # PREDICTION / SCORING
    # ========================================================================
    
    def predict(self, features_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate fraud scores for wallets.
        
        Args:
            features_df: DataFrame with wallet features
            
        Returns:
            pd.DataFrame: DataFrame with fraud scores
        """
        self.logger.info("Generating fraud scores...")
        
        # Prepare features
        X, _ = self.prepare_features(features_df, fit_scaler=False)
        
        # Get wallet addresses
        wallet_addresses = features_df["wallet_address"].values
        
        # Initialize scores DataFrame
        scores_df = pd.DataFrame({
            "wallet_address": wallet_addresses,
        })
        
        # Isolation Forest scores
        if self.isolation_forest is not None:
            raw_scores = self.isolation_forest.decision_function(X)
            scores_df["isolation_forest_score"] = anomaly_score_to_probability(raw_scores)
            scores_df["isolation_forest_prediction"] = self.isolation_forest.predict(X)
        
        # LOF scores (requires refitting for predict)
        if self.lof is not None:
            # LOF doesn't have predict for new data with novelty=False
            # We use negative_outlier_factor_ which is already computed during fit
            lof_scores = -self.lof.negative_outlier_factor_
            scores_df["lof_score"] = anomaly_score_to_probability(lof_scores)
        
        # DBSCAN labels
        if self.dbscan is not None:
            labels = self.dbscan.fit_predict(X)
            # Noise points (label = -1) are considered suspicious
            scores_df["dbscan_cluster"] = labels
            scores_df["dbscan_is_noise"] = (labels == -1).astype(int)
        
        # Compute ensemble score
        scores_df = self._compute_ensemble_score(scores_df)
        
        # Add risk categories
        scores_df["risk_category"] = scores_df["fraud_score"].apply(calculate_risk_category)
        
        # Add metadata
        scores_df["scored_at"] = datetime.now(timezone.utc)
        scores_df["model_version"] = "1.0.0"
        
        self.logger.info(f"Generated scores for {len(scores_df)} wallets")
        self.logger.info(f"Risk distribution: {scores_df['risk_category'].value_counts().to_dict()}")
        
        return scores_df
    
    def _compute_ensemble_score(self, scores_df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute ensemble fraud score from individual model scores.
        
        Args:
            scores_df: DataFrame with individual model scores
            
        Returns:
            pd.DataFrame: DataFrame with ensemble score added
        """
        score_columns = []
        weights = []
        
        if "isolation_forest_score" in scores_df.columns:
            score_columns.append("isolation_forest_score")
            weights.append(0.5)  # Higher weight for Isolation Forest
        
        if "lof_score" in scores_df.columns:
            score_columns.append("lof_score")
            weights.append(0.3)
        
        if "dbscan_is_noise" in scores_df.columns:
            score_columns.append("dbscan_is_noise")
            weights.append(0.2)
        
        if score_columns:
            # Normalize weights
            weights = np.array(weights) / sum(weights)
            
            # Compute weighted average
            ensemble_score = np.zeros(len(scores_df))
            for col, weight in zip(score_columns, weights):
                ensemble_score += scores_df[col].values * weight
            
            scores_df["fraud_score"] = ensemble_score
        else:
            # Fallback: use isolation forest score directly
            if "isolation_forest_score" in scores_df.columns:
                scores_df["fraud_score"] = scores_df["isolation_forest_score"]
            else:
                scores_df["fraud_score"] = 0.0
        
        return scores_df
    
    # ========================================================================
    # SAVE SCORES TO BIGQUERY
    # ========================================================================
    
    def save_scores(
        self,
        scores_df: pd.DataFrame,
        append: bool = False
    ) -> int:
        """
        Save fraud scores to BigQuery.
        
        Args:
            scores_df: DataFrame with fraud scores
            append: If True, append to existing table; else replace
            
        Returns:
            int: Number of rows saved
        """
        self.logger.info(
            f"Saving scores to {self.bq_config.ml_dataset}.{self.bq_config.wallet_fraud_scores_table}"
        )
        
        # Select columns for BigQuery
        output_columns = [
            "wallet_address",
            "fraud_score",
            "risk_category",
            "scored_at",
            "model_version",
        ]
        
        # Add model-specific scores if available
        if "isolation_forest_score" in scores_df.columns:
            output_columns.append("isolation_forest_score")
        if "lof_score" in scores_df.columns:
            output_columns.append("lof_score")
        if "dbscan_is_noise" in scores_df.columns:
            output_columns.append("dbscan_is_noise")
        
        output_df = scores_df[output_columns].copy()
        
        write_disposition = "WRITE_APPEND" if append else "WRITE_TRUNCATE"
        
        rows_saved = self.bq.load_dataframe_to_table(
            output_df,
            self.bq_config.ml_dataset,
            self.bq_config.wallet_fraud_scores_table,
            write_disposition=write_disposition
        )
        
        self.logger.info(f"Successfully saved {rows_saved} fraud scores")
        return rows_saved
    
    def save_model(self, path: str = None) -> str:
        """
        Save trained models to disk.
        
        Args:
            path: Directory to save models (defaults to config.model_path)
            
        Returns:
            str: Path to saved model
        """
        path = path or CONFIG.model_path
        
        model_data = {
            "isolation_forest": self.isolation_forest,
            "lof": self.lof,
            "dbscan": self.dbscan,
            "scaler": self.scaler,
            "feature_columns": self.feature_columns,
            "config": self.config,
        }
        
        saved_path = save_model(
            model_data,
            path,
            metadata=self.training_metadata
        )
        
        self.logger.info(f"Model saved to {saved_path}")
        return saved_path


# ============================================================================
# HIGH-LEVEL API FUNCTIONS
# ============================================================================

def train_fraud_model(
    features_df: pd.DataFrame = None,
    model_type: str = "isolation_forest"
) -> Tuple[FraudDetector, Dict]:
    """
    Train a fraud detection model.
    
    Args:
        features_df: Feature DataFrame (loads from BigQuery if None)
        model_type: Type of model to train
        
    Returns:
        Tuple: (trained detector, training metadata)
    """
    logger = setup_logger(__name__)
    
    # Initialize detector
    detector = FraudDetector()
    
    # Load features if not provided
    if features_df is None:
        logger.info("Loading features from BigQuery...")
        features_df = detector.feature_engineer.load_features()
    
    # Train model
    metadata = detector.fit(features_df, model_type=model_type)
    
    return detector, metadata


def score_wallets(
    detector: FraudDetector,
    features_df: pd.DataFrame = None
) -> pd.DataFrame:
    """
    Score wallets for fraud using trained model.
    
    Args:
        detector: Trained fraud detector
        features_df: Feature DataFrame (loads from BigQuery if None)
        
    Returns:
        pd.DataFrame: Fraud scores
    """
    # Load features if not provided
    if features_df is None:
        features_df = detector.feature_engineer.load_features()
    
    # Generate scores
    scores_df = detector.predict(features_df)
    
    return scores_df


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def run_fraud_detection_pipeline() -> pd.DataFrame:
    """
    Run the complete fraud detection pipeline.
    
    This function:
    1. Loads wallet features from BigQuery
    2. Trains the fraud detection model
    3. Scores all wallets
    4. Saves scores to BigQuery
    
    Returns:
        pd.DataFrame: Fraud scores
    """
    logger = setup_logger(__name__)
    logger.info("=" * 60)
    logger.info("FRAUD DETECTION PIPELINE")
    logger.info("=" * 60)
    
    # Initialize detector
    detector = FraudDetector()
    
    # Load features
    logger.info("Loading wallet features...")
    features_df = detector.feature_engineer.load_features()
    logger.info(f"Loaded {len(features_df)} wallet features")
    
    # Train model
    logger.info("\n--- Training Phase ---")
    metadata = detector.fit(features_df, model_type="ensemble")
    
    # Score wallets
    logger.info("\n--- Scoring Phase ---")
    scores_df = detector.predict(features_df)
    
    # Save scores
    logger.info("\n--- Saving Results ---")
    detector.save_scores(scores_df)
    
    # Save model
    if CONFIG.save_model:
        detector.save_model()
    
    # Print summary
    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Wallets scored: {len(scores_df)}")
    logger.info(f"High risk wallets: {(scores_df['risk_category'] == 'high').sum()}")
    logger.info(f"Critical risk wallets: {(scores_df['risk_category'] == 'critical').sum()}")
    
    return scores_df


if __name__ == "__main__":
    # Run as standalone script
    scores = run_fraud_detection_pipeline()
    print(f"\nFraud detection complete!")
    print(f"Total wallets scored: {len(scores)}")
    print(f"\nRisk distribution:")
    print(scores["risk_category"].value_counts())

