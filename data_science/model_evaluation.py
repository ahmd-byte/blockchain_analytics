"""
Model Evaluation and Analysis Module.

This module provides tools for evaluating fraud detection models,
analyzing score distributions, and generating performance reports.

Since we use unsupervised learning (no ground truth labels), evaluation
focuses on:
- Score distributions and anomaly rates
- Cluster quality metrics
- Feature importance analysis
- Stability across time windows
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import silhouette_score, calinski_harabasz_score, davies_bouldin_score

from .config import CONFIG
from .utils import setup_logger, BigQueryMLHelper


class ModelEvaluator:
    """
    Evaluator for fraud detection models.
    
    This class provides methods to analyze model outputs and
    generate performance reports for unsupervised fraud detection.
    
    Attributes:
        logger: Logger instance
        bq: BigQuery helper
    """
    
    def __init__(self):
        """Initialize the model evaluator."""
        self.logger = setup_logger(__name__)
        self.bq = BigQueryMLHelper()
    
    # ========================================================================
    # SCORE DISTRIBUTION ANALYSIS
    # ========================================================================
    
    def analyze_score_distribution(
        self,
        scores_df: pd.DataFrame
    ) -> Dict:
        """
        Analyze the distribution of fraud scores.
        
        Args:
            scores_df: DataFrame with fraud scores
            
        Returns:
            Dict: Distribution statistics and analysis
        """
        scores = scores_df["fraud_score"]
        
        analysis = {
            "basic_stats": {
                "count": len(scores),
                "mean": float(scores.mean()),
                "std": float(scores.std()),
                "min": float(scores.min()),
                "max": float(scores.max()),
                "median": float(scores.median()),
            },
            "percentiles": {
                f"p{p}": float(scores.quantile(p/100))
                for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]
            },
            "risk_distribution": scores_df["risk_category"].value_counts().to_dict(),
            "thresholds": {
                "above_0.5": int((scores > 0.5).sum()),
                "above_0.7": int((scores > 0.7).sum()),
                "above_0.9": int((scores > 0.9).sum()),
            }
        }
        
        # Anomaly detection rate
        analysis["anomaly_rate"] = {
            "at_10_percent": float((scores > scores.quantile(0.9)).mean()),
            "at_5_percent": float((scores > scores.quantile(0.95)).mean()),
            "at_1_percent": float((scores > scores.quantile(0.99)).mean()),
        }
        
        return analysis
    
    # ========================================================================
    # FEATURE IMPORTANCE ANALYSIS
    # ========================================================================
    
    def analyze_feature_importance(
        self,
        model,
        features_df: pd.DataFrame,
        feature_columns: List[str]
    ) -> pd.DataFrame:
        """
        Analyze feature importance using permutation importance.
        
        For Isolation Forest, we use mean absolute SHAP-like importance
        based on feature contributions to anomaly scores.
        
        Args:
            model: Trained isolation forest model
            features_df: Feature DataFrame
            feature_columns: List of feature column names
            
        Returns:
            pd.DataFrame: Feature importance rankings
        """
        self.logger.info("Analyzing feature importance...")
        
        X = features_df[feature_columns].values
        base_scores = model.decision_function(X)
        
        importance_scores = []
        
        for i, col in enumerate(feature_columns):
            # Create permuted version
            X_permuted = X.copy()
            np.random.seed(CONFIG.model.random_seed)
            X_permuted[:, i] = np.random.permutation(X_permuted[:, i])
            
            # Score with permuted feature
            permuted_scores = model.decision_function(X_permuted)
            
            # Calculate importance as mean absolute difference
            importance = np.abs(base_scores - permuted_scores).mean()
            importance_scores.append({
                "feature": col,
                "importance": importance,
            })
        
        importance_df = pd.DataFrame(importance_scores)
        importance_df = importance_df.sort_values("importance", ascending=False)
        importance_df["rank"] = range(1, len(importance_df) + 1)
        importance_df["importance_normalized"] = (
            importance_df["importance"] / importance_df["importance"].sum()
        )
        
        return importance_df
    
    # ========================================================================
    # CLUSTER QUALITY METRICS
    # ========================================================================
    
    def evaluate_clustering(
        self,
        X: np.ndarray,
        labels: np.ndarray
    ) -> Dict:
        """
        Evaluate clustering quality using multiple metrics.
        
        Args:
            X: Feature array
            labels: Cluster labels
            
        Returns:
            Dict: Clustering quality metrics
        """
        metrics = {
            "n_clusters": len(set(labels)) - (1 if -1 in labels else 0),
            "n_noise": int((labels == -1).sum()),
            "noise_ratio": float((labels == -1).mean()),
        }
        
        # Only compute metrics if we have multiple clusters
        non_noise_mask = labels != -1
        if metrics["n_clusters"] > 1 and non_noise_mask.sum() > 0:
            try:
                metrics["silhouette_score"] = float(
                    silhouette_score(X[non_noise_mask], labels[non_noise_mask])
                )
            except Exception:
                metrics["silhouette_score"] = None
            
            try:
                metrics["calinski_harabasz_score"] = float(
                    calinski_harabasz_score(X[non_noise_mask], labels[non_noise_mask])
                )
            except Exception:
                metrics["calinski_harabasz_score"] = None
            
            try:
                metrics["davies_bouldin_score"] = float(
                    davies_bouldin_score(X[non_noise_mask], labels[non_noise_mask])
                )
            except Exception:
                metrics["davies_bouldin_score"] = None
        
        return metrics
    
    # ========================================================================
    # HIGH RISK WALLET ANALYSIS
    # ========================================================================
    
    def analyze_high_risk_wallets(
        self,
        scores_df: pd.DataFrame,
        features_df: pd.DataFrame,
        threshold: float = 0.7
    ) -> pd.DataFrame:
        """
        Analyze characteristics of high-risk wallets.
        
        Args:
            scores_df: DataFrame with fraud scores
            features_df: DataFrame with wallet features
            threshold: Fraud score threshold for high risk
            
        Returns:
            pd.DataFrame: Analysis of high-risk wallet characteristics
        """
        # Merge scores with features
        merged = scores_df.merge(features_df, on="wallet_address")
        
        # Split into high risk and normal
        high_risk = merged[merged["fraud_score"] >= threshold]
        normal = merged[merged["fraud_score"] < threshold]
        
        self.logger.info(f"High risk wallets: {len(high_risk)}")
        self.logger.info(f"Normal wallets: {len(normal)}")
        
        # Compare feature distributions
        feature_cols = [c for c in features_df.columns 
                       if c not in ["wallet_address", "feature_timestamp", "feature_version"]]
        
        comparison = []
        for col in feature_cols:
            if col in merged.columns:
                try:
                    comparison.append({
                        "feature": col,
                        "high_risk_mean": high_risk[col].mean(),
                        "normal_mean": normal[col].mean(),
                        "ratio": high_risk[col].mean() / max(normal[col].mean(), 1e-10),
                        "high_risk_std": high_risk[col].std(),
                        "normal_std": normal[col].std(),
                    })
                except Exception:
                    pass
        
        comparison_df = pd.DataFrame(comparison)
        comparison_df = comparison_df.sort_values("ratio", ascending=False)
        
        return comparison_df
    
    # ========================================================================
    # REPORT GENERATION
    # ========================================================================
    
    def generate_report(
        self,
        scores_df: pd.DataFrame,
        features_df: pd.DataFrame,
        model_metadata: Dict = None
    ) -> Dict:
        """
        Generate a comprehensive evaluation report.
        
        Args:
            scores_df: DataFrame with fraud scores
            features_df: DataFrame with wallet features
            model_metadata: Optional model training metadata
            
        Returns:
            Dict: Comprehensive evaluation report
        """
        self.logger.info("Generating evaluation report...")
        
        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": {
                "total_wallets": len(scores_df),
                "features_count": len(features_df.columns) - 3,  # Exclude metadata
            },
            "score_distribution": self.analyze_score_distribution(scores_df),
            "high_risk_analysis": None,
            "model_metadata": model_metadata,
        }
        
        # High risk analysis
        high_risk_comparison = self.analyze_high_risk_wallets(scores_df, features_df)
        report["high_risk_analysis"] = {
            "top_distinguishing_features": high_risk_comparison.head(10).to_dict("records")
        }
        
        # Add risk counts
        report["summary"]["risk_counts"] = scores_df["risk_category"].value_counts().to_dict()
        
        return report
    
    # ========================================================================
    # VISUALIZATION
    # ========================================================================
    
    def plot_score_distribution(
        self,
        scores_df: pd.DataFrame,
        save_path: str = None
    ) -> None:
        """
        Plot the distribution of fraud scores.
        
        Args:
            scores_df: DataFrame with fraud scores
            save_path: Optional path to save the plot
        """
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))
        
        # Histogram
        axes[0].hist(scores_df["fraud_score"], bins=50, edgecolor="black", alpha=0.7)
        axes[0].axvline(x=0.4, color="orange", linestyle="--", label="Medium threshold")
        axes[0].axvline(x=0.7, color="red", linestyle="--", label="High threshold")
        axes[0].set_xlabel("Fraud Score")
        axes[0].set_ylabel("Count")
        axes[0].set_title("Fraud Score Distribution")
        axes[0].legend()
        
        # Risk category bar chart
        risk_counts = scores_df["risk_category"].value_counts()
        colors = {"low": "green", "medium": "orange", "high": "red", "critical": "darkred"}
        bar_colors = [colors.get(cat, "gray") for cat in risk_counts.index]
        axes[1].bar(risk_counts.index, risk_counts.values, color=bar_colors)
        axes[1].set_xlabel("Risk Category")
        axes[1].set_ylabel("Count")
        axes[1].set_title("Wallets by Risk Category")
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches="tight")
            self.logger.info(f"Plot saved to {save_path}")
        
        plt.close()
    
    def plot_feature_importance(
        self,
        importance_df: pd.DataFrame,
        top_n: int = 15,
        save_path: str = None
    ) -> None:
        """
        Plot feature importance rankings.
        
        Args:
            importance_df: DataFrame with feature importance scores
            top_n: Number of top features to show
            save_path: Optional path to save the plot
        """
        fig, ax = plt.subplots(figsize=(10, 8))
        
        top_features = importance_df.head(top_n)
        
        ax.barh(
            range(len(top_features)),
            top_features["importance_normalized"],
            color="steelblue"
        )
        ax.set_yticks(range(len(top_features)))
        ax.set_yticklabels(top_features["feature"])
        ax.invert_yaxis()
        ax.set_xlabel("Normalized Importance")
        ax.set_title(f"Top {top_n} Most Important Features")
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches="tight")
            self.logger.info(f"Plot saved to {save_path}")
        
        plt.close()


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def evaluate_model(
    scores_df: pd.DataFrame,
    features_df: pd.DataFrame,
    model_metadata: Dict = None
) -> Dict:
    """
    Evaluate a trained fraud detection model.
    
    Args:
        scores_df: DataFrame with fraud scores
        features_df: DataFrame with wallet features
        model_metadata: Optional model training metadata
        
    Returns:
        Dict: Evaluation report
    """
    evaluator = ModelEvaluator()
    return evaluator.generate_report(scores_df, features_df, model_metadata)

