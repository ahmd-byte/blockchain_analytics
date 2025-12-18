"""
Main Pipeline Runner for Data Science Module.

This script orchestrates the complete data science pipeline:
1. Feature engineering - compute wallet features
2. Model training - train fraud detection models
3. Scoring - generate fraud scores for wallets
4. Evaluation - analyze model performance
5. Save results - persist to BigQuery

Usage:
    python -m data_science.run_pipeline [--mode MODE]
    
Modes:
    - full: Run complete pipeline (default)
    - features: Only compute features
    - train: Only train model (assumes features exist)
    - score: Only score wallets (assumes model exists)
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from .config import CONFIG
from .utils import setup_logger, BigQueryMLHelper
from .feature_engineering import FeatureEngineer, run_feature_engineering
from .fraud_model import FraudDetector, run_fraud_detection_pipeline
from .model_evaluation import ModelEvaluator, evaluate_model


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run the blockchain fraud detection pipeline"
    )
    parser.add_argument(
        "--mode",
        type=str,
        default="full",
        choices=["full", "features", "train", "score"],
        help="Pipeline mode to run"
    )
    parser.add_argument(
        "--model-type",
        type=str,
        default="ensemble",
        choices=["isolation_forest", "lof", "dbscan", "ensemble"],
        help="Type of model to train"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="outputs/",
        help="Directory for output files"
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Don't save results to BigQuery"
    )
    
    return parser.parse_args()


def run_full_pipeline(
    model_type: str = "ensemble",
    save_results: bool = True,
    output_dir: str = "outputs/"
) -> dict:
    """
    Run the complete data science pipeline.
    
    Args:
        model_type: Type of model to train
        save_results: Whether to save results to BigQuery
        output_dir: Directory for output files
        
    Returns:
        dict: Pipeline results and metadata
    """
    logger = setup_logger(__name__)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    pipeline_start = datetime.now(timezone.utc)
    results = {
        "pipeline_start": pipeline_start.isoformat(),
        "mode": "full",
        "model_type": model_type,
    }
    
    logger.info("=" * 70)
    logger.info("BLOCKCHAIN FRAUD DETECTION PIPELINE")
    logger.info("=" * 70)
    
    # =========================================================================
    # STEP 1: FEATURE ENGINEERING
    # =========================================================================
    logger.info("\n" + "=" * 70)
    logger.info("STEP 1: FEATURE ENGINEERING")
    logger.info("=" * 70)
    
    feature_engineer = FeatureEngineer()
    features_df = feature_engineer.compute_all_features()
    
    if save_results:
        feature_engineer.save_features(features_df)
    
    results["features"] = {
        "wallets_processed": len(features_df),
        "features_computed": len(features_df.columns) - 3,
    }
    
    # =========================================================================
    # STEP 2: MODEL TRAINING
    # =========================================================================
    logger.info("\n" + "=" * 70)
    logger.info("STEP 2: MODEL TRAINING")
    logger.info("=" * 70)
    
    detector = FraudDetector()
    training_metadata = detector.fit(features_df, model_type=model_type)
    
    results["training"] = training_metadata
    
    # Save model
    if save_results:
        model_path = detector.save_model(str(output_path / "models"))
        results["model_path"] = model_path
    
    # =========================================================================
    # STEP 3: SCORING
    # =========================================================================
    logger.info("\n" + "=" * 70)
    logger.info("STEP 3: SCORING WALLETS")
    logger.info("=" * 70)
    
    scores_df = detector.predict(features_df)
    
    if save_results:
        detector.save_scores(scores_df)
    
    results["scoring"] = {
        "wallets_scored": len(scores_df),
        "risk_distribution": scores_df["risk_category"].value_counts().to_dict(),
    }
    
    # =========================================================================
    # STEP 4: EVALUATION
    # =========================================================================
    logger.info("\n" + "=" * 70)
    logger.info("STEP 4: MODEL EVALUATION")
    logger.info("=" * 70)
    
    evaluator = ModelEvaluator()
    evaluation_report = evaluator.generate_report(
        scores_df, features_df, training_metadata
    )
    
    results["evaluation"] = evaluation_report
    
    # Generate plots
    if save_results:
        evaluator.plot_score_distribution(
            scores_df, 
            str(output_path / "score_distribution.png")
        )
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    pipeline_end = datetime.now(timezone.utc)
    results["pipeline_end"] = pipeline_end.isoformat()
    results["duration_seconds"] = (pipeline_end - pipeline_start).total_seconds()
    
    logger.info("\n" + "=" * 70)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Total duration: {results['duration_seconds']:.2f} seconds")
    logger.info(f"Wallets processed: {len(features_df)}")
    logger.info(f"High risk wallets: {results['scoring']['risk_distribution'].get('high', 0)}")
    logger.info(f"Critical risk wallets: {results['scoring']['risk_distribution'].get('critical', 0)}")
    
    # Save results to JSON
    if save_results:
        results_file = output_path / f"pipeline_results_{pipeline_start.strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_file, "w") as f:
            json.dump(results, f, indent=2, default=str)
        logger.info(f"Results saved to {results_file}")
    
    return results


def run_features_only(save_results: bool = True) -> dict:
    """Run only the feature engineering step."""
    logger = setup_logger(__name__)
    
    logger.info("Running feature engineering only...")
    feature_engineer = FeatureEngineer()
    features_df = feature_engineer.compute_all_features()
    
    if save_results:
        feature_engineer.save_features(features_df)
    
    return {
        "mode": "features",
        "wallets_processed": len(features_df),
        "features_computed": len(features_df.columns) - 3,
    }


def run_training_only(model_type: str = "ensemble", save_results: bool = True) -> dict:
    """Run only the model training step (assumes features exist)."""
    logger = setup_logger(__name__)
    
    logger.info("Running model training only...")
    
    # Load existing features
    feature_engineer = FeatureEngineer()
    features_df = feature_engineer.load_features()
    
    # Train model
    detector = FraudDetector()
    metadata = detector.fit(features_df, model_type=model_type)
    
    if save_results:
        detector.save_model()
    
    return {
        "mode": "train",
        "training_metadata": metadata,
    }


def run_scoring_only(model_path: str = None, save_results: bool = True) -> dict:
    """Run only the scoring step (assumes model exists)."""
    logger = setup_logger(__name__)
    
    logger.info("Running scoring only...")
    
    # Load existing features
    feature_engineer = FeatureEngineer()
    features_df = feature_engineer.load_features()
    
    # Initialize detector and score
    detector = FraudDetector()
    
    # Train on existing features (would normally load saved model)
    detector.fit(features_df)
    scores_df = detector.predict(features_df)
    
    if save_results:
        detector.save_scores(scores_df)
    
    return {
        "mode": "score",
        "wallets_scored": len(scores_df),
        "risk_distribution": scores_df["risk_category"].value_counts().to_dict(),
    }


def main():
    """Main entry point for the pipeline."""
    args = parse_args()
    
    if args.mode == "full":
        results = run_full_pipeline(
            model_type=args.model_type,
            save_results=not args.no_save,
            output_dir=args.output_dir
        )
    elif args.mode == "features":
        results = run_features_only(save_results=not args.no_save)
    elif args.mode == "train":
        results = run_training_only(
            model_type=args.model_type,
            save_results=not args.no_save
        )
    elif args.mode == "score":
        results = run_scoring_only(save_results=not args.no_save)
    else:
        print(f"Unknown mode: {args.mode}")
        sys.exit(1)
    
    print("\nPipeline completed successfully!")
    print(json.dumps(results, indent=2, default=str))


if __name__ == "__main__":
    main()

