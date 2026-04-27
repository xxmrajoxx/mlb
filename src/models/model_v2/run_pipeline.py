"""
Pipeline orchestrator. Runs end-to-end:

    1. Load matchup data from SQL Server (all seasons)
    2. Preprocess and split chronologically
    3. Tune hyperparameters via Optuna on val
    4. Train initial model on train, validated on val
    5. Compute SHAP on val, prune to top-N features
    6. Retrain pruned model
    7. Train bootstrap models for prediction uncertainty
    8. Evaluate on test set
    9. Generate predictions for all seasons (incl. 2026 inference)
    10. Export predictions to SQL Server
    11. Save all artifacts and plots
"""
import logging
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from model_1_k_probability import config
from model_1_k_probability import data_loader, preprocessing, train, evaluate, export

# Configure logging for the whole pipeline
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(config.ARTIFACT_DIR / "pipeline.log"),
    ],
)
logger = logging.getLogger("pipeline")


def main(force_refresh_data: bool = False, n_optuna_trials: int = None):
    logger.info("=" * 70)
    logger.info("MODEL 1: PER-MATCHUP K PROBABILITY PIPELINE")
    logger.info("=" * 70)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # --------------------------------------------------------------
    # STEP 1: Load data
    # --------------------------------------------------------------
    logger.info("[1/11] Loading data")
    df_all = data_loader.load_matchup_data(
        seasons=config.ALL_SEASONS,
        use_cache=True,
        force_refresh=force_refresh_data,
    )
    data_loader.summarize_data(df_all)

    # --------------------------------------------------------------
    # STEP 2: Preprocess
    # --------------------------------------------------------------
    logger.info("[2/11] Preprocessing")
    feature_cols = preprocessing.select_feature_columns(df_all)
    df_train_val_test = df_all[
        df_all["season"].isin(
            config.TRAIN_SEASONS + config.VAL_SEASONS + config.TEST_SEASONS
        )
    ].copy()
    df_train_val_test = preprocessing.filter_training_rows(df_train_val_test)

    # Inference set is kept without the target filter — we score everything
    df_inference_raw = df_all[df_all["season"].isin(config.INFERENCE_SEASONS)].copy()

    # Split
    train_df, val_df, test_df, inference_df = preprocessing.chronological_split(
        pd.concat([df_train_val_test, df_inference_raw], ignore_index=True)
    )

    X_train, y_train, w_train = preprocessing.get_xgb_inputs(train_df, feature_cols)
    X_val, y_val, w_val = preprocessing.get_xgb_inputs(val_df, feature_cols)
    X_test, y_test, w_test = preprocessing.get_xgb_inputs(test_df, feature_cols)

    # --------------------------------------------------------------
    # STEP 3: Tune hyperparameters
    # --------------------------------------------------------------
    logger.info("[3/11] Tuning hyperparameters with Optuna")
    best_params = train.tune_hyperparameters(
        X_train, y_train, w_train,
        X_val, y_val, w_val,
        n_trials=n_optuna_trials,
    )

    # --------------------------------------------------------------
    # STEP 4: Train initial model
    # --------------------------------------------------------------
    logger.info("[4/11] Training initial model")
    initial_model, initial_rounds = train.train_final_model(
        X_train, y_train, w_train,
        X_val, y_val, w_val,
        best_params,
    )

    # --------------------------------------------------------------
    # STEP 5: SHAP importance + prune features
    # --------------------------------------------------------------
    logger.info("[5/11] Computing SHAP importance")
    # Sample val set to keep SHAP computation tractable
    sample_size = min(5000, len(X_val))
    shap_sample_idx = np.random.RandomState(config.RANDOM_SEED).choice(
        len(X_val), size=sample_size, replace=False
    )
    shap_importance = train.compute_shap_importance(
        initial_model, X_val.iloc[shap_sample_idx]
    )
    evaluate.plot_shap_top_features(
        shap_importance,
        config.PLOT_DIR / f"shap_importance_full_{timestamp}.png",
    )

    pruned_features = train.prune_features(
        shap_importance, top_n=config.TOP_N_FEATURES_AFTER_SHAP
    )

    # --------------------------------------------------------------
    # STEP 6: Retrain on pruned features
    # --------------------------------------------------------------
    logger.info("[6/11] Retraining with pruned feature set")
    X_train_p = X_train[pruned_features]
    X_val_p = X_val[pruned_features]
    X_test_p = X_test[pruned_features]

    # Small fresh tune on the pruned set
    best_params_pruned = train.tune_hyperparameters(
        X_train_p, y_train, w_train,
        X_val_p, y_val, w_val,
        n_trials=max(20, (n_optuna_trials or config.N_OPTUNA_TRIALS) // 2),
    )
    final_model, final_rounds = train.train_final_model(
        X_train_p, y_train, w_train,
        X_val_p, y_val, w_val,
        best_params_pruned,
    )

    # --------------------------------------------------------------
    # STEP 7: Bootstrap models
    # --------------------------------------------------------------
    logger.info("[7/11] Training bootstrap models for uncertainty")
    bootstrap_models = train.train_bootstrap_models(
        X_train_p, y_train, w_train,
        X_val_p, y_val, w_val,
        best_params_pruned,
    )

    # --------------------------------------------------------------
    # STEP 8: Evaluate on test
    # --------------------------------------------------------------
    logger.info("[8/11] Evaluating on test set")
    import xgboost as xgb
    dtest = xgb.DMatrix(X_test_p)
    y_test_pred = final_model.predict(dtest)

    test_metrics = evaluate.compute_metrics(
        y_test.values, y_test_pred, w_test.values, label="TEST"
    )

    cal_table = evaluate.calibration_bins(
        y_test.values, y_test_pred, w_test.values, n_bins=10
    )
    logger.info(f"Calibration table (test):\n{cal_table.to_string()}")
    evaluate.plot_calibration(
        cal_table,
        config.PLOT_DIR / f"calibration_test_{timestamp}.png",
        title="Calibration on held-out test (2025)",
    )

    # Subgroup analysis
    test_with_pred = test_df.copy()
    test_with_pred["y_pred"] = y_test_pred
    subgroup = evaluate.evaluate_by_subgroup(
        test_with_pred,
        y_true_col=config.TARGET_COL,
        y_pred_col="y_pred",
        weight_col=config.WEIGHT_COL,
        group_cols=["pitcher_is_starter", "hitter_batting_order"],
    )
    logger.info(f"Subgroup breakdown:\n{subgroup.to_string()}")
    subgroup.to_csv(config.ARTIFACT_DIR / f"subgroup_metrics_{timestamp}.csv", index=False)

    # --------------------------------------------------------------
    # STEP 9: Generate predictions for all data (train+val+test+inference)
    # --------------------------------------------------------------
    logger.info("[9/11] Generating predictions for all rows")
    all_df_for_pred = pd.concat(
        [train_df, val_df, test_df, inference_df], ignore_index=True
    )
    X_all_p = all_df_for_pred[pruned_features].astype(np.float32)
    predictions = evaluate.predict_with_uncertainty(bootstrap_models, X_all_p)

    # --------------------------------------------------------------
    # STEP 10: Build export dataframe and write to SQL Server
    # --------------------------------------------------------------
    logger.info("[10/11] Building and exporting predictions table")
    export_df = export.build_predictions_dataframe(
        raw_df=all_df_for_pred,
        predictions=predictions,
        model=final_model,
        feature_cols=pruned_features,
    )

    # Also save CSV locally as a backup
    csv_path = config.ARTIFACT_DIR / f"predictions_{timestamp}.csv"
    export_df.to_csv(csv_path, index=False)
    logger.info(f"Saved predictions CSV backup to {csv_path}")

    try:
        export.write_predictions_to_sql(export_df)
    except Exception as e:
        logger.error(f"Failed to write to SQL Server: {e}")
        logger.warning("Predictions saved to CSV; please bulk-load manually.")

    # --------------------------------------------------------------
    # STEP 11: Save artifacts
    # --------------------------------------------------------------
    logger.info("[11/11] Saving artifacts")
    result = train.TrainingResult(
        best_params=best_params_pruned,
        feature_cols=pruned_features,
        model=final_model,
        shap_importance=shap_importance,
        val_rmse=test_metrics["rmse"],
        n_boost_rounds=final_rounds,
        timestamp=timestamp,
    )
    artifact_path = train.save_artifacts(result, bootstrap_models)

    logger.info("=" * 70)
    logger.info("PIPELINE COMPLETE")
    logger.info(f"Artifacts saved to: {artifact_path}")
    logger.info(f"Test RMSE: {test_metrics['rmse']:.6f}")
    logger.info(f"Test calibration bias: {test_metrics['calibration_bias']:+.6f}")
    logger.info("=" * 70)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--force-refresh", action="store_true",
                        help="Ignore cache and re-pull from SQL Server")
    parser.add_argument("--trials", type=int, default=None,
                        help="Number of Optuna trials (default from config)")
    args = parser.parse_args()

    main(force_refresh_data=args.force_refresh,
         n_optuna_trials=args.trials)
