"""
Training module: Optuna hyperparameter tuning, XGBoost model fitting,
SHAP-based feature pruning, bootstrap models for uncertainty.
"""
import json
import logging
import pickle
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import optuna
import pandas as pd
import shap
import xgboost as xgb

from model_1_k_probability import config

logger = logging.getLogger(__name__)

# Silence Optuna's per-trial logging; we log our own summaries
optuna.logging.set_verbosity(optuna.logging.WARNING)


@dataclass
class TrainingResult:
    """Bundle of everything produced by training — saved as an artifact."""
    best_params: Dict
    feature_cols: List[str]
    model: xgb.Booster
    shap_importance: pd.DataFrame
    val_rmse: float
    n_boost_rounds: int
    timestamp: str


def _objective(trial: optuna.Trial, X_train, y_train, w_train,
               X_val, y_val, w_val) -> float:
    """Optuna objective: minimize weighted RMSE on validation set."""
    params = {
        "objective": config.XGB_OBJECTIVE,
        "eval_metric": config.XGB_EVAL_METRIC,
        "seed": config.RANDOM_SEED,
        "tree_method": "hist",
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.15, log=True),
        "max_depth": trial.suggest_int("max_depth", 3, 10),
        "min_child_weight": trial.suggest_float("min_child_weight", 1.0, 20.0),
        "subsample": trial.suggest_float("subsample", 0.5, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.3, 1.0),
        "colsample_bylevel": trial.suggest_float("colsample_bylevel", 0.3, 1.0),
        "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
        "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
        "gamma": trial.suggest_float("gamma", 1e-8, 5.0, log=True),
    }

    dtrain = xgb.DMatrix(X_train, label=y_train, weight=w_train)
    dval = xgb.DMatrix(X_val, label=y_val, weight=w_val)

    model = xgb.train(
        params,
        dtrain,
        num_boost_round=config.MAX_BOOST_ROUNDS,
        evals=[(dval, "val")],
        early_stopping_rounds=config.EARLY_STOPPING_ROUNDS,
        verbose_eval=False,
    )

    return float(model.best_score)


def tune_hyperparameters(X_train, y_train, w_train,
                         X_val, y_val, w_val,
                         n_trials: int = None) -> Dict:
    """Run Optuna study and return best params."""
    n_trials = n_trials or config.N_OPTUNA_TRIALS

    logger.info(f"Starting Optuna study with {n_trials} trials")
    study = optuna.create_study(
        direction="minimize",
        sampler=optuna.samplers.TPESampler(seed=config.RANDOM_SEED),
    )
    study.optimize(
        lambda t: _objective(t, X_train, y_train, w_train, X_val, y_val, w_val),
        n_trials=n_trials,
        show_progress_bar=True,
    )

    logger.info(f"Best validation RMSE: {study.best_value:.6f}")
    logger.info(f"Best params: {study.best_params}")
    return study.best_params


def train_final_model(X_train, y_train, w_train,
                      X_val, y_val, w_val,
                      params: Dict) -> Tuple[xgb.Booster, int]:
    """Train final model on train set with early stopping on val."""
    full_params = {
        "objective": config.XGB_OBJECTIVE,
        "eval_metric": config.XGB_EVAL_METRIC,
        "seed": config.RANDOM_SEED,
        "tree_method": "hist",
        **params,
    }

    dtrain = xgb.DMatrix(X_train, label=y_train, weight=w_train)
    dval = xgb.DMatrix(X_val, label=y_val, weight=w_val)

    logger.info("Training final model with best params")
    model = xgb.train(
        full_params,
        dtrain,
        num_boost_round=config.MAX_BOOST_ROUNDS,
        evals=[(dtrain, "train"), (dval, "val")],
        early_stopping_rounds=config.EARLY_STOPPING_ROUNDS,
        verbose_eval=100,
    )

    best_iter = int(model.best_iteration)
    logger.info(f"Final model best iteration: {best_iter}")
    return model, best_iter


def compute_shap_importance(model: xgb.Booster, X_sample: pd.DataFrame
                            ) -> pd.DataFrame:
    """
    Compute SHAP values on a sample of rows and return mean absolute
    importance per feature.
    """
    logger.info(f"Computing SHAP values on {len(X_sample):,} sample rows")
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X_sample)

    mean_abs = np.abs(shap_values).mean(axis=0)
    df = pd.DataFrame({
        "feature": X_sample.columns.tolist(),
        "mean_abs_shap": mean_abs,
    }).sort_values("mean_abs_shap", ascending=False).reset_index(drop=True)

    logger.info("Top 20 features by SHAP importance:")
    for i, row in df.head(20).iterrows():
        logger.info(f"  {i + 1:2d}. {row['feature']}: {row['mean_abs_shap']:.6f}")

    return df


def prune_features(shap_importance: pd.DataFrame, top_n: int) -> List[str]:
    """Return the top-N features by SHAP importance."""
    top = shap_importance.head(top_n)["feature"].tolist()
    logger.info(f"Pruned to top {len(top)} features by SHAP importance")
    return top


def train_bootstrap_models(X_train, y_train, w_train,
                           X_val, y_val, w_val,
                           params: Dict, n_models: int = None
                           ) -> List[xgb.Booster]:
    """
    Train several models with different seeds / subsample seeds so we can
    estimate prediction uncertainty at inference time.
    """
    n_models = n_models or config.N_BOOTSTRAP_MODELS
    models = []

    for i in range(n_models):
        seed = config.RANDOM_SEED + i * 7
        full_params = {
            "objective": config.XGB_OBJECTIVE,
            "eval_metric": config.XGB_EVAL_METRIC,
            "seed": seed,
            "tree_method": "hist",
            **params,
        }
        # Bootstrap: resample training rows with replacement
        rng = np.random.RandomState(seed)
        idx = rng.randint(0, len(X_train), size=len(X_train))
        X_boot = X_train.iloc[idx]
        y_boot = y_train.iloc[idx]
        w_boot = w_train.iloc[idx]

        dtrain = xgb.DMatrix(X_boot, label=y_boot, weight=w_boot)
        dval = xgb.DMatrix(X_val, label=y_val, weight=w_val)

        model = xgb.train(
            full_params,
            dtrain,
            num_boost_round=config.MAX_BOOST_ROUNDS,
            evals=[(dval, "val")],
            early_stopping_rounds=config.EARLY_STOPPING_ROUNDS,
            verbose_eval=False,
        )
        models.append(model)
        logger.info(f"Bootstrap model {i + 1}/{n_models} trained "
                    f"(best_iter={model.best_iteration}, best_score={model.best_score:.6f})")

    return models


def save_artifacts(result: TrainingResult, bootstrap_models: List[xgb.Booster],
                   artifact_dir: Path = None) -> Path:
    """Save everything needed to re-load and use the model."""
    artifact_dir = artifact_dir or config.ARTIFACT_DIR
    timestamp = result.timestamp
    subdir = artifact_dir / f"model_1_run_{timestamp}"
    subdir.mkdir(parents=True, exist_ok=True)

    # Main model
    result.model.save_model(str(subdir / "model.json"))

    # Bootstrap models
    for i, m in enumerate(bootstrap_models):
        m.save_model(str(subdir / f"bootstrap_model_{i}.json"))

    # Feature list
    with open(subdir / "features.json", "w") as f:
        json.dump(result.feature_cols, f, indent=2)

    # Hyperparameters
    with open(subdir / "hyperparameters.json", "w") as f:
        json.dump(result.best_params, f, indent=2)

    # SHAP importance
    result.shap_importance.to_csv(subdir / "shap_importance.csv", index=False)

    # Metadata
    meta = {
        "timestamp": timestamp,
        "val_rmse": result.val_rmse,
        "n_boost_rounds": result.n_boost_rounds,
        "n_features": len(result.feature_cols),
        "n_bootstrap_models": len(bootstrap_models),
        "xgb_objective": config.XGB_OBJECTIVE,
        "train_seasons": config.TRAIN_SEASONS,
        "val_seasons": config.VAL_SEASONS,
        "test_seasons": config.TEST_SEASONS,
    }
    with open(subdir / "metadata.json", "w") as f:
        json.dump(meta, f, indent=2)

    logger.info(f"Saved artifacts to {subdir}")
    return subdir
