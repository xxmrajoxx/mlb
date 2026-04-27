"""
Train the per-PA strikeout classifiers.

Three base models -> isotonic calibration -> weighted ensemble.

We deliberately keep the base model choices simple and well-known. The tricky
part for betting EV isn't getting AUC up by 0.005 - it's getting calibrated
probabilities. That's why isotonic regression sits at the end of every model.
"""

import logging
import numpy as np
import pandas as pd
import xgboost as xgb
import lightgbm as lgb
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.isotonic import IsotonicRegression

import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Base learners
# ---------------------------------------------------------------------------
def train_xgb(X_train, y_train, w_train, X_val, y_val, w_val):
    """Train XGBoost with early stopping on the validation set."""
    logger.info("Training XGBoost...")
    dtrain = xgb.DMatrix(X_train, label=y_train, weight=w_train)
    dval = xgb.DMatrix(X_val, label=y_val, weight=w_val)

    booster = xgb.train(
        params=config.XGB_PARAMS,
        dtrain=dtrain,
        num_boost_round=config.XGB_NUM_ROUNDS,
        evals=[(dtrain, "train"), (dval, "val")],
        early_stopping_rounds=config.XGB_EARLY_STOP,
        verbose_eval=100,
    )
    logger.info(f"XGB best iteration: {booster.best_iteration}")
    return booster


def predict_xgb(booster, X):
    return booster.predict(xgb.DMatrix(X), iteration_range=(0, booster.best_iteration + 1))


def train_lgbm(X_train, y_train, w_train, X_val, y_val, w_val):
    """Train LightGBM with early stopping."""
    logger.info("Training LightGBM...")
    train_set = lgb.Dataset(X_train, label=y_train, weight=w_train)
    val_set = lgb.Dataset(X_val, label=y_val, weight=w_val, reference=train_set)

    booster = lgb.train(
        params=config.LGBM_PARAMS,
        train_set=train_set,
        num_boost_round=config.LGBM_NUM_ROUNDS,
        valid_sets=[train_set, val_set],
        valid_names=["train", "val"],
        callbacks=[
            lgb.early_stopping(config.LGBM_EARLY_STOP),
            lgb.log_evaluation(100),
        ],
    )
    logger.info(f"LGBM best iteration: {booster.best_iteration}")
    return booster


def predict_lgbm(booster, X):
    return booster.predict(X, num_iteration=booster.best_iteration)


def _expand_to_binary(X, y, w):
    """
    Expand fractional-target rows into binary rows for sklearn estimators
    that require binary y.

    For a row with y=0.333 (1 K out of 3 PAs) and weight 3, we produce:
      - 1 copy of (X, 1) with weight 1
      - 2 copies of (X, 0) with weight 1

    This is mathematically equivalent to training on the original Bernoulli
    sequence and is what sklearn's LogisticRegression actually wants to see.
    """
    y = np.asarray(y, dtype=float)
    w = np.asarray(w, dtype=float)

    # Number of K outcomes for each row, rounded to nearest integer
    n_pa = np.maximum(np.round(w).astype(int), 1)
    n_k = np.minimum(np.round(y * n_pa).astype(int), n_pa)
    n_no_k = n_pa - n_k

    # Build expanded arrays
    repeats = n_pa
    X_exp = np.repeat(X.values if hasattr(X, "values") else X, repeats, axis=0)

    y_exp = np.zeros(int(n_pa.sum()), dtype=int)
    cursor = 0
    for k_count, total in zip(n_k, n_pa):
        # First k_count entries are 1, rest are 0
        y_exp[cursor:cursor + k_count] = 1
        cursor += total

    return X_exp, y_exp


def train_logreg(X_train, y_train, w_train):
    """Logistic Regression baseline.

    Needs imputation + scaling, neither of which the trees need. We bundle
    those preprocessors with the model so prediction is one call.

    Because y can be fractional (per-hitter K rate), we expand each row into
    its binary-equivalent set of rows before fitting. sklearn's
    LogisticRegression requires binary labels.
    """
    logger.info("Training Logistic Regression baseline...")
    imputer = SimpleImputer(strategy="median")
    scaler = StandardScaler()

    X_imp = imputer.fit_transform(X_train)
    X_scl = scaler.fit_transform(X_imp)

    # Expand fractional targets into per-PA binary rows
    X_exp, y_exp = _expand_to_binary(pd.DataFrame(X_scl), y_train, w_train)

    logger.info(f"  Logreg fitting on expanded shape: {X_exp.shape}")

    model = LogisticRegression(
        max_iter=2000,
        C=1.0,
        solver="lbfgs",
        random_state=42,
    )
    model.fit(X_exp, y_exp)

    return {"imputer": imputer, "scaler": scaler, "model": model}


def predict_logreg(bundle, X):
    X_imp = bundle["imputer"].transform(X)
    X_scl = bundle["scaler"].transform(X_imp)
    return bundle["model"].predict_proba(X_scl)[:, 1]


# ---------------------------------------------------------------------------
# Calibration
# ---------------------------------------------------------------------------
def fit_isotonic(probs, y_true, sample_weight=None):
    """
    Isotonic calibration: maps raw model output -> well-calibrated probability.

    Critical for betting - if the model says 30% but actually only happens
    25% of the time, every bet you place using the raw output is mispriced.

    Supports fractional y_true (per-hitter K rate) via sample_weight.
    """
    cal = IsotonicRegression(out_of_bounds="clip")
    cal.fit(probs, y_true, sample_weight=sample_weight)
    return cal


def apply_isotonic(cal, probs):
    return cal.predict(probs)


# ---------------------------------------------------------------------------
# Ensemble
# ---------------------------------------------------------------------------
def ensemble_probs(p_xgb, p_lgbm, p_logreg):
    w = config.ENSEMBLE_WEIGHTS
    return (
        w["xgb"] * p_xgb
        + w["lgbm"] * p_lgbm
        + w["logreg"] * p_logreg
    )
