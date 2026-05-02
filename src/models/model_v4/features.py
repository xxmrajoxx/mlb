"""
Feature engineering for the strikeout model.

The source view already contains a *lot* of pre-computed rolling features,
so most of our work here is selecting the right columns, encoding
categoricals, and converting types - not creating new features.
"""

import logging
import numpy as np
import pandas as pd

import config

logger = logging.getLogger(__name__)


def select_feature_columns(df: pd.DataFrame) -> list[str]:
    """
    Return the list of columns to feed the model.

    Anything in LEAKAGE_COLS is excluded. Everything else that is numeric
    or one of our designated categorical columns is kept.
    """
    _exclude = set(config.LEAKAGE_COLS) | set(config.FEATURES_TO_DROP)
    candidate_cols = [c for c in df.columns if c not in _exclude]

    feature_cols = []
    for col in candidate_cols:
        if col in config.CATEGORICAL_COLS:
            feature_cols.append(col)
        elif pd.api.types.is_numeric_dtype(df[col]):
            feature_cols.append(col)
        else:
            # Try to coerce - if it works, it's a feature.
            try:
                pd.to_numeric(df[col], errors="raise")
                feature_cols.append(col)
            except (ValueError, TypeError):
                logger.debug(f"Skipping non-numeric column: {col}")

    logger.info(f"Selected {len(feature_cols)} feature columns")
    return feature_cols


def prepare_features(
    df: pd.DataFrame,
    feature_cols: list[str],
    category_maps: dict | None = None,
) -> tuple[pd.DataFrame, dict]:
    """
    Convert dtypes, encode categoricals, return model-ready feature matrix.

    Parameters
    ----------
    df : raw frame
    feature_cols : columns selected by select_feature_columns
    category_maps : reuse encodings learned on training set when transforming
                    val / test (so unseen categories map to -1 consistently).

    Returns
    -------
    X : feature matrix (DataFrame, all numeric)
    category_maps : the encodings (pass these back in for val/test)
    """
    X = df[feature_cols].copy()
    category_maps = dict(category_maps) if category_maps else {}

    for col in config.CATEGORICAL_COLS:
        if col not in X.columns:
            continue

        if col not in category_maps:
            # Learn encoding from this frame (training).
            uniques = X[col].dropna().unique()
            category_maps[col] = {v: i for i, v in enumerate(uniques)}

        mapping = category_maps[col]
        # Unknown categories -> -1, NaN -> -1
        X[col] = X[col].map(mapping).fillna(-1).astype(int)

    # Force everything else to numeric. Anything that fails goes to NaN -
    # the gradient boosters handle NaN fine, sklearn linear models do not
    # (we'll impute those when we get there).
    for col in X.columns:
        if col not in config.CATEGORICAL_COLS:
            X[col] = pd.to_numeric(X[col], errors="coerce")

    # Replace inf with NaN so the model treats them as missing.
    X = X.replace([np.inf, -np.inf], np.nan)

    return X, category_maps


def get_target_and_weights(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """
    Return per-PA K probability (continuous in [0,1]) and PA-count sample weights.

    The source view has one row per HITTER, where each hitter may have faced
    the pitcher multiple times. The target column `y_k_rate` is K count divided
    by PA count, i.e. the strikeout rate for that hitter against that pitcher
    in that game.

    To train the model correctly:
      - y is the per-PA K probability directly (a fraction in [0,1])
      - sample_weight is the number of PAs that row represents (so a 3-PA row
        contributes 3x as much to the loss as a 1-PA row)

    XGBoost and LightGBM both handle fractional targets when objective is
    binary:logistic - the loss becomes -[y*log(p) + (1-y)*log(1-p)] with y as
    a fraction, which is mathematically equivalent to having one Bernoulli per
    PA with a PA-weighted sum.

    For the logistic regression baseline (sklearn), we replicate this by passing
    y as the fractional rate and using PA counts as sample weights - sklearn's
    LogisticRegression supports fractional labels when used this way.
    """
    y = pd.to_numeric(df[config.TARGET_COL], errors="coerce").fillna(0)
    y = y.clip(lower=0.0, upper=1.0)

    if config.SAMPLE_WEIGHT_COL in df.columns:
        w = pd.to_numeric(df[config.SAMPLE_WEIGHT_COL], errors="coerce").fillna(1.0)
        w = w.clip(lower=0.1)  # avoid zero weights
    else:
        w = pd.Series(1.0, index=df.index)

    return y, w
