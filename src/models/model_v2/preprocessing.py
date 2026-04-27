"""
Preprocessing: feature selection, type handling, chronological splits,
leakage defense.
"""
import logging
from typing import List, Tuple

import numpy as np
import pandas as pd

from model_1_k_probability import config

logger = logging.getLogger(__name__)


def select_feature_columns(df: pd.DataFrame) -> List[str]:
    """
    Determine which columns are training features. Excludes IDs, labels,
    and any suspected-leakage columns.
    """
    exclude = set(config.ID_COLS) | set(config.LABEL_COLS) | set(config.SUSPECTED_LEAKAGE_COLS)

    feature_cols = []
    for col in df.columns:
        if col in exclude:
            continue
        # Only numeric types — categoricals need explicit handling
        if pd.api.types.is_numeric_dtype(df[col]):
            feature_cols.append(col)
        else:
            logger.debug(f"Dropping non-numeric column: {col} (dtype={df[col].dtype})")

    logger.info(f"Selected {len(feature_cols)} feature columns")

    # Log any suspected-leakage columns that were present (red flag)
    present_leakage = [c for c in config.SUSPECTED_LEAKAGE_COLS if c in df.columns]
    if present_leakage:
        logger.warning(
            f"LEAKAGE DEFENSE: dropped {len(present_leakage)} suspected leakage "
            f"columns that were present in source table: {present_leakage}"
        )

    return feature_cols


def filter_training_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only rows suitable for training:
    - At least MIN_PAS_FOR_TRAINING plate appearances in the matchup
    - Target value is present and sensible (0-1 inclusive)
    """
    before = len(df)

    df = df[df[config.LABEL_PA_COL] >= config.MIN_PAS_FOR_TRAINING].copy()
    df = df[df[config.TARGET_COL].notna()].copy()
    df = df[(df[config.TARGET_COL] >= 0) & (df[config.TARGET_COL] <= 1)].copy()

    after = len(df)
    logger.info(f"Filtered rows: {before:,} -> {after:,} (dropped {before - after:,})")
    return df


def chronological_split(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Split data chronologically into train/val/test/inference based on seasons
    defined in config. Never use random splits for time-series prediction.

    Returns
    -------
    train_df, val_df, test_df, inference_df
    """
    train_df = df[df["season"].isin(config.TRAIN_SEASONS)].copy()
    val_df = df[df["season"].isin(config.VAL_SEASONS)].copy()
    test_df = df[df["season"].isin(config.TEST_SEASONS)].copy()
    inference_df = df[df["season"].isin(config.INFERENCE_SEASONS)].copy()

    logger.info(f"Train rows: {len(train_df):,} (seasons {config.TRAIN_SEASONS})")
    logger.info(f"Val rows:   {len(val_df):,} (seasons {config.VAL_SEASONS})")
    logger.info(f"Test rows:  {len(test_df):,} (seasons {config.TEST_SEASONS})")
    logger.info(f"Inference rows: {len(inference_df):,} (seasons {config.INFERENCE_SEASONS})")

    # Within each split, sort by date for reproducibility
    for name, d in [("train", train_df), ("val", val_df), ("test", test_df), ("inference", inference_df)]:
        if len(d) > 0:
            d.sort_values(["game_date", "gamePk", "hitter_id", "pitcher_id"],
                          inplace=True, ignore_index=True)

    return train_df, val_df, test_df, inference_df


def handle_missing_values(
    df: pd.DataFrame, feature_cols: List[str]
) -> pd.DataFrame:
    """
    XGBoost handles NaN natively (it learns a default direction at each split).
    We keep NaN as-is for numeric features — NO imputation.

    This matters a lot: imputing a NaN rolling feature with 0 tells XGBoost
    "this player had a terrible last 10 games" rather than "we don't know".
    XGBoost's built-in NaN handling is smarter than imputation for this
    kind of data.
    """
    df = df.copy()

    missing_pct = df[feature_cols].isna().mean().sort_values(ascending=False)
    high_missing = missing_pct[missing_pct > 0.5]
    if len(high_missing) > 0:
        logger.warning(
            f"{len(high_missing)} features have >50% missing values. "
            f"Top: {high_missing.head(10).to_dict()}"
        )

    return df


def get_xgb_inputs(df: pd.DataFrame, feature_cols: List[str]):
    """Prepare the inputs XGBoost expects."""
    X = df[feature_cols].astype(np.float32)
    y = df[config.TARGET_COL].astype(np.float32)
    w = df[config.WEIGHT_COL].astype(np.float32)
    return X, y, w
