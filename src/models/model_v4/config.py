"""
Configuration for the MLB Strikeout Prediction Pipeline.

Edit these values to point at your SQL Server and tune training behaviour.
Nothing in this file talks to the database directly - it's just constants.
"""

from datetime import datetime

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
# Source feature view. We point at the handedness-enriched view rather than
# the raw matchup features table - it adds pitcher_throws, hitter_bats, and
# platoon flags as columns. To create that view, run sql/create_handedness_view.sql
# once. If you ever need to fall back to raw features, change this back to
# "dbo.fact_hitter_pitcher_matchup_model_featuresv2".
SOURCE_TABLE = "dbo.fact_hitter_pitcher_matchup_with_handedness"

# Output tables that this pipeline writes to.
PA_PREDICTIONS_TABLE = "fact_pa_strikeout_predictions"     # per plate-appearance
GAME_PREDICTIONS_TABLE = "fact_pitcher_game_strikeout_predictions"  # per pitcher-game
EV_TABLE = "fact_pitcher_strikeout_betting_ev"             # EV / odds table (manual entry)
MODEL_METRICS_TABLE = "fact_model_evaluation_metrics"      # model performance log

# ---------------------------------------------------------------------------
# Train / Validation / Test split (time-based - NO leakage)
# ---------------------------------------------------------------------------
# We use *seasons* to split. Anything before TRAIN_END is training data,
# VALIDATION season tunes / calibrates, TEST season is held-out evaluation.
TRAIN_SEASONS = [2023, 2024]
VALIDATION_SEASON = 2025
TEST_SEASON = 2026  # current season - real-world performance

# ---------------------------------------------------------------------------
# Modelling
# ---------------------------------------------------------------------------
TARGET_COL = "y_k_rate"           # per-PA strikeout indicator (0 or 1)
SAMPLE_WEIGHT_COL = "sample_weight"

# Columns we never feed to the model (identifiers, leakage, post-game info).
LEAKAGE_COLS = [
    "gamePk", "game_date", "season",
    "hitter_id", "hitter_name", "hitter_team_id", "hitter_team_name",
    "pitcher_id", "pitcher_name", "pitcher_team_id", "pitcher_team_name",
    # The following four are post-event (the very thing we are predicting):
    "hitter_plate_appearances", "hitter_strikeouts", "hitter_walks",
    "hitter_hits", "hitter_home_runs",
    "y_k_rate", "sample_weight",
]

CATEGORICAL_COLS = [
    "hitter_position",
    "hitter_lineup_position_name",
    # Handedness features - added via the handedness-enriched view.
    # These give the trees direct access to platoon information rather than
    # forcing them to infer it from whiff-vs-rhp/lhp split stats.
    "hitter_bats",        # 'L', 'R', or 'S' (switch hitter)
    "pitcher_throws",     # 'L' or 'R'
    "platoon_matchup",    # 'Same', 'Opposite', or 'Switch'
]

# XGBoost params - tuned for tabular, slightly imbalanced binary classification
XGB_PARAMS = {
    "objective": "binary:logistic",
    "eval_metric": "logloss",
    "learning_rate": 0.05,
    "max_depth": 6,
    "min_child_weight": 5,
    "subsample": 0.85,
    "colsample_bytree": 0.85,
    "reg_alpha": 0.1,
    "reg_lambda": 1.0,
    "tree_method": "hist",
    "n_jobs": -1,
    "random_state": 42,
}
XGB_NUM_ROUNDS = 2000
XGB_EARLY_STOP = 100

# LightGBM params
LGBM_PARAMS = {
    "objective": "binary",
    "metric": "binary_logloss",
    "learning_rate": 0.05,
    "num_leaves": 63,
    "max_depth": -1,
    "min_data_in_leaf": 50,
    "feature_fraction": 0.85,
    "bagging_fraction": 0.85,
    "bagging_freq": 5,
    "lambda_l1": 0.1,
    "lambda_l2": 1.0,
    "verbose": -1,
    "n_jobs": -1,
    "seed": 42,
}
LGBM_NUM_ROUNDS = 2000
LGBM_EARLY_STOP = 100

# Final ensemble weights (must sum to 1.0)
ENSEMBLE_WEIGHTS = {
    "xgb": 0.5,
    "lgbm": 0.4,
    "logreg": 0.1,
}

# ---------------------------------------------------------------------------
# Run metadata
# ---------------------------------------------------------------------------
RUN_TIMESTAMP = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
MODEL_VERSION = "v1.0.0"
