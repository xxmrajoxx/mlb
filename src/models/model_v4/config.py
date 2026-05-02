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

# Columns to drop before training — redundant, near-zero importance, or exact duplicates
# identified by cross-model importance analysis (XGB gain + LGBM gain, normalised 0-100).
# All entries scored avg_norm < 13; nothing with real signal is removed.
# 334 raw features -> ~218 after these drops.
FEATURES_TO_DROP = [
    # --- Zero / near-zero importance in both models ---
    "is_first_matchup",         # superseded by h2h_career_pa == 0
    "h2h_sample_large",
    "h2h_sample_medium",
    "h2h_sample_small",         # h2h_career_pa is a better continuous proxy
    "h2h_career_hr",            # weakest h2h feature
    "h2h_career_games",         # h2h_career_pa already captures sample size
    "h2h_career_bb",
    "hitter_avg_hr_last_5",
    "hitter_pct_2plus_k_last_3",
    "pitcher_is_reliever",      # exact inverse of pitcher_is_starter (zero importance)
    "pitcher_avg_hr_last_5",

    # --- Exact duplicate (pitcher_gamesStarted encodes the same thing; LGBM ranks it #1) ---
    "pitcher_is_starter",

    # --- Redundant handedness flags (hitter_bats + pitcher_throws already in model) ---
    "is_same_side_matchup",
    "hitter_is_lefty",
    "pitcher_is_lefty",
    "hitter_is_switch",

    # --- Weak hitter basic stat families: keep 1 best window, drop the other 5 ---
    # batting_avg (keep avg_last_10)
    "hitter_avg_batting_avg_last_3",
    "hitter_avg_batting_avg_last_5",
    "hitter_weighted_batting_avg_last_3",
    "hitter_weighted_batting_avg_last_5",
    "hitter_weighted_batting_avg_last_10",
    # obp (keep avg_last_5)
    "hitter_avg_obp_last_3",
    "hitter_avg_obp_last_10",
    "hitter_weighted_obp_last_3",
    "hitter_weighted_obp_last_5",
    "hitter_weighted_obp_last_10",
    # slg (keep weighted_last_3)
    "hitter_avg_slg_last_3",
    "hitter_avg_slg_last_5",
    "hitter_avg_slg_last_10",
    "hitter_weighted_slg_last_5",
    "hitter_weighted_slg_last_10",
    # ops (keep weighted_last_5)
    "hitter_avg_ops_last_3",
    "hitter_avg_ops_last_5",
    "hitter_avg_ops_last_10",
    "hitter_weighted_ops_last_3",
    "hitter_weighted_ops_last_10",
    # tb_rate (keep weighted_last_5)
    "hitter_avg_tb_rate_last_3",
    "hitter_avg_tb_rate_last_5",
    "hitter_avg_tb_rate_last_10",
    "hitter_weighted_tb_rate_last_3",
    "hitter_weighted_tb_rate_last_10",
    # tb count — redundant with tb_rate
    "hitter_avg_tb_last_3",
    "hitter_avg_tb_last_5",
    # rbi — not predictive of strikeouts
    "hitter_avg_rbi_last_3",
    # walk_rate (keep weighted_last_10)
    "hitter_avg_walk_rate_last_3",
    "hitter_avg_walk_rate_last_5",
    "hitter_avg_walk_rate_last_10",
    "hitter_weighted_walk_rate_last_3",
    "hitter_weighted_walk_rate_last_5",
    # hit_rate (keep weighted_last_5)
    "hitter_avg_hit_rate_last_3",
    "hitter_avg_hit_rate_last_5",
    "hitter_avg_hit_rate_last_10",
    "hitter_weighted_hit_rate_last_3",
    "hitter_weighted_hit_rate_last_10",
    # hr_rate (keep weighted_last_10)
    "hitter_avg_hr_rate_last_3",
    "hitter_avg_hr_rate_last_5",
    "hitter_avg_hr_rate_last_10",
    "hitter_weighted_hr_rate_last_3",
    "hitter_weighted_hr_rate_last_5",

    # --- Count stat window reduction ---
    # bb count: keep last_10 per side
    "hitter_avg_bb_last_3",
    "hitter_avg_bb_last_5",
    "pitcher_avg_bb_last_3",
    "pitcher_avg_bb_last_5",
    # hits count: keep last_10 per side
    "hitter_avg_hits_last_3",
    "hitter_avg_hits_last_5",
    "pitcher_avg_hits_last_3",
    "pitcher_avg_hits_last_5",
    # hr count: keep pitcher_last_10 only; hitter hr_rate is better than hr count
    "pitcher_avg_hr_last_3",
    "hitter_avg_hr_last_3",
    "hitter_avg_hr_last_10",
    # k count: k9 / k_rate are better; last_10 and last_5 for pitcher keep the signal
    "hitter_avg_k_last_3",
    "hitter_avg_k_last_5",
    "pitcher_avg_k_last_3",
    # pa/ab: sum_ versions add nothing over avg_; trim to last_5 and last_10
    "hitter_sum_pa_last_3",
    "hitter_sum_pa_last_5",
    "hitter_sum_pa_last_10",
    "hitter_sum_ab_last_3",
    "hitter_sum_ab_last_5",
    "hitter_sum_ab_last_10",
    "hitter_avg_pa_last_3",
    "hitter_avg_pa_last_5",
    "hitter_avg_pa_last_10",
    "hitter_avg_ab_last_3",

    # --- prev_* features: mostly single-game noise ---
    # keep hitter_prev_contact_rate, hitter_prev_whiff_rate, pitcher_prev_* statcast
    "hitter_prev_pa",
    "hitter_prev_ab",
    "hitter_prev_hits",
    "hitter_prev_bb",
    "hitter_prev_ops",
    "hitter_prev_k",
    "hitter_prev_k_rate",
    "pitcher_prev_ip",
    "pitcher_prev_bf",

    # --- Pitcher putaway_rate: keep weighted_last_5 only ---
    "pitcher_avg_putaway_rate_last_3",
    "pitcher_avg_putaway_rate_last_5",
    "pitcher_avg_putaway_rate_last_10",
    "pitcher_weighted_putaway_rate_last_3",
    "pitcher_weighted_putaway_rate_last_10",

    # --- Pitcher csw_rate: keep avg_last_10 and weighted_last_5 ---
    "pitcher_avg_csw_rate_last_3",
    "pitcher_avg_csw_rate_last_5",
    "pitcher_weighted_csw_rate_last_10",

    # --- Pitcher kbb: keep avg_last_5 and avg_last_10 ---
    "pitcher_avg_kbb_last_3",
    "pitcher_weighted_kbb_last_3",
    "pitcher_weighted_kbb_last_5",
    "pitcher_weighted_kbb_last_10",

    # --- Pitcher k9: last_3 window weakest; last_5 and last_10 sufficient ---
    "pitcher_avg_k9_last_3",
    "pitcher_weighted_k9_last_3",

    # --- Whiff vs handedness: keep last_10 + last_5 for hitter; last_10 for pitcher ---
    "hitter_avg_whiff_rate_vs_lhp_last_3",
    "hitter_weighted_whiff_rate_vs_lhp_last_3",
    "hitter_avg_whiff_rate_vs_rhp_last_3",
    "hitter_weighted_whiff_rate_vs_rhp_last_5",
    "pitcher_weighted_whiff_rate_vs_rhb_last_3",
    "pitcher_weighted_whiff_rate_vs_rhb_last_5",
    "pitcher_weighted_whiff_rate_vs_lhb_last_3",
    "pitcher_weighted_whiff_rate_vs_lhb_last_5",

    # --- Two-strike whiff: keep avg_last_5 and weighted_last_10 ---
    "hitter_avg_two_strike_whiff_rate_last_3",
    "hitter_weighted_two_strike_whiff_rate_last_3",
    "hitter_weighted_two_strike_whiff_rate_last_5",
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

# XGBoost params
# Key changes vs prior version:
#   - learning_rate 0.05 -> 0.02: prior run stopped at 73-87 rounds (way too early);
#     slower LR forces gradual learning and expects convergence ~250-400 rounds instead
#   - max_depth 6 -> 5: shallower trees reduce fast overfitting on noisy per-PA data
#   - min_child_weight 5 -> 20: with PA-count sample weights the minimum hessian sum
#     per leaf needs to be higher to get stable leaf values
#   - colsample_bytree 0.85 -> 0.70: more feature diversity per tree
#   - gamma 0.05 (new): minimum gain required for a split; prunes low-value nodes
#   - NUM_ROUNDS 2000 -> 5000: ceiling raised; early stopping still controls actual stop
XGB_PARAMS = {
    "objective": "binary:logistic",
    "eval_metric": "logloss",
    "learning_rate": 0.02,
    "max_depth": 5,
    "min_child_weight": 20,
    "gamma": 0.05,
    "subsample": 0.85,
    "colsample_bytree": 0.70,
    "reg_alpha": 0.1,
    "reg_lambda": 1.0,
    "tree_method": "hist",
    "n_jobs": -1,
    "random_state": 42,
}
XGB_NUM_ROUNDS = 5000
XGB_EARLY_STOP = 100

# LightGBM params
# Key changes vs prior version:
#   - learning_rate 0.05 -> 0.02: same reason as XGB
#   - num_leaves 63 -> 31: this was the main LGBM problem; leaf-wise growth with 63
#     leaves + only 55 rounds meant very complex, high-variance trees that lost to
#     logistic regression — 31 leaves is more appropriate for this dataset
#   - min_data_in_leaf 50 -> 80: pairs with the reduced leaf count
#   - min_sum_hessian_in_leaf 20.0 (new): critical with PA-count weights — controls
#     minimum weighted coverage per leaf, equivalent to min_child_weight in XGB
#   - feature_fraction 0.85 -> 0.70: matches XGB for consistent diversity
#   - bagging_fraction 0.85 -> 0.75: slightly more aggressive row subsampling
#   - NUM_ROUNDS 2000 -> 5000: ceiling raised
LGBM_PARAMS = {
    "objective": "binary",
    "metric": "binary_logloss",
    "learning_rate": 0.02,
    "num_leaves": 31,
    "max_depth": -1,
    "min_data_in_leaf": 80,
    "min_sum_hessian_in_leaf": 20.0,
    "feature_fraction": 0.70,
    "bagging_fraction": 0.75,
    "bagging_freq": 5,
    "lambda_l1": 0.1,
    "lambda_l2": 1.0,
    "verbose": -1,
    "n_jobs": -1,
    "seed": 42,
}
LGBM_NUM_ROUNDS = 5000
LGBM_EARLY_STOP = 100

# Ensemble weights — adjusted because LGBM was scoring below logistic regression
# (val AUC 0.5456 vs logreg 0.5895). Rebalanced toward XGB and logreg until LGBM
# is retrained with the corrected params above.
ENSEMBLE_WEIGHTS = {
    "xgb": 0.55,
    "lgbm": 0.25,
    "logreg": 0.20,
}

# ---------------------------------------------------------------------------
# Run metadata
# ---------------------------------------------------------------------------
RUN_TIMESTAMP = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
MODEL_VERSION = "v1.0.0"
