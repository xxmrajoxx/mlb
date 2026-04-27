"""
Central configuration for Model 1 training pipeline.

Change values here rather than in the individual modules.
"""
from pathlib import Path

# =======================================================================
# PATHS
# =======================================================================
PROJECT_ROOT = Path(__file__).parent.parent
ARTIFACT_DIR = PROJECT_ROOT / "artifacts"
CACHE_DIR = PROJECT_ROOT / "cache"
PLOT_DIR = PROJECT_ROOT / "plots"

for d in (ARTIFACT_DIR, CACHE_DIR, PLOT_DIR):
    d.mkdir(parents=True, exist_ok=True)

# =======================================================================
# DATA
# =======================================================================
SOURCE_TABLE = "mlb.dbo.fact_hitter_pitcher_matchup_model_featuresv2"
PREDICTION_TABLE = "mlb.dbo.fact_model_1_k_probability_predictions"

# Only rows where the hitter had at least this many PAs vs the pitcher.
# Matchups with 0 PAs are scoring/inference-only, not training.
MIN_PAS_FOR_TRAINING = 1

# Seasons in the data source
TRAIN_SEASONS = [2023]
VAL_SEASONS = [2024]
TEST_SEASONS = [2025]
INFERENCE_SEASONS = [2026]   # current / partial season
ALL_SEASONS = TRAIN_SEASONS + VAL_SEASONS + TEST_SEASONS + INFERENCE_SEASONS

# =======================================================================
# TARGET AND WEIGHTING
# =======================================================================
TARGET_COL = "y_k_rate"              # strikeouts / plate_appearances
WEIGHT_COL = "sample_weight"          # = plate_appearances
LABEL_COUNT_COL = "hitter_strikeouts" # for count-based evaluation
LABEL_PA_COL = "hitter_plate_appearances"

# =======================================================================
# FEATURES
# =======================================================================
# Columns that should NEVER be treated as features (identifiers, labels)
ID_COLS = [
    "gamePk", "game_date", "season",
    "hitter_id", "hitter_name", "hitter_position",
    "hitter_team_id", "hitter_team_name",
    "pitcher_id", "pitcher_name",
    "pitcher_team_id", "pitcher_team_name",
    "hitter_lineup_position", "hitter_lineup_position_name",
]

LABEL_COLS = [
    "hitter_strikeouts", "hitter_plate_appearances",
    "hitter_walks", "hitter_hits", "hitter_home_runs",
    "first_inning_faced",
    "y_k_rate", "sample_weight",
]

# Columns that could leak same-game info — belt-and-suspenders check
# (Should already be absent from v2 table, but this is defense-in-depth)
SUSPECTED_LEAKAGE_COLS = [
    "matchup_whiff_rate", "matchup_csw_rate", "matchup_called_strike_rate",
    "pitches_seen_vs_pitcher", "swings_vs_pitcher", "whiffs_vs_pitcher",
    "called_strikes_vs_pitcher",
    "hitter_pitcher_plate_appearances", "hitter_pitcher_hits",
    "hitter_pitcher_strikeouts", "hitter_pitcher_walks",
    "hitter_game_strikeouts", "hitter_game_plate_appearances",
    "pitcher_strikeOuts",
]

# =======================================================================
# MODEL
# =======================================================================
RANDOM_SEED = 42
N_OPTUNA_TRIALS = 100
EARLY_STOPPING_ROUNDS = 50
MAX_BOOST_ROUNDS = 2000
N_BOOTSTRAP_MODELS = 5   # for prediction uncertainty estimation

# XGBoost objective: reg:squarederror with sample weights works well here
# because we have a continuous [0,1] target weighted by PA counts.
# reg:logistic would force predictions into [0,1] cleanly but can be less
# flexible. Try both during experimentation.
XGB_OBJECTIVE = "reg:squarederror"
XGB_EVAL_METRIC = "rmse"

# Feature pruning: after initial training, keep top N by SHAP importance
TOP_N_FEATURES_AFTER_SHAP = 80

# =======================================================================
# LOGGING
# =======================================================================
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
