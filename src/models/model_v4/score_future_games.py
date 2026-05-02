"""
Score future MLB games using the trained model artifacts.

Workflow:
    1. Load trained models from ./artifacts/
    2. Pull synthetic future-game rows from fact_future_matchups view
    3. Apply the same feature pipeline used during training
    4. Predict P(K) per synthetic PA row using XGB / LGBM / LogReg
    5. Apply isotonic calibration
    6. Ensemble
    7. Aggregate to game level via Poisson-binomial sum
    8. Write predictions into:
         - fact_pitcher_game_strikeout_predictions (split_set='future')
         - fact_pitcher_strikeout_betting_ev (rows ready for odds entry)

Usage:
    python score_future_games.py

Prerequisites:
    - Run pipeline.py at least once to generate trained model artifacts
    - Run fetch_probable_pitchers.py to populate dim_probable_pitchers
    - Create fact_future_matchups view via sql/create_future_matchups_view.sql
"""
from __future__ import annotations
import logging
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb
import lightgbm as lgb
from sqlalchemy import text

# Local module imports - same pattern as pipeline.py
sys.path.insert(0, ".")
import config
import features as features_module
import models as models_module
import game_aggregation
from sql_loader import get_engine, load_dataframe
from pipeline import ensure_output_tables, load_pitcher_season_games

logger = logging.getLogger("score_future")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


# ============================================================================
# Loading trained artifacts
# ============================================================================

def load_artifacts(artifact_dir: Path = Path("./artifacts")):
    """Load the four pieces saved by pipeline.py."""
    logger.info(f"Loading model artifacts from {artifact_dir}")

    if not artifact_dir.exists():
        raise FileNotFoundError(
            f"Artifact directory {artifact_dir} not found. "
            f"Run pipeline.py first to train and save the models."
        )

    # XGBoost
    xgb_model = xgb.Booster()
    xgb_model.load_model(str(artifact_dir / "xgb.json"))

    # LightGBM
    lgbm_model = lgb.Booster(model_file=str(artifact_dir / "lgbm.txt"))

    # Logistic Regression bundle (imputer + scaler + model)
    with open(artifact_dir / "logreg.pkl", "rb") as f:
        logreg_bundle = pickle.load(f)

    # Calibrators (one per base model + one for ensemble)
    with open(artifact_dir / "calibrators.pkl", "rb") as f:
        calibrators = pickle.load(f)

    # Feature column list AND categorical maps (frozen at training time)
    with open(artifact_dir / "feature_cols.pkl", "rb") as f:
        feature_bundle = pickle.load(f)

    feature_cols = feature_bundle["feature_cols"]
    cat_maps = feature_bundle["cat_maps"]

    logger.info(f"Loaded models. Expected feature columns: {len(feature_cols)}")
    return {
        "xgb": xgb_model,
        "lgbm": lgbm_model,
        "logreg": logreg_bundle,
        "calibrators": calibrators,
        "feature_cols": feature_cols,
        "cat_maps": cat_maps,
    }


# ============================================================================
# Loading future matchups
# ============================================================================

def load_future_matchups() -> pd.DataFrame:
    """Pull synthetic rows from fact_future_matchups view."""
    engine = get_engine()
    sql = "SELECT * FROM dbo.fact_future_matchups"
    logger.info(f"Loading future matchups: {sql}")
    df = pd.read_sql(sql, engine)
    logger.info(f"Loaded {len(df):,} synthetic matchup rows for {df['gamePk'].nunique()} games")
    return df


# ============================================================================
# Scoring
# ============================================================================

def score_matchups(future_df: pd.DataFrame, artifacts: dict) -> pd.DataFrame:
    """Apply the trained model pipeline to future matchup rows."""
    feature_cols = artifacts["feature_cols"]
    cat_maps = artifacts["cat_maps"]

    # Make sure every column the model expects is present (fill with NaN if missing).
    # Use a single pd.concat instead of repeated inserts to avoid pandas fragmentation warnings.
    missing = list(set(feature_cols) - set(future_df.columns))
    if missing:
        logger.warning(f"Future data missing {len(missing)} feature columns - filling with NaN. "
                       f"Sample missing columns: {missing[:5]}")
        nan_block = pd.DataFrame(np.nan, index=future_df.index, columns=missing)
        future_df = pd.concat([future_df, nan_block], axis=1).copy()

    # Apply the same preprocessing used during training. cat_maps preserves
    # the categorical encodings so 'L' / 'R' / 'S' map to the same integers
    # they did at training time.
    X_future, _ = features_module.prepare_features(future_df, feature_cols, cat_maps)
    X_future = X_future[feature_cols]  # ensure column order matches training

    logger.info(f"Feature matrix shape: {X_future.shape}")

    # Predictions from each base model
    p_xgb = artifacts["xgb"].predict(xgb.DMatrix(X_future))
    p_lgbm = artifacts["lgbm"].predict(X_future)

    lr = artifacts["logreg"]
    X_imp = lr["imputer"].transform(X_future)
    X_scl = lr["scaler"].transform(X_imp)
    p_lr = lr["model"].predict_proba(X_scl)[:, 1]

    # Apply calibration (use the same helper pipeline.py uses at training time)
    cals = artifacts["calibrators"]
    p_xgb_cal = models_module.apply_isotonic(cals["xgb"], p_xgb)
    p_lgbm_cal = models_module.apply_isotonic(cals["lgbm"], p_lgbm)
    p_lr_cal = models_module.apply_isotonic(cals["logreg"], p_lr)

    # Ensemble (uses config.ENSEMBLE_WEIGHTS internally)
    p_ens_raw = models_module.ensemble_probs(p_xgb_cal, p_lgbm_cal, p_lr_cal)

    # Final calibration on the ensemble
    p_ens = models_module.apply_isotonic(cals["ensemble"], p_ens_raw)

    # Attach predictions to the row data for aggregation
    out = future_df.copy()
    out["prob_xgb"] = p_xgb_cal
    out["prob_lgbm"] = p_lgbm_cal
    out["prob_logreg"] = p_lr_cal
    out["prob_strikeout"] = p_ens

    return out


# ============================================================================
# Aggregation and persistence
# ============================================================================

def aggregate_and_save(scored: pd.DataFrame):
    """Aggregate per-PA predictions to pitcher-game and write to SQL."""
    logger.info("Aggregating to pitcher-game level...")
    games = game_aggregation.aggregate_to_pitcher_game(scored)
    logger.info(f"Built {len(games):,} pitcher-game predictions")

    # Mark these as 'future' so you can distinguish from train/val/test rows
    games["split_set"] = "future"
    games["model_version"] = config.MODEL_VERSION
    games["scored_at"] = config.RUN_TIMESTAMP

    # Make sure the output tables exist before we try to DELETE/INSERT.
    # The pipeline.py training run is the usual creator, but when we re-run
    # scoring after dropping a table for migration, this saves us.
    ensure_output_tables()

    engine = get_engine()

    # Wipe any existing 'future' rows so re-runs don't duplicate
    with engine.begin() as conn:
        result = conn.execute(text(
            f"DELETE FROM dbo.{config.GAME_PREDICTIONS_TABLE} WHERE split_set = 'future'"
        ))
        logger.info(f"Cleared {result.rowcount} existing 'future' game predictions")

        # Also clear future EV rows that haven't been bet on yet
        result = conn.execute(text(f"""
            DELETE FROM dbo.{config.EV_TABLE}
            WHERE sportsbook IS NULL
              AND game_date >= CAST(GETDATE() AS DATE)
        """))
        logger.info(f"Cleared {result.rowcount} pristine future EV rows")

    # Write game-level predictions
    load_dataframe(games, config.GAME_PREDICTIONS_TABLE, if_exists="append")
    logger.info(f"Wrote {len(games)} game predictions")

    # Write EV template rows for the new future games
    ev_rows = games[[
        "gamePk", "game_date", "season",
        "pitcher_id", "pitcher_name", "pitcher_team_name",
        "pitcher_throws",
        "opponent_team_name",
        "opp_lhb_count", "opp_rhb_count", "opp_switch_count",
        "opp_same_side_count", "opp_opposite_side_count",
        "predicted_strikeouts", "predicted_k_stddev",
        "actual_strikeouts",
    ]].copy()

    # Historical game counts per season
    season_games = load_pitcher_season_games()
    ev_rows = ev_rows.merge(season_games, on="pitcher_id", how="left")
    for col in ["games_2023", "games_2024", "games_2025", "games_2026"]:
        ev_rows[col] = ev_rows[col].fillna(0).astype(int)

    for c in ["sportsbook", "line", "over_odds", "under_odds",
              "model_prob_over", "model_prob_under",
              "implied_prob_over", "implied_prob_under",
              "edge_over", "edge_under", "ev_over", "ev_under",
              "recommended_side", "kelly_fraction", "bet_result"]:
        ev_rows[c] = None

    ev_rows["model_version"] = config.MODEL_VERSION
    ev_rows["scored_at"] = config.RUN_TIMESTAMP

    load_dataframe(ev_rows, config.EV_TABLE, if_exists="append")
    logger.info(f"Wrote {len(ev_rows)} EV rows ready for odds entry")


# ============================================================================
# Main
# ============================================================================

def run():
    logger.info("=" * 70)
    logger.info("MLB Strikeout - Future Game Scoring")
    logger.info("=" * 70)

    artifacts = load_artifacts()
    future_df = load_future_matchups()

    if future_df.empty:
        logger.warning("No future matchups found. Did you run fetch_probable_pitchers.py? "
                       "Did you create the fact_future_matchups view?")
        return

    scored = score_matchups(future_df, artifacts)
    aggregate_and_save(scored)
    logger.info("Future game scoring complete.")


if __name__ == "__main__":
    run()
