"""
End-to-end MLB strikeout prediction pipeline.

Run order:
    python pipeline.py

What it does:
  1. Loads matchup data from SQL.
  2. Splits by season (train: 2023-24, val: 2025, test: 2026).
  3. Trains XGB + LightGBM + LogReg on per-PA P(strikeout).
  4. Calibrates each base model with isotonic regression on the val set.
  5. Builds a weighted ensemble.
  6. Evaluates on val and test.
  7. Aggregates per-PA predictions into pitcher-game totals.
  8. Writes results to SQL Server (PA, game, metrics, and EV-template tables).
"""

import logging
import os
import pickle
import numpy as np
import pandas as pd

import config
import data_loader
import features
import models
import evaluation
import game_aggregation
from sql_loader import load_dataframe, execute_sql, get_engine
from sqlalchemy import text

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Output schemas (created if missing - keeps SQL Server happy with pandas dtypes)
# ---------------------------------------------------------------------------
def ensure_output_tables():
    """Create the output tables if they don't already exist."""
    engine = get_engine()

    pa_table_sql = f"""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{config.PA_PREDICTIONS_TABLE}')
    BEGIN
        CREATE TABLE dbo.{config.PA_PREDICTIONS_TABLE} (
            gamePk            BIGINT,
            game_date         DATE,
            season            INT,
            split_set         VARCHAR(20),
            hitter_id         INT,
            hitter_name       VARCHAR(100),
            hitter_position   VARCHAR(10),
            hitter_team_id    INT,
            hitter_team_name  VARCHAR(100),
            hitter_lineup_position VARCHAR(20),
            pitcher_id        INT,
            pitcher_name      VARCHAR(100),
            pitcher_team_id   INT,
            pitcher_team_name VARCHAR(100),
            pitcher_is_starter BIT,
            hitter_bats       VARCHAR(2),     -- 'L', 'R', or 'S'
            pitcher_throws    VARCHAR(2),     -- 'L' or 'R'
            platoon_matchup   VARCHAR(10),    -- 'Same', 'Opposite', or 'Switch'
            prob_xgb          FLOAT,
            prob_lgbm         FLOAT,
            prob_logreg       FLOAT,
            prob_strikeout    FLOAT,        -- calibrated ensemble
            actual_strikeout  FLOAT,
            model_version     VARCHAR(20),
            scored_at         DATETIME
        )
    END
    """

    game_table_sql = f"""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{config.GAME_PREDICTIONS_TABLE}')
    BEGIN
        CREATE TABLE dbo.{config.GAME_PREDICTIONS_TABLE} (
            gamePk             BIGINT,
            game_date          DATE,
            season             INT,
            split_set          VARCHAR(20),
            pitcher_id         INT,
            pitcher_name       VARCHAR(100),
            pitcher_team_id    INT,
            pitcher_team_name  VARCHAR(100),
            pitcher_throws     VARCHAR(2),     -- 'L' or 'R'
            opponent_team_id   INT,
            opponent_team_name VARCHAR(100),
            batters_faced_modeled INT,
            -- Lineup composition aggregates (counts of L / R / S hitters faced)
            opp_lhb_count       INT,            -- left-handed batters in lineup
            opp_rhb_count       INT,            -- right-handed batters
            opp_switch_count    INT,            -- switch hitters
            opp_same_side_count INT,            -- batters with same handedness as pitcher
            opp_opposite_side_count INT,        -- batters with opposite handedness
            predicted_strikeouts  FLOAT,
            predicted_k_stddev    FLOAT,
            most_likely_k         INT,
            most_likely_k_prob    FLOAT,
            prob_over_3_5  FLOAT,
            prob_over_4_5  FLOAT,
            prob_over_5_5  FLOAT,
            prob_over_6_5  FLOAT,
            prob_over_7_5  FLOAT,
            prob_over_8_5  FLOAT,
            prob_over_9_5  FLOAT,
            actual_strikeouts INT,
            model_version  VARCHAR(20),
            scored_at      DATETIME
        )
    END
    """

    ev_table_sql = f"""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{config.EV_TABLE}')
    BEGIN
        CREATE TABLE dbo.{config.EV_TABLE} (
            -- Identifying info
            gamePk            BIGINT,
            game_date         DATE,
            season            INT,
            pitcher_id        INT,
            pitcher_name      VARCHAR(100),
            pitcher_team_name VARCHAR(100),
            pitcher_throws    VARCHAR(2),      -- 'L' or 'R'
            opponent_team_name VARCHAR(100),

            -- Lineup composition (handedness counts)
            opp_lhb_count           INT,
            opp_rhb_count           INT,
            opp_switch_count        INT,
            opp_same_side_count     INT,
            opp_opposite_side_count INT,

            -- Model output
            predicted_strikeouts FLOAT,
            predicted_k_stddev   FLOAT,

            -- Sportsbook line you fill in manually
            sportsbook       VARCHAR(50),
            line             FLOAT,            -- e.g. 5.5
            over_odds        DECIMAL(6,2),     -- e.g. 1.90 (decimal odds, AU style)
            under_odds       DECIMAL(6,2),     -- e.g. 1.90

            -- Computed columns (the pipeline fills in based on model probs)
            model_prob_over   FLOAT,
            model_prob_under  FLOAT,
            implied_prob_over FLOAT,
            implied_prob_under FLOAT,
            edge_over         FLOAT,           -- model_prob_over - implied_prob_over
            edge_under        FLOAT,
            ev_over           FLOAT,           -- expected return on $1 OVER bet
            ev_under          FLOAT,           -- expected return on $1 UNDER bet
            recommended_side  VARCHAR(10),     -- 'OVER' / 'UNDER' / 'PASS'
            kelly_fraction    FLOAT,           -- recommended bet fraction of bankroll

            actual_strikeouts INT,
            bet_result        VARCHAR(10),     -- WIN / LOSS / PUSH / NULL
            model_version     VARCHAR(20),
            scored_at         DATETIME
        )
    END
    """

    metrics_table_sql = f"""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{config.MODEL_METRICS_TABLE}')
    BEGIN
        CREATE TABLE dbo.{config.MODEL_METRICS_TABLE} (
            run_timestamp DATETIME,
            model_version VARCHAR(20),
            split_set     VARCHAR(20),
            metric_type   VARCHAR(20),
            label         VARCHAR(50),
            n             BIGINT,
            accuracy      FLOAT,
            roc_auc       FLOAT,
            log_loss_val  FLOAT,
            brier         FLOAT,
            precision_val FLOAT,
            recall        FLOAT,
            f1            FLOAT,
            mae           FLOAT,
            rmse          FLOAT,
            bias          FLOAT,
            base_rate     FLOAT,
            pred_rate     FLOAT
        )
    END
    """

    # Add new columns to the EV table if they don't exist yet
    # (handles tables created before these features were added).
    ev_history_cols_sql = """
        IF NOT EXISTS (SELECT 1 FROM sys.columns
                       WHERE object_id = OBJECT_ID('dbo.{ev}') AND name = 'games_2023')
            ALTER TABLE dbo.{ev} ADD games_2023 INT;
        IF NOT EXISTS (SELECT 1 FROM sys.columns
                       WHERE object_id = OBJECT_ID('dbo.{ev}') AND name = 'games_2024')
            ALTER TABLE dbo.{ev} ADD games_2024 INT;
        IF NOT EXISTS (SELECT 1 FROM sys.columns
                       WHERE object_id = OBJECT_ID('dbo.{ev}') AND name = 'games_2025')
            ALTER TABLE dbo.{ev} ADD games_2025 INT;
        IF NOT EXISTS (SELECT 1 FROM sys.columns
                       WHERE object_id = OBJECT_ID('dbo.{ev}') AND name = 'games_2026')
            ALTER TABLE dbo.{ev} ADD games_2026 INT;
        IF NOT EXISTS (SELECT 1 FROM sys.columns
                       WHERE object_id = OBJECT_ID('dbo.{ev}') AND name = 'weighted_k_per_bf_last_3')
            ALTER TABLE dbo.{ev} ADD weighted_k_per_bf_last_3 FLOAT;
        IF NOT EXISTS (SELECT 1 FROM sys.columns
                       WHERE object_id = OBJECT_ID('dbo.{ev}') AND name = 'weighted_k_per_bf_last_5')
            ALTER TABLE dbo.{ev} ADD weighted_k_per_bf_last_5 FLOAT;
        IF NOT EXISTS (SELECT 1 FROM sys.columns
                       WHERE object_id = OBJECT_ID('dbo.{ev}') AND name = 'weighted_k_per_bf_last_10')
            ALTER TABLE dbo.{ev} ADD weighted_k_per_bf_last_10 FLOAT;
        IF NOT EXISTS (SELECT 1 FROM sys.columns
                       WHERE object_id = OBJECT_ID('dbo.{ev}') AND name = 'avg_strike_pct_last_3')
            ALTER TABLE dbo.{ev} ADD avg_strike_pct_last_3 FLOAT;
        IF NOT EXISTS (SELECT 1 FROM sys.columns
                       WHERE object_id = OBJECT_ID('dbo.{ev}') AND name = 'avg_strike_pct_last_5')
            ALTER TABLE dbo.{ev} ADD avg_strike_pct_last_5 FLOAT;
        IF NOT EXISTS (SELECT 1 FROM sys.columns
                       WHERE object_id = OBJECT_ID('dbo.{ev}') AND name = 'avg_strike_pct_last_10')
            ALTER TABLE dbo.{ev} ADD avg_strike_pct_last_10 FLOAT;
        IF NOT EXISTS (SELECT 1 FROM sys.columns
                       WHERE object_id = OBJECT_ID('dbo.{ev}') AND name = 'avg_pitches_per_inning_last_3')
            ALTER TABLE dbo.{ev} ADD avg_pitches_per_inning_last_3 FLOAT;
        IF NOT EXISTS (SELECT 1 FROM sys.columns
                       WHERE object_id = OBJECT_ID('dbo.{ev}') AND name = 'avg_pitches_per_inning_last_5')
            ALTER TABLE dbo.{ev} ADD avg_pitches_per_inning_last_5 FLOAT;
        IF NOT EXISTS (SELECT 1 FROM sys.columns
                       WHERE object_id = OBJECT_ID('dbo.{ev}') AND name = 'avg_pitches_per_inning_last_10')
            ALTER TABLE dbo.{ev} ADD avg_pitches_per_inning_last_10 FLOAT;
    """.format(ev=config.EV_TABLE)

    with engine.begin() as conn:
        for sql in [pa_table_sql, game_table_sql, ev_table_sql, metrics_table_sql]:
            conn.execute(text(sql))
        conn.execute(text(ev_history_cols_sql))
    logger.info("Output tables ready")


def clear_existing_predictions():
    """
    Wipe predictions from the prediction tables for the splits we are about to
    re-score. Without this, every pipeline run appends duplicate rows on top of
    the previous run's rows.

    We DO NOT clear:
      - fact_model_evaluation_metrics (we want the historical run log)
      - any rows in the EV table where you've already entered odds (sportsbook
        IS NOT NULL) - that's manual work and shouldn't be wiped

    We DO clear:
      - all rows in fact_pa_strikeout_predictions for splits validation/test
      - all rows in fact_pitcher_game_strikeout_predictions for splits validation/test
      - rows in fact_pitcher_strikeout_betting_ev that are still pristine
        (no odds entered yet) - those will be re-created with current predictions
    """
    engine = get_engine()
    splits = "('validation', 'test')"

    cleanup_sqls = [
        f"DELETE FROM dbo.{config.PA_PREDICTIONS_TABLE} WHERE split_set IN {splits}",
        f"DELETE FROM dbo.{config.GAME_PREDICTIONS_TABLE} WHERE split_set IN {splits}",
        # Only delete EV rows that haven't had odds entered yet - keep your
        # historical bets intact even if predictions are refreshed.
        f"DELETE FROM dbo.{config.EV_TABLE} WHERE sportsbook IS NULL",
    ]

    with engine.begin() as conn:
        for sql in cleanup_sqls:
            result = conn.execute(text(sql))
            logger.info(f"Cleared previous predictions: {sql.split(' WHERE')[0]}  "
                        f"({result.rowcount} rows)")


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def save_pa_predictions(pa_df: pd.DataFrame, split_set: str):
    """Write per-PA scoring rows to SQL."""
    cols_to_write = [
        "gamePk", "game_date", "season",
        "hitter_id", "hitter_name", "hitter_position",
        "hitter_team_id", "hitter_team_name", "hitter_lineup_position",
        "pitcher_id", "pitcher_name", "pitcher_team_id", "pitcher_team_name",
        "pitcher_is_starter",
        "hitter_bats", "pitcher_throws", "platoon_matchup",
        "prob_xgb", "prob_lgbm", "prob_logreg", "prob_strikeout",
        "actual_strikeout",
    ]
    out = pa_df[[c for c in cols_to_write if c in pa_df.columns]].copy()
    out["split_set"] = split_set
    out["model_version"] = config.MODEL_VERSION
    out["scored_at"] = config.RUN_TIMESTAMP

    load_dataframe(out, config.PA_PREDICTIONS_TABLE, if_exists="append")


def save_game_predictions(game_df: pd.DataFrame, split_set: str):
    out = game_df.copy()
    out = out.rename(columns={f"prob_over_{x}": f"prob_over_{str(x).replace('.', '_')}"
                              for x in [3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5]})
    out["split_set"] = split_set
    out["model_version"] = config.MODEL_VERSION
    out["scored_at"] = config.RUN_TIMESTAMP
    load_dataframe(out, config.GAME_PREDICTIONS_TABLE, if_exists="append")


def save_metrics(metrics_list: list[dict], split_set: str, metric_type: str):
    rows = []
    for m in metrics_list:
        rows.append({
            "run_timestamp": config.RUN_TIMESTAMP,
            "model_version": config.MODEL_VERSION,
            "split_set": split_set,
            "metric_type": metric_type,
            "label": m.get("label"),
            "n": m.get("n") or m.get("n_games"),
            "accuracy": m.get("accuracy"),
            "roc_auc": m.get("roc_auc"),
            "log_loss_val": m.get("log_loss"),
            "brier": m.get("brier"),
            "precision_val": m.get("precision"),
            "recall": m.get("recall"),
            "f1": m.get("f1"),
            "mae": m.get("mae"),
            "rmse": m.get("rmse"),
            "bias": m.get("bias"),
            "base_rate": m.get("base_rate"),
            "pred_rate": m.get("pred_rate"),
        })
    df = pd.DataFrame(rows)
    load_dataframe(df, config.MODEL_METRICS_TABLE, if_exists="append")


def load_pitcher_recent_stats() -> pd.DataFrame:
    """
    Return each pitcher's most recent rolling form stats (from their last game).

    Uses the latest gamePk per pitcher in fact_pitcher_model_featuresv2 so the
    values reflect the pre-game rolling window heading into their next start.

    Columns returned:
        pitcher_id
        weighted_k_per_bf_last_3/5/10  — PA-weighted K rate per batter faced
        avg_strike_pct_last_3/5/10     — strike percentage
        avg_pitches_per_inning_last_3/5/10 — pitches per inning
    """
    engine = get_engine()
    sql = """
        WITH latest AS (
            SELECT player_id, MAX(gamePk) AS latest_gamePk
            FROM dbo.fact_pitcher_model_featuresv2
            GROUP BY player_id
        )
        SELECT
            CAST(p.player_id AS INT)        AS pitcher_id,
            p.weighted_k_per_bf_last_3,
            p.weighted_k_per_bf_last_5,
            p.weighted_k_per_bf_last_10,
            p.avg_strike_pct_last_3,
            p.avg_strike_pct_last_5,
            p.avg_strike_pct_last_10,
            p.avg_pitches_per_inning_last_3,
            p.avg_pitches_per_inning_last_5,
            p.avg_pitches_per_inning_last_10
        FROM dbo.fact_pitcher_model_featuresv2 p
        INNER JOIN latest l
            ON CAST(p.player_id AS INT) = CAST(l.player_id AS INT)
           AND p.gamePk = l.latest_gamePk
    """
    return pd.read_sql(sql, engine)


def load_pitcher_season_games() -> pd.DataFrame:
    """
    Return pitcher_id + distinct game counts per season (2023-2026).
    Counts every gamePk the pitcher appears in regardless of start/relief.
    """
    engine = get_engine()
    sql = """
        SELECT
            CAST(pitcher_id AS INT) AS pitcher_id,
            COUNT(DISTINCT CASE WHEN season = 2023 THEN gamePk END) AS games_2023,
            COUNT(DISTINCT CASE WHEN season = 2024 THEN gamePk END) AS games_2024,
            COUNT(DISTINCT CASE WHEN season = 2025 THEN gamePk END) AS games_2025,
            COUNT(DISTINCT CASE WHEN season = 2026 THEN gamePk END) AS games_2026
        FROM dbo.fact_hitter_pitcher_matchup_model_featuresv2
        WHERE season BETWEEN 2023 AND 2026
        GROUP BY CAST(pitcher_id AS INT)
    """
    return pd.read_sql(sql, engine)


def update_ev_actuals(game_df: pd.DataFrame):
    """
    Back-fill actual_strikeouts and bet_result on EV rows that have odds entered
    (sportsbook IS NOT NULL) but were scored before the game was played.

    clear_existing_predictions() intentionally preserves those rows to protect
    historical bets, but it never updates them with actual results. This does.

    bet_result logic:
        OVER bet  → WIN if actual > line, LOSS if actual < line, PUSH if equal
        UNDER bet → WIN if actual < line, LOSS if actual > line, PUSH if equal
        PASS      → recorded as PASS (no bet placed)
    """
    actuals = game_df[game_df["actual_strikeouts"].notna()][
        ["gamePk", "pitcher_id", "actual_strikeouts"]
    ].copy()

    if actuals.empty:
        logger.info("update_ev_actuals: no completed games to back-fill")
        return

    engine = get_engine()
    updated = 0
    with engine.begin() as conn:
        for _, row in actuals.iterrows():
            result = conn.execute(
                text(f"""
                    UPDATE dbo.{config.EV_TABLE}
                    SET
                        actual_strikeouts = :actual_k,
                        bet_result = CASE
                            WHEN recommended_side = 'PASS' THEN 'PASS'
                            WHEN line IS NULL              THEN NULL
                            WHEN recommended_side = 'OVER'  AND :actual_k > line  THEN 'WIN'
                            WHEN recommended_side = 'OVER'  AND :actual_k < line  THEN 'LOSS'
                            WHEN recommended_side = 'OVER'  AND :actual_k = line  THEN 'PUSH'
                            WHEN recommended_side = 'UNDER' AND :actual_k < line  THEN 'WIN'
                            WHEN recommended_side = 'UNDER' AND :actual_k > line  THEN 'LOSS'
                            WHEN recommended_side = 'UNDER' AND :actual_k = line  THEN 'PUSH'
                            ELSE NULL
                        END
                    WHERE gamePk    = :gamePk
                      AND pitcher_id = :pitcher_id
                      AND sportsbook IS NOT NULL
                      AND actual_strikeouts IS NULL
                """),
                {
                    "actual_k": int(row["actual_strikeouts"]),
                    "gamePk":   int(row["gamePk"]),
                    "pitcher_id": int(row["pitcher_id"]),
                },
            )
            updated += result.rowcount

    logger.info(f"update_ev_actuals: back-filled {updated} sportsbook rows with actual results")


def save_ev_template(game_df: pd.DataFrame):
    """
    Pre-populate the EV table with rows for the test season's pitcher-games.

    Sportsbook / line / odds columns are left NULL for you to fill in -
    when you do, you can run a separate SQL script (see README) to compute
    edge, EV, and recommended side automatically.
    """
    rows = game_df[[
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
    rows = rows.merge(season_games, on="pitcher_id", how="left")
    for col in ["games_2023", "games_2024", "games_2025", "games_2026"]:
        rows[col] = rows[col].fillna(0).astype(int)

    # Recent form stats (weighted K rate, strike %, pitches per inning — last 3/5/10)
    recent_stats = load_pitcher_recent_stats()
    rows = rows.merge(recent_stats, on="pitcher_id", how="left")

    # Empty columns for manual entry
    for c in ["sportsbook", "line", "over_odds", "under_odds",
              "model_prob_over", "model_prob_under",
              "implied_prob_over", "implied_prob_under",
              "edge_over", "edge_under", "ev_over", "ev_under",
              "recommended_side", "kelly_fraction", "bet_result"]:
        rows[c] = None

    rows["model_version"] = config.MODEL_VERSION
    rows["scored_at"] = config.RUN_TIMESTAMP

    load_dataframe(rows, config.EV_TABLE, if_exists="append")


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def run():
    logger.info("=" * 70)
    logger.info(f"MLB Strikeout Pipeline - version {config.MODEL_VERSION}")
    logger.info("=" * 70)

    # 1. Load
    df = data_loader.load_matchup_data()
    train_df, val_df, test_df = data_loader.split_by_season(df)

    # 2. Feature selection (use training schema as the truth)
    feature_cols = features.select_feature_columns(train_df)

    X_train, cat_maps = features.prepare_features(train_df, feature_cols)
    X_val, _ = features.prepare_features(val_df, feature_cols, cat_maps)
    X_test, _ = features.prepare_features(test_df, feature_cols, cat_maps)

    y_train, w_train = features.get_target_and_weights(train_df)
    y_val, w_val = features.get_target_and_weights(val_df)
    y_test, w_test = features.get_target_and_weights(test_df)

    logger.info(
        f"Feature matrix shapes - "
        f"train {X_train.shape}  val {X_val.shape}  test {X_test.shape}"
    )

    # 3. Train base learners
    xgb_model = models.train_xgb(X_train, y_train, w_train, X_val, y_val, w_val)
    lgbm_model = models.train_lgbm(X_train, y_train, w_train, X_val, y_val, w_val)
    logreg_model = models.train_logreg(X_train, y_train, w_train)

    # 4. Predict on val/test (raw)
    p_xgb_val = models.predict_xgb(xgb_model, X_val)
    p_lgbm_val = models.predict_lgbm(lgbm_model, X_val)
    p_lr_val = models.predict_logreg(logreg_model, X_val)

    p_xgb_test = models.predict_xgb(xgb_model, X_test)
    p_lgbm_test = models.predict_lgbm(lgbm_model, X_test)
    p_lr_test = models.predict_logreg(logreg_model, X_test)

    # 5. Calibrate each base model on val
    # Pass sample weights so isotonic regression weights each row by the number
    # of PAs it represents - critical now that y is fractional.
    cal_xgb = models.fit_isotonic(p_xgb_val, y_val, sample_weight=w_val)
    cal_lgbm = models.fit_isotonic(p_lgbm_val, y_val, sample_weight=w_val)
    cal_lr = models.fit_isotonic(p_lr_val, y_val, sample_weight=w_val)

    p_xgb_val_cal = models.apply_isotonic(cal_xgb, p_xgb_val)
    p_lgbm_val_cal = models.apply_isotonic(cal_lgbm, p_lgbm_val)
    p_lr_val_cal = models.apply_isotonic(cal_lr, p_lr_val)

    p_xgb_test_cal = models.apply_isotonic(cal_xgb, p_xgb_test)
    p_lgbm_test_cal = models.apply_isotonic(cal_lgbm, p_lgbm_test)
    p_lr_test_cal = models.apply_isotonic(cal_lr, p_lr_test)

    # 6. Ensemble (calibrated)
    p_ens_val = models.ensemble_probs(p_xgb_val_cal, p_lgbm_val_cal, p_lr_val_cal)
    p_ens_test = models.ensemble_probs(p_xgb_test_cal, p_lgbm_test_cal, p_lr_test_cal)

    # Sanity calibration on top of the ensemble too (cheap insurance)
    cal_ens = models.fit_isotonic(p_ens_val, y_val, sample_weight=w_val)
    p_ens_val = models.apply_isotonic(cal_ens, p_ens_val)
    p_ens_test = models.apply_isotonic(cal_ens, p_ens_test)

    # 7. Per-PA evaluation
    val_metrics = [
        evaluation.evaluate_classifier(y_val, p_xgb_val_cal, label="xgb_calibrated"),
        evaluation.evaluate_classifier(y_val, p_lgbm_val_cal, label="lgbm_calibrated"),
        evaluation.evaluate_classifier(y_val, p_lr_val_cal, label="logreg_calibrated"),
        evaluation.evaluate_classifier(y_val, p_ens_val, label="ensemble"),
    ]
    test_metrics = [
        evaluation.evaluate_classifier(y_test, p_xgb_test_cal, label="xgb_calibrated"),
        evaluation.evaluate_classifier(y_test, p_lgbm_test_cal, label="lgbm_calibrated"),
        evaluation.evaluate_classifier(y_test, p_lr_test_cal, label="logreg_calibrated"),
        evaluation.evaluate_classifier(y_test, p_ens_test, label="ensemble"),
    ]

    # 8. Build PA-level prediction frames for SQL output
    def make_pa_output(source_df, p_xgb, p_lgbm, p_lr, p_ens, y):
        out = source_df[[
            "gamePk", "game_date", "season",
            "hitter_id", "hitter_name", "hitter_position",
            "hitter_team_id", "hitter_team_name", "hitter_lineup_position",
            "pitcher_id", "pitcher_name", "pitcher_team_id", "pitcher_team_name",
        ]].copy()
        out["pitcher_is_starter"] = source_df.get("pitcher_is_starter", 0)
        # Pass through PA counts and actual K counts - the aggregation step
        # uses these to correctly handle the fact that each row in the source
        # view represents one HITTER (not one PA), and that hitter may face
        # the pitcher multiple times in a game.
        out["hitter_plate_appearances"] = source_df.get("hitter_plate_appearances", 1)
        out["hitter_strikeouts"] = source_df.get("hitter_strikeouts", 0)
        # Handedness columns - keep on the PA-level output for inspection
        out["hitter_bats"] = source_df.get("hitter_bats")
        out["pitcher_throws"] = source_df.get("pitcher_throws")
        out["platoon_matchup"] = source_df.get("platoon_matchup")
        out["prob_xgb"] = p_xgb
        out["prob_lgbm"] = p_lgbm
        out["prob_logreg"] = p_lr
        out["prob_strikeout"] = p_ens
        out["actual_strikeout"] = y.values
        return out

    pa_val = make_pa_output(val_df, p_xgb_val_cal, p_lgbm_val_cal, p_lr_val_cal, p_ens_val, y_val)
    pa_test = make_pa_output(test_df, p_xgb_test_cal, p_lgbm_test_cal, p_lr_test_cal, p_ens_test, y_test)

    # 9. Aggregate to pitcher-game level
    game_val = game_aggregation.aggregate_to_pitcher_game(pa_val)
    game_test = game_aggregation.aggregate_to_pitcher_game(pa_test)

    val_game_metrics = []
    if game_val["actual_strikeouts"].notna().any():
        val_game_metrics.append(
            evaluation.evaluate_game_total(
                game_val.dropna(subset=["actual_strikeouts"]),
                label="ensemble_game"
            )
        )
    test_game_metrics = []
    if game_test["actual_strikeouts"].notna().any():
        test_game_metrics.append(
            evaluation.evaluate_game_total(
                game_test.dropna(subset=["actual_strikeouts"]),
                label="ensemble_game"
            )
        )

    # 10. Save artefacts (models + calibrators) for later inference
    os.makedirs("artifacts", exist_ok=True)
    xgb_model.save_model("artifacts/xgb.json")
    lgbm_model.save_model("artifacts/lgbm.txt")
    with open("artifacts/logreg.pkl", "wb") as f:
        pickle.dump(logreg_model, f)
    with open("artifacts/calibrators.pkl", "wb") as f:
        pickle.dump({
            "xgb": cal_xgb, "lgbm": cal_lgbm, "logreg": cal_lr, "ensemble": cal_ens,
        }, f)
    with open("artifacts/feature_cols.pkl", "wb") as f:
        pickle.dump({"feature_cols": feature_cols, "cat_maps": cat_maps}, f)
    logger.info("Saved model artefacts to ./artifacts/")

    # 11. Persist results to SQL Server
    ensure_output_tables()
    clear_existing_predictions()  # wipe previous-run rows so we don't duplicate

    save_pa_predictions(pa_val, split_set="validation")
    save_pa_predictions(pa_test, split_set="test")
    save_game_predictions(game_val, split_set="validation")
    save_game_predictions(game_test, split_set="test")
    save_ev_template(game_test)   # only test season into EV template - that's where you'll bet
    update_ev_actuals(game_test)  # back-fill actual_strikeouts on protected sportsbook rows

    save_metrics(val_metrics, "validation", "per_pa")
    save_metrics(test_metrics, "test", "per_pa")
    save_metrics(val_game_metrics, "validation", "game_total")
    save_metrics(test_game_metrics, "test", "game_total")

    logger.info("Pipeline complete.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    run()
