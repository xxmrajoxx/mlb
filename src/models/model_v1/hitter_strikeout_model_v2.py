import os
import sys
import logging
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score
)
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import OrdinalEncoder
from sklearn.impute import SimpleImputer
from xgboost import XGBRegressor

from sql.sql_loader import get_engine


# =========================
# Logging
# =========================
os.makedirs("logs", exist_ok=True)
os.makedirs("outputs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(
            "logs/train_pitcher_strikeout_regressor.log",
            encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# =========================
# Load data
# =========================
def load_model_data():
    logger.info("Loading model data from SQL Server...")

    query = """
    SELECT *
    FROM mlb.dbo.fact_hitter_pitcher_matchup_model_features
    WHERE hitter_strikeOuts IS NOT NULL
      AND pitcher_strikeOuts IS NOT NULL
    """

    engine = get_engine()
    df = pd.read_sql(query, engine)
    logger.info(f"Loaded {len(df):,} rows and {len(df.columns):,} columns")
    return df


# =========================
# Prepare features
# =========================
def prepare_features(df):
    logger.info("Preparing features...")

    df = df.copy()

    # Regression target: actual hitter strikeouts in this game vs this pitcher
    target_col = "hitter_strikeOuts"

    meta_cols = [
        "gamePk", "game_date", "season",
        "hitter_id", "hitter_name", "hitter_position",
        "hitter_team_id", "hitter_team_name",
        "pitcher_id", "pitcher_name",
        "pitcher_team_id", "pitcher_team_name",
        "hitter_batting_order", "hitter_lineup_position", "hitter_lineup_position_name",
        "pitcher_strikeOuts"
    ]

    # Drop direct target leakage / post-game outcome fields
    drop_cols = [
        "hitter_strikeOuts",
    ]

    leakage_cols = [
        # same-game matchup outcomes
        "pitches_seen_vs_pitcher",
        "swings_vs_pitcher",
        "whiffs_vs_pitcher",
        "called_strikes_vs_pitcher",
        "matchup_whiff_rate",
        "matchup_called_strike_rate",
        "matchup_csw_rate",

        # same-game hitter box score
        "hitter_game_plate_appearances",
        "hitter_game_strikeouts",

        # same-game rolling outcome proxies
        "hitter_game_avg_pa_last_3",
        "hitter_game_avg_pa_last_5",
        "hitter_game_avg_pa_last_10",
        "hitter_game_avg_strikeouts_last_3",
        "hitter_game_avg_strikeouts_last_5",
        "hitter_game_avg_strikeouts_last_10",
        "hitter_game_weighted_pa_last_3",
        "hitter_game_weighted_pa_last_5",
        "hitter_game_weighted_pa_last_10",
        "hitter_game_weighted_strikeouts_last_3",
        "hitter_game_weighted_strikeouts_last_5",
        "hitter_game_weighted_strikeouts_last_10",

        # historical direct matchup totals
        "hitter_pitcher_plate_appearances",
        "hitter_pitcher_hits",
        "hitter_pitcher_singles",
        "hitter_pitcher_doubles",
        "hitter_pitcher_triples",
        "hitter_pitcher_home_runs",
        "hitter_pitcher_walks",
        "hitter_pitcher_hit_by_pitch",
        "hitter_pitcher_sac_flies",
        "hitter_pitcher_sac_bunts",
        "hitter_pitcher_outs_recorded",
        "hitter_pitcher_rbi",
        "hitter_pitcher_first_inning_faced",
    ]

    cols_to_drop = set(meta_cols + drop_cols + leakage_cols)
    cols_to_drop = [c for c in cols_to_drop if c in df.columns]

    X = df.drop(columns=cols_to_drop).copy()
    y = df[target_col].copy()

    meta_df = df[[c for c in meta_cols if c in df.columns] + [target_col]].copy()
    meta_df = meta_df.rename(columns={"hitter_strikeOuts": "actual_hitter_ks"})

    ph_mask = meta_df.get(
        "hitter_lineup_position_name",
        pd.Series(dtype=str)
    ) == "Pinch Hitter"
    logger.info(f"Pinch hitter rows: {ph_mask.sum():,}")

    logger.info(f"Feature matrix shape: {X.shape}")
    logger.info(f"Target mean hitter Ks: {y.mean():.4f}")

    return X, y, meta_df


# =========================
# Split by season
# =========================
def split_by_season(X, y, meta_df, train_season=2025, test_season=2026):
    logger.info(f"Splitting — train: {train_season}, test: {test_season}")

    train_mask = meta_df["season"] == train_season
    test_mask = meta_df["season"] == test_season

    X_train = X.loc[train_mask].copy()
    y_train = y.loc[train_mask].copy()
    meta_train = meta_df.loc[train_mask].copy()

    X_test = X.loc[test_mask].copy()
    y_test = y.loc[test_mask].copy()
    meta_test = meta_df.loc[test_mask].copy()

    logger.info(f"Train rows: {len(X_train):,} | Test rows: {len(X_test):,}")
    logger.info(f"Train avg hitter Ks: {y_train.mean():.4f} | Test avg hitter Ks: {y_test.mean():.4f}")

    return X_train, X_test, y_train, y_test, meta_train, meta_test


# =========================
# Encode + Impute
# =========================
def encode_and_impute(X_train, X_test):
    logger.info("Encoding and imputing...")

    X_train = X_train.copy()
    X_test = X_test.copy()

    for col in X_train.columns:
        if "date" in col.lower():
            X_train[col] = pd.to_datetime(X_train[col], errors="coerce").map(
                lambda x: x.toordinal() if pd.notnull(x) else np.nan
            )
            X_test[col] = pd.to_datetime(X_test[col], errors="coerce").map(
                lambda x: x.toordinal() if pd.notnull(x) else np.nan
            )

    cat_cols = X_train.select_dtypes(include=["object", "category"]).columns.tolist()
    logger.info(f"Categorical columns: {cat_cols}")

    if cat_cols:
        enc = OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)
        X_train[cat_cols] = enc.fit_transform(X_train[cat_cols].astype(str))
        X_test[cat_cols] = enc.transform(X_test[cat_cols].astype(str))

    for df_part in [X_train, X_test]:
        bool_cols = df_part.select_dtypes(include=["bool"]).columns
        df_part[bool_cols] = df_part[bool_cols].astype(int)

    for col in X_train.columns:
        X_train[col] = pd.to_numeric(X_train[col], errors="coerce")
    for col in X_test.columns:
        X_test[col] = pd.to_numeric(X_test[col], errors="coerce")

    X_test = X_test.reindex(columns=X_train.columns)

    imputer = SimpleImputer(strategy="median")
    X_train_arr = imputer.fit_transform(X_train)
    X_test_arr = imputer.transform(X_test)

    X_train = pd.DataFrame(X_train_arr, columns=X_train.columns, index=X_train.index)
    X_test = pd.DataFrame(X_test_arr, columns=X_test.columns, index=X_test.index)

    logger.info(f"Encoded X_train shape: {X_train.shape} | X_test shape: {X_test.shape}")

    return X_train, X_test, imputer


# =========================
# Time-series CV
# =========================
def timeseries_cv(X_train, y_train, meta_train, n_splits=4):
    logger.info(f"Running {n_splits}-fold time-series CV on training data...")

    date_order = pd.to_datetime(meta_train["game_date"]).values
    sort_idx = np.argsort(date_order)

    X_sorted = X_train.iloc[sort_idx]
    y_sorted = y_train.iloc[sort_idx]

    tscv = TimeSeriesSplit(n_splits=n_splits)

    mae_scores = []
    rmse_scores = []

    for fold, (tr_idx, val_idx) in enumerate(tscv.split(X_sorted)):
        X_tr, X_val = X_sorted.iloc[tr_idx], X_sorted.iloc[val_idx]
        y_tr, y_val = y_sorted.iloc[tr_idx], y_sorted.iloc[val_idx]

        model = XGBRegressor(
            n_estimators=300,
            max_depth=6,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            objective="reg:squarederror",
            eval_metric="rmse",
            verbosity=0
        )

        model.fit(X_tr, y_tr)
        pred = model.predict(X_val)
        pred = np.clip(pred, 0, None)

        mae = mean_absolute_error(y_val, pred)
        rmse = np.sqrt(mean_squared_error(y_val, pred))

        mae_scores.append(mae)
        rmse_scores.append(rmse)

        logger.info(f"Fold {fold + 1}: MAE={mae:.4f} | RMSE={rmse:.4f}")

    logger.info(f"CV mean MAE: {np.mean(mae_scores):.4f} ± {np.std(mae_scores):.4f}")
    logger.info(f"CV mean RMSE: {np.mean(rmse_scores):.4f} ± {np.std(rmse_scores):.4f}")

    return {"mae": mae_scores, "rmse": rmse_scores}


# =========================
# Train final model
# =========================
def train_model(X_train, y_train):
    logger.info("Training XGBoost regressor...")

    model = XGBRegressor(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        objective="reg:squarederror",
        eval_metric="rmse",
        verbosity=0
    )

    model.fit(X_train, y_train)
    logger.info("Model training complete")
    return model


# =========================
# Evaluate hitter-level model
# =========================
def evaluate_model(model, X_test, y_test, meta_test):
    logger.info("Evaluating on 2026 holdout set...")

    y_pred = model.predict(X_test)
    y_pred = np.clip(y_pred, 0, None)

    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)

    logger.info(f"Hitter-level MAE:  {mae:.4f}")
    logger.info(f"Hitter-level RMSE: {rmse:.4f}")
    logger.info(f"Hitter-level R²:   {r2:.4f}")

    results_df = meta_test.copy()
    results_df["predicted_hitter_ks"] = y_pred
    results_df["actual_hitter_ks"] = y_test.values
    results_df["load_ts"] = pd.Timestamp.now()

    return results_df, {
        "mae": mae,
        "rmse": rmse,
        "r2": r2
    }


# =========================
# Aggregate to pitcher Ks
# =========================
def aggregate_pitcher_ks(results_df):
    logger.info("Aggregating hitter predictions to pitcher totals...")

    df = results_df.copy()

    # Exclude pinch hitters for cleaner lineup-level aggregation
    if "hitter_lineup_position_name" in df.columns:
        df = df[df["hitter_lineup_position_name"] != "Pinch Hitter"].copy()

    agg = (
        df.groupby(
            ["gamePk", "game_date", "pitcher_id", "pitcher_name",
             "pitcher_team_id", "pitcher_team_name"],
            dropna=False
        )
        .agg(
            expected_pitcher_ks=("predicted_hitter_ks", "sum"),
            actual_pitcher_ks=("actual_hitter_ks", "sum"),
            hitter_count=("hitter_id", "count"),
            avg_predicted_hitter_ks=("predicted_hitter_ks", "mean")
        )
        .reset_index()
    )

    pitcher_mae = mean_absolute_error(agg["actual_pitcher_ks"], agg["expected_pitcher_ks"])
    pitcher_rmse = np.sqrt(mean_squared_error(agg["actual_pitcher_ks"], agg["expected_pitcher_ks"]))

    logger.info(f"Pitcher-level MAE:  {pitcher_mae:.4f}")
    logger.info(f"Pitcher-level RMSE: {pitcher_rmse:.4f}")

    print("\n=== Pitcher K Aggregation ===")
    print(f"Games evaluated : {len(agg):,}")
    print(f"Pitcher MAE     : {pitcher_mae:.4f}")
    print(f"Pitcher RMSE    : {pitcher_rmse:.4f}")
    print("\nSample predictions:")
    print(
        agg[["pitcher_name", "game_date", "expected_pitcher_ks", "actual_pitcher_ks"]]
        .sort_values("game_date", ascending=False)
        .head(10)
        .to_string(index=False)
    )

    return agg, {
        "pitcher_mae": pitcher_mae,
        "pitcher_rmse": pitcher_rmse
    }


# =========================
# EV Calculator
# =========================
def compute_ev(expected_ks, line, over_odds, under_odds, stake=10.0):
    from scipy.stats import poisson

    p_over = float(1 - poisson.cdf(int(line), expected_ks))
    p_under = float(poisson.cdf(int(line) - 1 if line == int(line) else int(line), expected_ks))

    implied_over = 1 / over_odds
    implied_under = 1 / under_odds
    total_vig = implied_over + implied_under

    profit_over = stake * (over_odds - 1)
    profit_under = stake * (under_odds - 1)

    ev_over_aud = (p_over * profit_over) - ((1 - p_over) * stake)
    ev_under_aud = (p_under * profit_under) - ((1 - p_under) * stake)

    edge_over = (p_over - implied_over) * 100
    edge_under = (p_under - implied_under) * 100

    if ev_over_aud > 0 and ev_over_aud >= ev_under_aud:
        signal = f"BET OVER {line} (+${ev_over_aud:.2f} EV per ${stake:.0f})"
    elif ev_under_aud > 0:
        signal = f"BET UNDER {line} (+${ev_under_aud:.2f} EV per ${stake:.0f})"
    else:
        signal = "NO BET — negative EV both sides"

    return {
        "pitcher_expected_ks": round(expected_ks, 2),
        "line": line,
        "stake_aud": stake,
        "over_odds": over_odds,
        "under_odds": under_odds,
        "model_p_over": round(p_over, 4),
        "model_p_under": round(p_under, 4),
        "implied_p_over": round(implied_over, 4),
        "implied_p_under": round(implied_under, 4),
        "book_margin_pct": round((total_vig - 1) * 100, 2),
        "ev_over_aud": round(ev_over_aud, 2),
        "ev_under_aud": round(ev_under_aud, 2),
        "edge_over_pct": round(edge_over, 2),
        "edge_under_pct": round(edge_under, 2),
        "signal": signal,
    }


# =========================
# Main
# =========================
def main():
    logger.info("=" * 60)
    logger.info("MLB Pitcher K Regression Pipeline — START")
    logger.info("=" * 60)

    df = load_model_data()

    X, y, meta_df = prepare_features(df)

    X_train, X_test, y_train, y_test, meta_train, meta_test = split_by_season(
        X, y, meta_df, train_season=2025, test_season=2026
    )

    X_train, X_test, imputer = encode_and_impute(X_train, X_test)

    cv_scores = timeseries_cv(X_train, y_train, meta_train, n_splits=4)

    model = train_model(X_train, y_train)

    results_df, metrics = evaluate_model(model, X_test, y_test, meta_test)

    agg_df, pitcher_metrics = aggregate_pitcher_ks(results_df)

    example_pitcher = agg_df.iloc[0]
    ev = compute_ev(
        expected_ks=example_pitcher["expected_pitcher_ks"],
        line=5.5,
        over_odds=1.91,
        under_odds=1.91,
        stake=10.0,
    )

    logger.info(f"EV example for {example_pitcher['pitcher_name']}: {ev}")

    print(f"\n=== EV Example: {example_pitcher['pitcher_name']} ===")
    print(f"Expected Ks : {ev['pitcher_expected_ks']}")
    print(f"Line        : {ev['line']}")
    print(f"Over odds   : {ev['over_odds']}")
    print(f"Under odds  : {ev['under_odds']}")
    print(f"EV Over     : ${ev['ev_over_aud']:+.2f}")
    print(f"EV Under    : ${ev['ev_under_aud']:+.2f}")
    print(f"Signal      : {ev['signal']}")

    logger.info("Pipeline complete")
    return model, results_df, agg_df


if __name__ == "__main__":
    main()