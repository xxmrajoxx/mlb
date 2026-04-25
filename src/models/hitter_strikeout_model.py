import os
import sys
import logging
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from sqlalchemy import text
from sklearn.calibration import CalibratedClassifierCV, calibration_curve
from sklearn.metrics import (
    accuracy_score, roc_auc_score, log_loss,
    precision_score, recall_score, f1_score, brier_score_loss,
    classification_report
)
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import OrdinalEncoder
import sklearn.impute as impute
from xgboost import XGBClassifier

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
            "logs/train_hitter_strikeout_classifier.log",
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

    # Binary target: 1 if hitter records at least 1 strikeout
    df["target_hitter_k"] = (df["hitter_strikeOuts"] > 0).astype(int)
    target_col = "target_hitter_k"

    # Meta columns — kept for aggregation and evaluation, not fed to model
    meta_cols = [
        "gamePk", "game_date", "season",
        "hitter_id", "hitter_name", "hitter_position",
        "hitter_team_id", "hitter_team_name",
        "pitcher_id", "pitcher_name",
        "pitcher_team_id", "pitcher_team_name",
        "hitter_batting_order", "hitter_lineup_position", "hitter_lineup_position_name"
    ]

    # Columns to always drop (raw outcomes / game totals)
    drop_cols = [
        "hitter_strikeOuts",
        "pitcher_strikeOuts",
    ]

    # Data leakage columns — same-game outcomes that wouldn't be known pre-game
    leakage_cols = [
        # Same-game matchup Statcast (measured during the game)
        "pitches_seen_vs_pitcher",
        "swings_vs_pitcher",
        "whiffs_vs_pitcher",
        "called_strikes_vs_pitcher",
        "matchup_whiff_rate",
        "matchup_called_strike_rate",
        "matchup_csw_rate",

        # Same-game hitter box score
        "hitter_game_plate_appearances",
        "hitter_game_strikeouts",

        # Rolling windows derived FROM same-game outcomes
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

        # Historical matchup accumulations (can include current game)
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

    cols_to_drop = set(meta_cols + drop_cols + leakage_cols + [target_col])
    cols_to_drop = [c for c in cols_to_drop if c in df.columns]

    X = df.drop(columns=cols_to_drop).copy()
    y = df[target_col].copy()

    # Keep meta + target alongside for season splitting and aggregation
    meta_df = df[
        [c for c in meta_cols if c in df.columns]
        + ["hitter_strikeOuts", target_col]
    ].copy()

    # Pinch hitters skew PA assumptions — flag but keep in training,
    # filter during aggregation
    ph_mask = meta_df.get("hitter_lineup_position_name", pd.Series(dtype=str)) == "Pinch Hitter"
    logger.info(f"Pinch hitter rows: {ph_mask.sum():,} (retained in training, filtered in aggregation)")

    logger.info(f"Feature matrix shape: {X.shape}")
    logger.info(f"Target distribution:\n{y.value_counts(normalize=True).round(3)}")

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
    logger.info(f"Train K rate: {y_train.mean():.3f} | Test K rate: {y_test.mean():.3f}")

    return X_train, X_test, y_train, y_test, meta_train, meta_test


# =========================
# Encode + Impute
# =========================
def encode_and_impute(X_train, X_test):
    """
    1. Convert date columns to ordinal integers
    2. OrdinalEncoder for categoricals (handles unseen test values gracefully)
    3. Median imputation — correct for Statcast NULLs on newer pitchers
       (zero-fill would signal 'no whiff rate' which is false; median says
        'league average' which is the honest pre-game assumption)
    """
    logger.info("Encoding and imputing...")

    X_train = X_train.copy()
    X_test = X_test.copy()

    # --- Date columns to ordinal ---
    for col in X_train.columns:
        if "date" in col.lower():
            X_train[col] = pd.to_datetime(X_train[col], errors="coerce").map(
                lambda x: x.toordinal() if pd.notnull(x) else np.nan
            )
            X_test[col] = pd.to_datetime(X_test[col], errors="coerce").map(
                lambda x: x.toordinal() if pd.notnull(x) else np.nan
            )

    # --- Categorical encoding (fitted on train only) ---
    cat_cols = X_train.select_dtypes(include=["object", "category"]).columns.tolist()
    logger.info(f"Categorical columns: {cat_cols}")

    if cat_cols:
        enc = OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)
        X_train[cat_cols] = enc.fit_transform(X_train[cat_cols].astype(str))
        X_test[cat_cols] = enc.transform(X_test[cat_cols].astype(str))

    # --- Bool to int ---
    for df_part in [X_train, X_test]:
        bool_cols = df_part.select_dtypes(include=["bool"]).columns
        df_part[bool_cols] = df_part[bool_cols].astype(int)

    # --- All columns to numeric ---
    for col in X_train.columns:
        X_train[col] = pd.to_numeric(X_train[col], errors="coerce")
    for col in X_test.columns:
        X_test[col] = pd.to_numeric(X_test[col], errors="coerce")

    # --- Align test columns to train ---
    X_test = X_test.reindex(columns=X_train.columns)

    # --- Median imputation fitted on train ---
    # Using train medians for test ensures no data leakage from test distribution
    imputer = impute.SimpleImputer(strategy="median")
    X_train_arr = imputer.fit_transform(X_train)
    X_test_arr = imputer.transform(X_test)

    X_train = pd.DataFrame(X_train_arr, columns=X_train.columns, index=X_train.index)
    X_test = pd.DataFrame(X_test_arr, columns=X_test.columns, index=X_test.index)

    null_train = X_train.isnull().sum().sum()
    null_test = X_test.isnull().sum().sum()
    logger.info(f"NULLs after imputation — train: {null_train}, test: {null_test}")
    logger.info(f"Encoded X_train shape: {X_train.shape} | X_test shape: {X_test.shape}")

    return X_train, X_test, imputer


# =========================
# Time-series CV on 2025 train set
# =========================
def timeseries_cv(X_train, y_train, meta_train, n_splits=4):
    """
    Walk-forward CV within 2025 training data.
    Ordered by game_date so each fold trains on earlier games,
    validates on later games — mimicking real prediction timing.
    This gives a reliable performance estimate before touching 2026.
    """
    logger.info(f"Running {n_splits}-fold time-series CV on training data...")

    # Sort by date within training set
    date_order = meta_train["game_date"].values
    sort_idx = np.argsort(date_order)
    X_sorted = X_train.iloc[sort_idx]
    y_sorted = y_train.iloc[sort_idx]

    tscv = TimeSeriesSplit(n_splits=n_splits)
    cv_scores = {"roc_auc": [], "log_loss": [], "brier": []}

    for fold, (tr_idx, val_idx) in enumerate(tscv.split(X_sorted)):
        X_tr, X_val = X_sorted.iloc[tr_idx], X_sorted.iloc[val_idx]
        y_tr, y_val = y_sorted.iloc[tr_idx], y_sorted.iloc[val_idx]

        model = XGBClassifier(
            n_estimators=200,
            max_depth=6,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            eval_metric="logloss",
            verbosity=0
        )
        model.fit(X_tr, y_tr)
        proba = model.predict_proba(X_val)[:, 1]

        cv_scores["roc_auc"].append(roc_auc_score(y_val, proba))
        cv_scores["log_loss"].append(log_loss(y_val, proba))
        cv_scores["brier"].append(brier_score_loss(y_val, proba))

        logger.info(
            f"  Fold {fold+1}: ROC AUC={cv_scores['roc_auc'][-1]:.4f} | "
            f"Log Loss={cv_scores['log_loss'][-1]:.4f} | "
            f"Brier={cv_scores['brier'][-1]:.4f}"
        )

    for metric, scores in cv_scores.items():
        logger.info(f"  CV mean {metric}: {np.mean(scores):.4f} ± {np.std(scores):.4f}")

    return cv_scores


# =========================
# Train final model with calibration
# =========================
def train_model(X_train, y_train):
    """
    XGBoost wrapped in CalibratedClassifierCV (isotonic regression).

    Why calibration matters for this use case:
    Raw XGBoost probabilities are systematically overconfident.
    When we sum P(K) across a lineup to get expected pitcher Ks,
    inflated probabilities compound — every hitter's probability
    is too high, so the total is consistently overstated.
    Calibrated probabilities mean P(K)=0.65 actually reflects
    65% historical K rate, making the lineup sum trustworthy for EV.

    isotonic > sigmoid (Platt) for larger datasets and non-monotonic
    miscalibration patterns which are common in sports data.
    """
    logger.info("Training calibrated XGBoost classifier...")

    base_model = XGBClassifier(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        eval_metric="logloss",
        verbosity=0
    )

    # cv=5: fits 5 base models, learns calibration on held-out folds
    # method='isotonic': non-parametric, better for larger datasets
    model = CalibratedClassifierCV(base_model, method="isotonic", cv=5)
    model.fit(X_train, y_train)

    logger.info("Calibrated model training complete")
    return model


# =========================
# Evaluate model
# =========================
def evaluate_model(model, X_test, y_test, meta_test):
    logger.info("Evaluating on 2026 holdout set...")

    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)[:, 1]

    # Core metrics
    accuracy   = accuracy_score(y_test, y_pred)
    roc_auc    = roc_auc_score(y_test, y_pred_proba)
    logloss    = log_loss(y_test, y_pred_proba)
    brier      = brier_score_loss(y_test, y_pred_proba)
    precision  = precision_score(y_test, y_pred, zero_division=0)
    recall     = recall_score(y_test, y_pred, zero_division=0)
    f1         = f1_score(y_test, y_pred, zero_division=0)

    logger.info(f"Accuracy:  {accuracy:.4f}")
    logger.info(f"ROC AUC:   {roc_auc:.4f}")
    logger.info(f"Log Loss:  {logloss:.4f}  (target: < 0.60 good, < 0.50 very good)")
    logger.info(f"Brier:     {brier:.4f}    (target: < 0.25; 0.25 = no-skill baseline)")
    logger.info(f"Precision: {precision:.4f}")
    logger.info(f"Recall:    {recall:.4f}")
    logger.info(f"F1:        {f1:.4f}")

    print("\n=== 2026 Holdout Evaluation ===")
    print(f"  ROC AUC   : {roc_auc:.4f}")
    print(f"  Log Loss  : {logloss:.4f}")
    print(f"  Brier     : {brier:.4f}")
    print(f"  Accuracy  : {accuracy:.4f}")
    print(f"  Precision : {precision:.4f}")
    print(f"  Recall    : {recall:.4f}")
    print(f"  F1        : {f1:.4f}")
    print("\n" + classification_report(y_test, y_pred))

    # Calibration curve — plot to outputs/
    _plot_calibration_curve(y_test, y_pred_proba)

    results_df = meta_test.copy()
    results_df["actual_target"]          = y_test.values
    results_df["predicted_target"]       = y_pred
    results_df["predicted_probability"]  = y_pred_proba
    results_df["load_ts"]                = pd.Timestamp.now()

    return results_df, {
        "accuracy": accuracy, "roc_auc": roc_auc,
        "log_loss": logloss, "brier": brier,
        "precision": precision, "recall": recall, "f1": f1
    }


def _plot_calibration_curve(y_true, y_prob, path="outputs/calibration_curve.png"):
    fraction_pos, mean_pred = calibration_curve(y_true, y_prob, n_bins=10)
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.plot(mean_pred, fraction_pos, "s-", label="Model", color="#1a73e8")
    ax.plot([0, 1], [0, 1], "k--", label="Perfect calibration")
    ax.set_xlabel("Mean predicted probability")
    ax.set_ylabel("Fraction of positives")
    ax.set_title("Calibration Curve — Hitter K Classifier")
    ax.legend()
    plt.tight_layout()
    plt.savefig(path, dpi=120)
    plt.close()
    logger.info(f"Calibration curve saved → {path}")


# =========================
# Pitcher K aggregation
# =========================
def aggregate_pitcher_ks(results_df):
    """
    Core aggregation: sum P(K) × expected_PA_weight per hitter
    to produce an expected pitcher strikeout total per game.

    PA weights by batting order (approximate MLB averages):
    Position 1 sees ~4.5 PA/game, position 9 ~3.8 PA/game.
    Normalised so weights sum to 1 across 9 spots, then scaled
    to a typical 27-PA game (9 innings × 3 outs).

    Pinch hitters are excluded — their PA is situational and
    unpredictable at lineup-post time.

    The output column `expected_pitcher_ks` is your model's
    prediction to compare against sportsbook lines.
    """
    logger.info("Aggregating per-hitter probabilities to pitcher K totals...")

    # PA weight by batting order (position → relative PA share)
    # Based on empirical MLB averages — lineup 1 gets most PA
    pa_weights = {
        1: 4.5, 2: 4.4, 3: 4.3, 4: 4.2, 5: 4.1,
        6: 4.0, 7: 3.9, 8: 3.85, 9: 3.8
    }
    max_pa = max(pa_weights.values())

    df = results_df.copy()

    # Exclude pinch hitters from aggregation
    if "hitter_lineup_position_name" in df.columns:
        df = df[df["hitter_lineup_position_name"] != "Pinch Hitter"].copy()

    # Normalise batting order to [0,1] — fall back to 1.0 if unknown
    df["pa_weight"] = df["hitter_batting_order"].map(
        lambda x: pa_weights.get(int(x), 4.0) / max_pa if pd.notnull(x) else 1.0
    )

    df["weighted_k_prob"] = df["predicted_probability"] * df["pa_weight"]

    agg = (
        df.groupby(["gamePk", "game_date", "pitcher_id", "pitcher_name",
                    "pitcher_team_id", "pitcher_team_name"])
        .agg(
            expected_pitcher_ks=("weighted_k_prob", "sum"),
            hitter_count=("predicted_probability", "count"),
            avg_hitter_k_prob=("predicted_probability", "mean"),
            actual_pitcher_ks=("actual_target", "sum"),   # sum of binary targets = actual Ks
        )
        .reset_index()
    )

    # Pitcher-level evaluation
    mae  = np.mean(np.abs(agg["expected_pitcher_ks"] - agg["actual_pitcher_ks"]))
    rmse = np.sqrt(np.mean((agg["expected_pitcher_ks"] - agg["actual_pitcher_ks"]) ** 2))

    logger.info(f"Pitcher-level MAE:  {mae:.3f}")
    logger.info(f"Pitcher-level RMSE: {rmse:.3f}")

    print(f"\n=== Pitcher K Aggregation ===")
    print(f"  Games evaluated : {len(agg):,}")
    print(f"  Pitcher MAE     : {mae:.3f}")
    print(f"  Pitcher RMSE    : {rmse:.3f}")
    print(f"\nSample predictions:")
    print(
        agg[["pitcher_name", "game_date", "expected_pitcher_ks", "actual_pitcher_ks"]]
        .sort_values("game_date", ascending=False)
        .head(10)
        .to_string(index=False)
    )

    return agg, {"pitcher_mae": mae, "pitcher_rmse": rmse}


# =========================
# EV Calculator — Australian decimal odds
# =========================
def compute_ev(
    expected_ks: float,
    line: float,
    over_odds: float,
    under_odds: float,
    stake: float = 10.0,
) -> dict:
    """
    Computes expected value for over/under pitcher K bets.

    Uses Australian decimal odds (e.g. 1.91, 2.10) as used on
    Sportsbet, TAB, Ladbrokes, Neds, PointsBet etc.

    Decimal odds meaning:
        1.91  → bet $10, get back $19.10 total if you win ($9.10 profit)
        2.10  → bet $10, get back $21.00 total if you win ($11.00 profit)

    Parameters
    ----------
    expected_ks : float
        Model's expected pitcher strikeout total (from aggregate_pitcher_ks)
    line : float
        Sportsbook's over/under line (e.g. 5.5)
    over_odds : float
        Decimal odds for the OVER bet (e.g. 1.91)
    under_odds : float
        Decimal odds for the UNDER bet (e.g. 1.91)
    stake : float
        Your bet size in AUD (default $10)

    Returns
    -------
    dict with all key betting figures in AUD dollar terms

    EV formula (decimal odds):
        EV = (p_win × profit) - (p_loss × stake)
        where profit = stake × (odds - 1)
    """
    from scipy.stats import poisson

    # --- Model probabilities via Poisson distribution ---
    # Treat expected_ks as the Poisson mean (lambda) for this pitcher
    # P(over line)  = P(K > line)  = 1 - CDF(floor(line))
    # P(under line) = P(K < line)  = CDF(ceil(line) - 1)
    p_over  = float(1 - poisson.cdf(int(line), expected_ks))
    p_under = float(poisson.cdf(int(line) - 1 if line == int(line) else int(line), expected_ks))

    # --- Implied probabilities from sportsbook odds ---
    # Decimal odds include the vig — implied prob = 1 / odds
    implied_over  = 1 / over_odds
    implied_under = 1 / under_odds
    total_vig     = implied_over + implied_under  # > 1.0 is the book's margin

    # --- Dollar EV per bet ---
    profit_over  = stake * (over_odds  - 1)   # what you WIN (excluding stake)
    profit_under = stake * (under_odds - 1)

    ev_over_aud  = (p_over  * profit_over)  - ((1 - p_over)  * stake)
    ev_under_aud = (p_under * profit_under) - ((1 - p_under) * stake)

    # --- Edge: model prob minus book's implied prob ---
    edge_over  = (p_over  - implied_over)  * 100
    edge_under = (p_under - implied_under) * 100

    # --- Signal: which side has positive EV ---
    if ev_over_aud > 0 and ev_over_aud >= ev_under_aud:
        signal = f"BET OVER {line}  (+${ev_over_aud:.2f} EV per ${stake:.0f})"
    elif ev_under_aud > 0:
        signal = f"BET UNDER {line}  (+${ev_under_aud:.2f} EV per ${stake:.0f})"
    else:
        signal = "NO BET — negative EV both sides"

    return {
        # Core inputs
        "pitcher_expected_ks":  round(expected_ks, 2),
        "line":                 line,
        "stake_aud":            stake,

        # Odds (decimal, as shown on Sportsbet/TAB etc.)
        "over_odds":            over_odds,
        "under_odds":           under_odds,

        # Model probabilities
        "model_p_over":         round(p_over,  4),
        "model_p_under":        round(p_under, 4),

        # Sportsbook implied probabilities (after vig)
        "implied_p_over":       round(implied_over,  4),
        "implied_p_under":      round(implied_under, 4),
        "book_margin_pct":      round((total_vig - 1) * 100, 2),  # e.g. 4.7% vig

        # Dollar EV — positive = profitable long-run, negative = avoid
        "ev_over_aud":          round(ev_over_aud,  2),
        "ev_under_aud":         round(ev_under_aud, 2),

        # Edge % = how much model prob exceeds implied prob
        "edge_over_pct":        round(edge_over,  2),
        "edge_under_pct":       round(edge_under, 2),

        # Plain-English signal
        "signal":               signal,
    }


# =========================
# Save to SQL — daily upsert
# =========================
def _upsert_hitter_predictions(df: pd.DataFrame, engine) -> int:
    """
    MERGE upsert for fact_hitter_k_predictions.
    Primary key: (gamePk, hitter_id, pitcher_id)
    Inserts new rows, updates existing ones — safe to run daily.
    """
    rows_affected = 0

    with engine.begin() as conn:
        for _, row in df.iterrows():
            sql = text("""
                MERGE dbo.fact_hitter_k_predictions AS target
                USING (SELECT
                    :gamePk                      AS gamePk,
                    :game_date                   AS game_date,
                    :season                      AS season,
                    :hitter_id                   AS hitter_id,
                    :hitter_name                 AS hitter_name,
                    :hitter_team_id              AS hitter_team_id,
                    :hitter_team_name            AS hitter_team_name,
                    :pitcher_id                  AS pitcher_id,
                    :pitcher_name                AS pitcher_name,
                    :pitcher_team_id             AS pitcher_team_id,
                    :pitcher_team_name           AS pitcher_team_name,
                    :hitter_batting_order        AS hitter_batting_order,
                    :hitter_lineup_position      AS hitter_lineup_position,
                    :hitter_lineup_position_name AS hitter_lineup_position_name,
                    :hitter_strikeOuts           AS hitter_strikeOuts,
                    :target_hitter_k             AS target_hitter_k,
                    :actual_target               AS actual_target,
                    :predicted_target            AS predicted_target,
                    :predicted_probability       AS predicted_probability,
                    :load_ts                     AS load_ts
                ) AS source
                ON  target.gamePk    = source.gamePk
                AND target.hitter_id = source.hitter_id
                AND target.pitcher_id = source.pitcher_id
                WHEN MATCHED THEN UPDATE SET
                    target.actual_target         = source.actual_target,
                    target.predicted_target      = source.predicted_target,
                    target.predicted_probability = source.predicted_probability,
                    target.load_ts               = source.load_ts
                WHEN NOT MATCHED THEN INSERT (
                    gamePk, game_date, season,
                    hitter_id, hitter_name,
                    hitter_team_id, hitter_team_name,
                    pitcher_id, pitcher_name,
                    pitcher_team_id, pitcher_team_name,
                    hitter_batting_order,
                    hitter_lineup_position, hitter_lineup_position_name,
                    hitter_strikeOuts, target_hitter_k,
                    actual_target, predicted_target, predicted_probability,
                    load_ts
                ) VALUES (
                    source.gamePk, source.game_date, source.season,
                    source.hitter_id, source.hitter_name,
                    source.hitter_team_id, source.hitter_team_name,
                    source.pitcher_id, source.pitcher_name,
                    source.pitcher_team_id, source.pitcher_team_name,
                    source.hitter_batting_order,
                    source.hitter_lineup_position, source.hitter_lineup_position_name,
                    source.hitter_strikeOuts, source.target_hitter_k,
                    source.actual_target, source.predicted_target,
                    source.predicted_probability, source.load_ts
                );
            """)
            conn.execute(sql, {
                "gamePk":                      int(row["gamePk"]),
                "game_date":                   pd.to_datetime(row["game_date"]).date(),
                "season":                      int(row["season"]),
                "hitter_id":                   int(row["hitter_id"]),
                "hitter_name":                 str(row["hitter_name"]),
                "hitter_team_id":              None if pd.isna(row.get("hitter_team_id")) else int(row["hitter_team_id"]),
                "hitter_team_name":            str(row.get("hitter_team_name", "")),
                "pitcher_id":                  int(row["pitcher_id"]),
                "pitcher_name":                str(row["pitcher_name"]),
                "pitcher_team_id":             None if pd.isna(row.get("pitcher_team_id")) else int(row["pitcher_team_id"]),
                "pitcher_team_name":           str(row.get("pitcher_team_name", "")),
                "hitter_batting_order":        None if pd.isna(row.get("hitter_batting_order")) else int(row["hitter_batting_order"]),
                "hitter_lineup_position":      str(row.get("hitter_lineup_position", "")),
                "hitter_lineup_position_name": str(row.get("hitter_lineup_position_name", "")),
                "hitter_strikeOuts":           None if pd.isna(row.get("hitter_strikeOuts")) else int(row["hitter_strikeOuts"]),
                "target_hitter_k":             int(row["target_hitter_k"]),
                "actual_target":               int(row["actual_target"]),
                "predicted_target":            int(row["predicted_target"]),
                "predicted_probability":       float(row["predicted_probability"]),
                "load_ts":                     pd.Timestamp.now(),
            })
            rows_affected += 1

    return rows_affected


def _upsert_pitcher_predictions(df: pd.DataFrame, engine) -> int:
    """
    MERGE upsert for fact_pitcher_k_predictions.
    Primary key: (gamePk, pitcher_id)
    Inserts new rows, updates existing ones — safe to run daily.
    """
    rows_affected = 0

    with engine.begin() as conn:
        for _, row in df.iterrows():
            sql = text("""
                MERGE dbo.fact_pitcher_k_predictions AS target
                USING (SELECT
                    :gamePk               AS gamePk,
                    :game_date            AS game_date,
                    :pitcher_id           AS pitcher_id,
                    :pitcher_name         AS pitcher_name,
                    :pitcher_team_id      AS pitcher_team_id,
                    :pitcher_team_name    AS pitcher_team_name,
                    :expected_pitcher_ks  AS expected_pitcher_ks,
                    :actual_pitcher_ks    AS actual_pitcher_ks,
                    :hitter_count         AS hitter_count,
                    :avg_hitter_k_prob    AS avg_hitter_k_prob,
                    :load_ts              AS load_ts
                ) AS source
                ON  target.gamePk    = source.gamePk
                AND target.pitcher_id = source.pitcher_id
                WHEN MATCHED THEN UPDATE SET
                    target.expected_pitcher_ks = source.expected_pitcher_ks,
                    target.actual_pitcher_ks   = source.actual_pitcher_ks,
                    target.hitter_count        = source.hitter_count,
                    target.avg_hitter_k_prob   = source.avg_hitter_k_prob,
                    target.load_ts             = source.load_ts
                WHEN NOT MATCHED THEN INSERT (
                    gamePk, game_date,
                    pitcher_id, pitcher_name,
                    pitcher_team_id, pitcher_team_name,
                    expected_pitcher_ks, actual_pitcher_ks,
                    hitter_count, avg_hitter_k_prob, load_ts
                ) VALUES (
                    source.gamePk, source.game_date,
                    source.pitcher_id, source.pitcher_name,
                    source.pitcher_team_id, source.pitcher_team_name,
                    source.expected_pitcher_ks, source.actual_pitcher_ks,
                    source.hitter_count, source.avg_hitter_k_prob, source.load_ts
                );
            """)
            conn.execute(sql, {
                "gamePk":              int(row["gamePk"]),
                "game_date":           pd.to_datetime(row["game_date"]).date(),
                "pitcher_id":          int(row["pitcher_id"]),
                "pitcher_name":        str(row["pitcher_name"]),
                "pitcher_team_id":     None if pd.isna(row.get("pitcher_team_id")) else int(row["pitcher_team_id"]),
                "pitcher_team_name":   str(row.get("pitcher_team_name", "")),
                "expected_pitcher_ks": float(row["expected_pitcher_ks"]),
                "actual_pitcher_ks":   int(row["actual_pitcher_ks"]),
                "hitter_count":        int(row["hitter_count"]),
                "avg_hitter_k_prob":   float(row["avg_hitter_k_prob"]),
                "load_ts":             pd.Timestamp.now(),
            })
            rows_affected += 1

    return rows_affected


def save_results_to_sql(results_df: pd.DataFrame, agg_df: pd.DataFrame) -> None:
    """
    Upserts per-hitter predictions and per-pitcher aggregations to SQL Server.

    Uses MERGE so it's safe to run every day:
      - New game rows are inserted
      - Existing rows (same gamePk + hitter/pitcher id) are updated
      - No truncation, no duplicates

    Tables must exist first — run create_prediction_tables.sql once.
    """
    engine = get_engine()

    logger.info("Upserting hitter predictions...")
    try:
        n = _upsert_hitter_predictions(results_df, engine)
        logger.info(f"Hitter predictions upserted: {n:,} rows")
    except Exception as e:
        logger.error(f"Failed to upsert hitter predictions: {e}")
        raise

    logger.info("Upserting pitcher aggregations...")
    try:
        n = _upsert_pitcher_predictions(agg_df, engine)
        logger.info(f"Pitcher aggregations upserted: {n:,} rows")
    except Exception as e:
        logger.error(f"Failed to upsert pitcher aggregations: {e}")
        raise

    logger.info("SQL upsert complete")


# =========================
# Main
# =========================
def main():
    logger.info("=" * 60)
    logger.info("MLB Pitcher K Prediction Pipeline — START")
    logger.info("=" * 60)

    # 1. Load
    df = load_model_data()

    # 2. Feature prep
    X, y, meta_df = prepare_features(df)

    # 3. Season split
    X_train, X_test, y_train, y_test, meta_train, meta_test = split_by_season(
        X, y, meta_df, train_season=2025, test_season=2026
    )

    # 4. Encode + impute (fitted on train, applied to test — no leakage)
    X_train, X_test, imputer = encode_and_impute(X_train, X_test)

    # 5. Time-series CV within 2025 — reliable estimate before touching holdout
    cv_scores = timeseries_cv(X_train, y_train, meta_train, n_splits=4)

    # 6. Train calibrated model on full 2025
    model = train_model(X_train, y_train)

    # 7. Evaluate on 2026 holdout
    results_df, metrics = evaluate_model(model, X_test, y_test, meta_test)

    # 8. Aggregate hitter probabilities → pitcher K totals
    agg_df, pitcher_metrics = aggregate_pitcher_ks(results_df)

    # 9. EV example — wire to real sportsbook lines in production
    #    Odds shown as Australian decimal (e.g. Sportsbet, TAB, Ladbrokes)
    #    1.91 = standard -110 equivalent, stake in AUD
    example_pitcher = agg_df.iloc[0]
    ev = compute_ev(
        expected_ks=example_pitcher["expected_pitcher_ks"],
        line=5.5,         # replace with actual sportsbook line
        over_odds=1.91,   # replace with live odds from your sportsbook
        under_odds=1.91,
        stake=10.0,       # AUD stake per bet
    )
    logger.info(f"EV example for {example_pitcher['pitcher_name']}: {ev}")
    print(f"\n=== EV Example: {example_pitcher['pitcher_name']} ===")
    print(f"  Expected Ks      : {ev['pitcher_expected_ks']}")
    print(f"  Line             : {ev['line']}")
    print(f"  Stake            : ${ev['stake_aud']:.2f} AUD")
    print(f"  Over odds        : {ev['over_odds']}  (model: {ev['model_p_over']:.1%} | implied: {ev['implied_p_over']:.1%})")
    print(f"  Under odds       : {ev['under_odds']}  (model: {ev['model_p_under']:.1%} | implied: {ev['implied_p_under']:.1%})")
    print(f"  Book margin      : {ev['book_margin_pct']}%")
    print(f"  EV Over          : ${ev['ev_over_aud']:+.2f}  (edge: {ev['edge_over_pct']:+.1f}%)")
    print(f"  EV Under         : ${ev['ev_under_aud']:+.2f}  (edge: {ev['edge_under_pct']:+.1f}%)")
    print(f"  Signal           : {ev['signal']}")

    # 10. Save to SQL — upsert so daily reruns never duplicate rows
    save_results_to_sql(results_df, agg_df)

    logger.info("Pipeline complete")
    return model, results_df, agg_df


if __name__ == "__main__":
    main()
