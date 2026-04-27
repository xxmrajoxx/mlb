"""
MLB Pitcher Strikeout Prediction Pipeline
=========================================

Predicts:
  1. Per-PA strikeout probability  (binary classifier)
  2. Game-level expected strikeouts pitcher will record vs each hitter and
     the full opposing lineup  (sum of calibrated PA probabilities)

Source table: mlb.dbo.fact_hitter_pitcher_matchup_model_featuresv2
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd

import lightgbm as lgb
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    brier_score_loss,
    log_loss,
    roc_auc_score,
    average_precision_score,
)
from sklearn.preprocessing import StandardScaler

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("mlb_k_model")


# --------------------------------------------------------------------------- #
# 1. CONFIG
# --------------------------------------------------------------------------- #
@dataclass
class Config:
    # IO
    output_dir: str = "outputs"

    # SQL Server SOURCE (where features are read from)
    # Default: mlb.dbo.fact_hitter_pitcher_matchup_model_featuresv2
    sql_source_table: str = "fact_hitter_pitcher_matchup_model_featuresv2"
    sql_source_schema: str = "dbo"
    sql_source_database: Optional[str] = None  # defaults to sql_database below
    # Optional WHERE clause filter (e.g. "season >= 2023")
    sql_source_where: Optional[str] = None

    # Splits (time-based; random splits leak in baseball)
    train_seasons: tuple = (2023, 2024)
    val_seasons: tuple = (2025,)
    test_seasons: tuple = (2026,)

    # Sampling rules
    keep_only_single_pa_rows: bool = True   # multi-PA rows have averaged features
    min_rolling_history_pa: int = 0         # 0 = keep all, LightGBM handles NaNs

    # SQL Server destination
    # Reads from env if not passed: MSSQL_SERVER, MSSQL_DATABASE,
    # MSSQL_USER, MSSQL_PASSWORD, MSSQL_DRIVER, MSSQL_TRUSTED.
    sql_server: Optional[str] = None
    sql_database: str = "mlb"
    sql_schema: str = "dbo"
    sql_user: Optional[str] = None
    sql_password: Optional[str] = None
    sql_driver: str = "ODBC Driver 18 for SQL Server"
    sql_trusted_connection: bool = False
    sql_encrypt: bool = True
    sql_trust_server_certificate: bool = True
    sql_table_per_pa: str = "fact_pitcher_strikeout_predictions_per_pa"
    sql_table_game: str = "fact_pitcher_strikeout_predictions_game"
    sql_if_exists: str = "replace"  # 'replace' | 'append' | 'fail'
    sql_chunksize: int = 1000
    sql_load: bool = True           # set False to skip SQL load of predictions

    # LightGBM
    lgb_params: dict = None

    def __post_init__(self):
        if self.lgb_params is None:
            self.lgb_params = dict(
                objective="binary",
                metric="binary_logloss",
                learning_rate=0.03,
                num_leaves=63,
                min_data_in_leaf=80,
                feature_fraction=0.85,
                bagging_fraction=0.85,
                bagging_freq=5,
                lambda_l2=1.0,
                verbose=-1,
                n_jobs=-1,
            )
        # pull from env if not explicitly set
        self.sql_server = self.sql_server or os.getenv("MSSQL_SERVER")
        self.sql_user = self.sql_user or os.getenv("MSSQL_USER")
        self.sql_password = self.sql_password or os.getenv("MSSQL_PASSWORD")
        env_db = os.getenv("MSSQL_DATABASE")
        if env_db:
            self.sql_database = env_db
        env_driver = os.getenv("MSSQL_DRIVER")
        if env_driver:
            self.sql_driver = env_driver
        env_trusted = os.getenv("MSSQL_TRUSTED")
        if env_trusted is not None:
            self.sql_trusted_connection = env_trusted.lower() in ("1", "true", "yes")
        # default source database to the destination database if not set
        if self.sql_source_database is None:
            self.sql_source_database = self.sql_database


# --------------------------------------------------------------------------- #
# 2. FEATURE GROUPS
# --------------------------------------------------------------------------- #
# The source table has ~250 columns. Many are redundant (avg_3, avg_5, avg_10,
# weighted_3, weighted_5, weighted_10 of the same underlying stat). We keep the
# weighted_5 and weighted_10 versions (they encode recency-decay) and drop the
# plain rolling means, which cuts ~40 features without losing signal.

# Columns that are the target itself OR derived from same-row outcome -> LEAK.
LEAK_COLS = [
    "hitter_strikeouts",   # target
    "hitter_walks",        # same-PA outcome
    "hitter_hits",         # same-PA outcome
    "hitter_home_runs",    # same-PA outcome
    "y_k_rate",            # = strikeouts / PA on the same row
    "sample_weight",       # equals PA; encodes outcome row-shape
]

# Identifiers we strip before training (kept aside for output joins)
ID_COLS = [
    "gamePk",
    "game_date",
    "season",
    "hitter_id",
    "hitter_name",
    "hitter_position",
    "hitter_team_id",
    "hitter_team_name",
    "pitcher_id",
    "pitcher_name",
    "pitcher_team_id",
    "pitcher_team_name",
    "hitter_plate_appearances",  # used as denominator only; not a feature
]

# Plain rolling means we drop in favour of their weighted twins.
# (Pattern: hitter_avg_<stat>_last_<N>  AND  pitcher_avg_<stat>_last_<N>)
# We KEEP weighted_* and the last_3 raw averages (most recent form has signal).
def is_redundant_avg(col: str) -> bool:
    if not col.startswith(("hitter_avg_", "pitcher_avg_")):
        return False
    # keep last_3 plain averages (most-recent-form snapshot)
    if col.endswith("_last_3"):
        return False
    # drop the longer-window plain averages (weighted twins exist)
    if col.endswith(("_last_5", "_last_10")):
        return True
    return False


# String-valued columns that aren't IDs but still aren't usable as numeric
# features. Most are redundant labels (e.g. "Third Base" twin of an integer).
NON_NUMERIC_FEATURE_COLS = {
    "hitter_lineup_position_name",
}


def build_feature_list(all_cols: Iterable[str], df: pd.DataFrame | None = None) -> list[str]:
    feats = []
    for c in all_cols:
        if c in ID_COLS or c in LEAK_COLS:
            continue
        if c in NON_NUMERIC_FEATURE_COLS:
            continue
        if c == "y":  # target itself
            continue
        if is_redundant_avg(c):
            continue
        # auto-drop any remaining non-numeric columns (string descriptors etc.)
        if df is not None:
            if df[c].dtype == "object":
                continue
        feats.append(c)
    return feats


# --------------------------------------------------------------------------- #
# 3. LOAD + CLEAN
# --------------------------------------------------------------------------- #
def load_data(cfg: Config) -> pd.DataFrame:
    """Read the feature table from SQL Server.

    Source: [<sql_source_database>].[<sql_source_schema>].[<sql_source_table>]
    Defaults to mlb.dbo.fact_hitter_pitcher_matchup_model_featuresv2.

    A WHERE filter can be applied via cfg.sql_source_where (e.g. season >= 2023)
    to keep memory in check on very large tables.
    """
    full_name = (
        f"[{cfg.sql_source_database}].[{cfg.sql_source_schema}].[{cfg.sql_source_table}]"
    )
    where = f"WHERE {cfg.sql_source_where}" if cfg.sql_source_where else ""
    sql = f"SELECT * FROM {full_name} {where}".strip()
    log.info(f"Loading from SQL: {sql}")

    engine = _build_mssql_engine(cfg, database=cfg.sql_source_database)
    df = pd.read_sql(sql, engine)
    log.info(f"  shape={df.shape}")

    # Defensive: make sure the date is a datetime (pyodbc usually returns
    # it as datetime already, but pandas' inferred dtype can vary).
    if "game_date" in df.columns:
        df["game_date"] = pd.to_datetime(df["game_date"])

    # Defensive: SQL Server NULL becomes pandas NaN automatically. Some
    # exports use the string 'NULL'; coerce those if any slipped through.
    obj_cols = df.select_dtypes(include="object").columns
    for c in obj_cols:
        df[c] = df[c].replace({"NULL": np.nan, "": np.nan})

    return df


def make_target(df: pd.DataFrame) -> pd.DataFrame:
    """Per-PA binary K target.

    A row's target = 1 if strikeouts >= 1 within its PAs, else 0.
    For single-PA rows this is exactly 'did the pitcher strike out the hitter
    in this plate appearance?'.
    """
    df = df.copy()
    df["y"] = (df["hitter_strikeouts"] >= 1).astype(int)
    return df


def filter_rows(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    """Keep clean per-PA rows for training (multi-PA rows have averaged features
    and pollute the per-PA signal)."""
    n0 = len(df)
    if cfg.keep_only_single_pa_rows:
        df = df[df["hitter_plate_appearances"] == 1].copy()
    log.info(f"  rows after PA=1 filter: {len(df)} (dropped {n0-len(df)})")
    return df


# --------------------------------------------------------------------------- #
# 4. SPLITS
# --------------------------------------------------------------------------- #
def time_split(df: pd.DataFrame, cfg: Config):
    train = df[df["season"].isin(cfg.train_seasons)].copy()
    val = df[df["season"].isin(cfg.val_seasons)].copy()
    test = df[df["season"].isin(cfg.test_seasons)].copy()
    log.info(
        f"  split sizes: train={len(train)}  val={len(val)}  test={len(test)}"
    )
    return train, val, test


# --------------------------------------------------------------------------- #
# 5. MODELS
# --------------------------------------------------------------------------- #
def train_lightgbm(X_tr, y_tr, X_val, y_val, feature_names, cfg: Config):
    log.info("Training LightGBM…")
    dtr = lgb.Dataset(X_tr, label=y_tr, feature_name=feature_names)
    dval = lgb.Dataset(X_val, label=y_val, reference=dtr, feature_name=feature_names)
    booster = lgb.train(
        cfg.lgb_params,
        dtr,
        num_boost_round=4000,
        valid_sets=[dtr, dval],
        valid_names=["train", "val"],
        callbacks=[
            lgb.early_stopping(stopping_rounds=150),
            lgb.log_evaluation(period=200),
        ],
    )
    return booster


def train_logreg_baseline(X_tr, y_tr, X_val, y_val):
    """Sanity-check baseline. If LightGBM only narrowly beats this,
    something is wrong with the feature set."""
    log.info("Training LogReg baseline…")
    # impute medians for LR (it can't handle NaN)
    med = np.nanmedian(X_tr, axis=0)
    Xtr = np.where(np.isnan(X_tr), med, X_tr)
    Xval = np.where(np.isnan(X_val), med, X_val)
    sc = StandardScaler().fit(Xtr)
    lr = LogisticRegression(max_iter=2000, C=0.5, n_jobs=-1)
    lr.fit(sc.transform(Xtr), y_tr)
    p_val = lr.predict_proba(sc.transform(Xval))[:, 1]
    log.info(
        f"  LR val: logloss={log_loss(y_val, p_val):.4f}  "
        f"AUC={roc_auc_score(y_val, p_val):.4f}"
    )
    return lr, sc, med


# --------------------------------------------------------------------------- #
# 6. CALIBRATION  (critical: we sum probabilities, so they must mean what they say)
# --------------------------------------------------------------------------- #
def fit_calibrator(p_raw: np.ndarray, y: np.ndarray) -> IsotonicRegression:
    iso = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
    iso.fit(p_raw, y)
    return iso


def evaluate(name: str, y_true, p_hat):
    log.info(
        f"  [{name}]  N={len(y_true)}  "
        f"logloss={log_loss(y_true, p_hat):.4f}  "
        f"Brier={brier_score_loss(y_true, p_hat):.4f}  "
        f"AUC={roc_auc_score(y_true, p_hat):.4f}  "
        f"PR-AUC={average_precision_score(y_true, p_hat):.4f}  "
        f"k_rate_true={y_true.mean():.4f}  k_rate_pred={p_hat.mean():.4f}"
    )


# --------------------------------------------------------------------------- #
# 7. GAME-LEVEL AGGREGATION
# --------------------------------------------------------------------------- #
def build_matchup_table(
    df_rows: pd.DataFrame,
    p_cal: np.ndarray,
    p_raw: np.ndarray,
) -> pd.DataFrame:
    """One row per (game, pitcher, hitter, batting_order_slot).

    Columns are everything you need to read the prediction at a glance:
      identifiers, calibrated K-prob, raw K-prob, and the most useful
      context features (recent form, pitcher form, h2h history).

    Tolerant of missing columns — only includes those present in df_rows.
    """
    desired_cols = [
        # game / teams
        "gamePk", "game_date", "season",
        "pitcher_id", "pitcher_name", "pitcher_team_name",
        "hitter_id", "hitter_name", "hitter_team_name",
        "hitter_position", "hitter_lineup_position",
        "hitter_lineup_position_name", "hitter_batting_order",
        "first_inning_faced",
        # pitcher form / role
        "pitcher_is_starter", "pitcher_gamesStarted",
        "pitcher_days_since_last_appearance",
        "pitcher_prev_k", "pitcher_prev_ip", "pitcher_prev_bf",
        "pitcher_prev_pitches", "pitcher_prev_k9",
        "pitcher_avg_k_last_3", "pitcher_avg_ip_last_3",
        "pitcher_avg_pitches_last_3", "pitcher_avg_k9_last_3",
        "pitcher_weighted_k_per_bf_last_5",
        "pitcher_weighted_k_per_bf_last_10",
        "pitcher_weighted_k9_last_5",
        "pitcher_weighted_strike_pct_last_5",
        "pitcher_weighted_whiff_rate_last_5",
        "pitcher_weighted_csw_rate_last_5",
        "pitcher_weighted_putaway_rate_last_5",
        "pitcher_weighted_chase_rate_last_5",
        "pitcher_avg_velocity_last_3",
        "pitcher_avg_zone_rate_last_3",
        # hitter form
        "hitter_prev_k", "hitter_prev_pa", "hitter_prev_ab",
        "hitter_prev_hits", "hitter_prev_hr", "hitter_prev_bb",
        "hitter_prev_ops", "hitter_prev_k_rate",
        "hitter_avg_k_last_3", "hitter_avg_pa_last_3",
        "hitter_weighted_k_rate_last_5", "hitter_weighted_k_rate_last_10",
        "hitter_weighted_walk_rate_last_5",
        "hitter_weighted_batting_avg_last_5",
        "hitter_weighted_ops_last_5",
        "hitter_weighted_whiff_rate_last_5",
        "hitter_weighted_contact_rate_last_5",
        "hitter_weighted_chase_rate_last_5",
        "hitter_weighted_two_strike_whiff_rate_last_5",
        "hitter_weighted_csw_against_rate_last_5",
        "hitter_avg_exit_velocity_last_5", "hitter_avg_xwoba_last_5",
        # head-to-head
        "h2h_career_pa", "h2h_career_k", "h2h_career_bb",
        "h2h_career_hits", "h2h_career_hr", "h2h_career_games",
        "h2h_career_k_rate", "is_first_matchup",
        # observed (for evaluation only — drop before live use)
        "hitter_plate_appearances", "hitter_strikeouts",
    ]
    keep = [c for c in desired_cols if c in df_rows.columns]
    out = df_rows[keep].copy()

    out["k_prob_calibrated"] = p_cal
    out["k_prob_raw"] = p_raw
    out["expected_k"] = p_cal
    out["k_variance"] = p_cal * (1.0 - p_cal)
    return out


def aggregate_pitcher_vs_lineup(matchup_df: pd.DataFrame) -> pd.DataFrame:
    """One row per (game, pitcher) — the headline 'how many K's will this
    starter rack up tonight' table.

    Each PA is ~Bernoulli(p_i); the sum across PAs is Poisson-binomial.
      E[K]   = Σ p_i
      Var[K] = Σ p_i (1 - p_i)
    Normal approx is fine for n ≥ ~10 PAs (a typical starter faces 18–28).
    """
    group_keys = [
        "gamePk", "game_date", "season",
        "pitcher_id", "pitcher_name", "pitcher_team_name",
        "hitter_team_name",
    ]
    g = (
        matchup_df.groupby(group_keys, dropna=False)
        .agg(
            total_PA_faced=("hitter_plate_appearances", "sum"),
            total_K_observed=("hitter_strikeouts", "sum"),
            expected_K=("expected_k", "sum"),
            variance_K=("k_variance", "sum"),
            n_unique_hitters=("hitter_id", "nunique"),
            avg_PA_K_prob=("k_prob_calibrated", "mean"),
            max_PA_K_prob=("k_prob_calibrated", "max"),
            min_PA_K_prob=("k_prob_calibrated", "min"),
        )
        .reset_index()
    )
    g["std_K"] = np.sqrt(g["variance_K"])
    # 80% interval, normal approx (z = 1.2816)
    g["k_lower_80"] = (g["expected_K"] - 1.2816 * g["std_K"]).clip(lower=0)
    g["k_upper_80"] = g["expected_K"] + 1.2816 * g["std_K"]
    # "K's above expected" = how the pitcher actually did vs the model
    g["k_above_expected"] = g["total_K_observed"] - g["expected_K"]
    return g


# --------------------------------------------------------------------------- #
# 8. SQL SERVER LOADER
# --------------------------------------------------------------------------- #
def _build_mssql_engine(cfg: Config, database: Optional[str] = None):
    """Build a SQLAlchemy engine for SQL Server via pyodbc/ODBC.

    Args:
        cfg: Config with SQL connection settings.
        database: Optional override for which database to connect to.
            Useful when the source table lives in a different database
            from the destination tables. Defaults to cfg.sql_database.

    Auth precedence:
      1. Trusted (Windows) connection if cfg.sql_trusted_connection is True
      2. Otherwise SQL auth via cfg.sql_user / cfg.sql_password
    """
    try:
        from sqlalchemy import create_engine
    except ImportError as e:
        raise RuntimeError(
            "SQLAlchemy is required for SQL Server loading. "
            "Install with: pip install sqlalchemy pyodbc"
        ) from e

    if not cfg.sql_server:
        raise ValueError(
            "No SQL Server host configured. Set cfg.sql_server or env MSSQL_SERVER."
        )

    db = database or cfg.sql_database
    parts = [
        f"DRIVER={{{cfg.sql_driver}}}",
        f"SERVER={cfg.sql_server}",
        f"DATABASE={db}",
    ]
    if cfg.sql_trusted_connection:
        parts.append("Trusted_Connection=yes")
    else:
        if not (cfg.sql_user and cfg.sql_password):
            raise ValueError(
                "SQL auth requires sql_user and sql_password "
                "(or env MSSQL_USER / MSSQL_PASSWORD), "
                "or set sql_trusted_connection=True for Windows auth."
            )
        parts.append(f"UID={cfg.sql_user}")
        parts.append(f"PWD={cfg.sql_password}")

    if cfg.sql_encrypt:
        parts.append("Encrypt=yes")
    if cfg.sql_trust_server_certificate:
        parts.append("TrustServerCertificate=yes")

    odbc_str = ";".join(parts) + ";"
    url = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_str)
    return create_engine(url, fast_executemany=True)


def _mssql_dtypes_per_pa():
    """Explicit type mapping so the table is well-shaped on the SQL side.
    Avoids pandas' default of NVARCHAR(MAX) for object columns and FLOAT for ints."""
    from sqlalchemy.types import (
        BigInteger, DateTime, Float, Integer, SmallInteger, String,
    )
    return {
        # identifiers
        "gamePk": BigInteger(),
        "game_date": DateTime(),
        "season": SmallInteger(),
        "pitcher_id": BigInteger(),
        "pitcher_name": String(100),
        "pitcher_team_name": String(100),
        "hitter_id": BigInteger(),
        "hitter_name": String(100),
        "hitter_team_name": String(100),
        "hitter_position": String(10),
        "hitter_lineup_position": SmallInteger(),
        "hitter_lineup_position_name": String(40),
        "hitter_batting_order": SmallInteger(),
        "first_inning_faced": SmallInteger(),
        # role / rest
        "pitcher_is_starter": SmallInteger(),
        "pitcher_gamesStarted": SmallInteger(),
        "pitcher_days_since_last_appearance": SmallInteger(),
        # observed (for backtest only)
        "hitter_plate_appearances": SmallInteger(),
        "hitter_strikeouts": SmallInteger(),
        # predictions
        "k_prob_calibrated": Float(),
        "k_prob_raw": Float(),
        "expected_k": Float(),
        "k_variance": Float(),
        "prediction_generated_at": DateTime(),
    }


def _mssql_dtypes_game():
    from sqlalchemy.types import (
        BigInteger, DateTime, Float, Integer, SmallInteger, String,
    )
    return {
        "gamePk": BigInteger(),
        "game_date": DateTime(),
        "season": SmallInteger(),
        "pitcher_id": BigInteger(),
        "pitcher_name": String(100),
        "pitcher_team_name": String(100),
        "hitter_team_name": String(100),
        "pitcher_is_starter": SmallInteger(),
        "total_PA_faced": SmallInteger(),
        "total_K_observed": SmallInteger(),
        "n_unique_hitters": SmallInteger(),
        "expected_K": Float(),
        "variance_K": Float(),
        "std_K": Float(),
        "k_lower_80": Float(),
        "k_upper_80": Float(),
        "k_above_expected": Float(),
        "avg_PA_K_prob": Float(),
        "max_PA_K_prob": Float(),
        "min_PA_K_prob": Float(),
        "prediction_generated_at": DateTime(),
    }


def _add_index_and_order_table(engine, schema: str, table: str,
                               order_clause: str, key_cols: list[str]):
    """After bulk-load, create a clustered index that physically orders
    the table by the desired sort. SQL Server doesn't honour pandas'
    insertion order for table scans without an index.
    """
    from sqlalchemy import text
    idx_name = f"CIX_{table}"
    cols_sql = ", ".join(f"[{c}]" for c in key_cols)
    sql = f"""
    IF EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = N'{idx_name}'
          AND object_id = OBJECT_ID(N'[{schema}].[{table}]')
    )
        DROP INDEX [{idx_name}] ON [{schema}].[{table}];
    CREATE CLUSTERED INDEX [{idx_name}]
        ON [{schema}].[{table}] ({cols_sql});
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def load_to_sqlserver(
    df_pa: pd.DataFrame,
    df_game: pd.DataFrame,
    cfg: Config,
):
    """Load both prediction tables to SQL Server.

    Tables:
      mlb.dbo.fact_pitcher_strikeout_predictions_per_pa
      mlb.dbo.fact_pitcher_strikeout_predictions_game

    Both:
      - have a `prediction_generated_at` UTC timestamp set per run
      - are physically ordered by pitcher_team_name, then starters first
        (pitcher_is_starter DESC), then game_date and pitcher_name
      - get a clustered index enforcing that order on disk
    """
    engine = _build_mssql_engine(cfg)

    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)  # SQL Server DATETIME2 is naive

    # --- shape per-PA frame ---
    df_pa = df_pa.copy()
    df_pa["prediction_generated_at"] = now_utc

    # ensure pitcher_is_starter exists for ordering even if it wasn't in build_matchup_table
    if "pitcher_is_starter" not in df_pa.columns:
        df_pa["pitcher_is_starter"] = np.nan

    # sort: pitcher_team_name asc, starters first (1 before 0), date asc, pitcher_name asc
    pa_sort_cols = ["pitcher_team_name", "pitcher_is_starter", "game_date", "pitcher_name"]
    pa_ascending = [True, False, True, True]
    pa_sort_cols = [c for c in pa_sort_cols if c in df_pa.columns]
    pa_ascending = pa_ascending[: len(pa_sort_cols)]
    df_pa = df_pa.sort_values(pa_sort_cols, ascending=pa_ascending, kind="mergesort").reset_index(drop=True)

    # --- shape game-level frame: enrich with pitcher_is_starter from PA frame ---
    df_game = df_game.copy()
    if "pitcher_is_starter" not in df_game.columns and "pitcher_is_starter" in df_pa.columns:
        starter_lookup = (
            df_pa.dropna(subset=["pitcher_is_starter"])
                 .groupby(["gamePk", "pitcher_id"])["pitcher_is_starter"]
                 .max()
                 .reset_index()
        )
        df_game = df_game.merge(starter_lookup, on=["gamePk", "pitcher_id"], how="left")
    df_game["prediction_generated_at"] = now_utc
    g_sort_cols = ["pitcher_team_name", "pitcher_is_starter", "game_date", "pitcher_name"]
    g_ascending = [True, False, True, True]
    g_sort_cols = [c for c in g_sort_cols if c in df_game.columns]
    g_ascending = g_ascending[: len(g_sort_cols)]
    df_game = df_game.sort_values(g_sort_cols, ascending=g_ascending, kind="mergesort").reset_index(drop=True)

    # --- load per-PA ---
    log.info(
        f"Loading {len(df_pa):,} rows -> "
        f"[{cfg.sql_database}].[{cfg.sql_schema}].[{cfg.sql_table_per_pa}] "
        f"(if_exists={cfg.sql_if_exists})"
    )
    df_pa.to_sql(
        name=cfg.sql_table_per_pa,
        con=engine,
        schema=cfg.sql_schema,
        if_exists=cfg.sql_if_exists,
        index=False,
        chunksize=cfg.sql_chunksize,
        method=None,        # uses fast_executemany on the engine
        dtype=_mssql_dtypes_per_pa(),
    )
    if cfg.sql_if_exists == "replace":
        _add_index_and_order_table(
            engine, cfg.sql_schema, cfg.sql_table_per_pa,
            order_clause="pitcher_team_name, pitcher_is_starter DESC, game_date, pitcher_name",
            key_cols=["pitcher_team_name", "pitcher_is_starter", "game_date", "pitcher_name"],
        )

    # --- load game-level ---
    log.info(
        f"Loading {len(df_game):,} rows -> "
        f"[{cfg.sql_database}].[{cfg.sql_schema}].[{cfg.sql_table_game}] "
        f"(if_exists={cfg.sql_if_exists})"
    )
    df_game.to_sql(
        name=cfg.sql_table_game,
        con=engine,
        schema=cfg.sql_schema,
        if_exists=cfg.sql_if_exists,
        index=False,
        chunksize=cfg.sql_chunksize,
        method=None,
        dtype=_mssql_dtypes_game(),
    )
    if cfg.sql_if_exists == "replace":
        _add_index_and_order_table(
            engine, cfg.sql_schema, cfg.sql_table_game,
            order_clause="pitcher_team_name, pitcher_is_starter DESC, game_date, pitcher_name",
            key_cols=["pitcher_team_name", "pitcher_is_starter", "game_date", "pitcher_name"],
        )

    log.info("SQL Server load complete.")


# --------------------------------------------------------------------------- #
# 9. MAIN
# --------------------------------------------------------------------------- #
def main(cfg: Config):
    Path(cfg.output_dir).mkdir(parents=True, exist_ok=True)

    df = load_data(cfg)
    df = make_target(df)
    df_filtered = filter_rows(df, cfg)

    feats = build_feature_list(df_filtered.columns, df_filtered)
    log.info(f"Using {len(feats)} features (down from {df.shape[1]} cols)")

    # save the feature list for the README/audit
    Path(cfg.output_dir, "features_used.json").write_text(
        json.dumps(feats, indent=2)
    )

    train, val, test = time_split(df_filtered, cfg)
    if len(train) == 0 or len(val) == 0:
        # Fallback: data might only contain one season — warn and degrade
        log.warning(
            "Empty train or val split with the configured seasons. "
            "Falling back to a within-season chronological 70/15/15 split."
        )
        df_sorted = df_filtered.sort_values("game_date")
        n = len(df_sorted)
        train = df_sorted.iloc[: int(0.70 * n)].copy()
        val = df_sorted.iloc[int(0.70 * n): int(0.85 * n)].copy()
        test = df_sorted.iloc[int(0.85 * n):].copy()
        log.info(f"  fallback sizes: train={len(train)}  val={len(val)}  test={len(test)}")

    X_tr, y_tr = train[feats].astype(float).values, train["y"].values
    X_val, y_val = val[feats].astype(float).values, val["y"].values
    X_te, y_te = test[feats].astype(float).values, test["y"].values

    booster = train_lightgbm(X_tr, y_tr, X_val, y_val, feats, cfg)
    booster.save_model(str(Path(cfg.output_dir, "lightgbm_model.txt")))

    # baseline (sanity)
    try:
        train_logreg_baseline(X_tr, y_tr, X_val, y_val)
    except Exception as e:
        log.warning(f"LR baseline skipped: {e}")

    # raw probabilities
    p_val_raw = booster.predict(X_val, num_iteration=booster.best_iteration)
    p_te_raw = (
        booster.predict(X_te, num_iteration=booster.best_iteration)
        if len(X_te) > 0
        else np.array([])
    )

    # calibrate on the validation fold
    iso = fit_calibrator(p_val_raw, y_val)
    p_val_cal = iso.predict(p_val_raw)
    p_te_cal = iso.predict(p_te_raw) if len(p_te_raw) > 0 else np.array([])

    log.info("=== VALIDATION ===")
    evaluate("val raw", y_val, p_val_raw)
    evaluate("val cal", y_val, p_val_cal)
    if len(test) > 0:
        log.info("=== TEST ===")
        evaluate("test raw", y_te, p_te_raw)
        evaluate("test cal", y_te, p_te_cal)

    # feature importance
    imp = pd.DataFrame({
        "feature": feats,
        "gain": booster.feature_importance(importance_type="gain"),
        "split": booster.feature_importance(importance_type="split"),
    }).sort_values("gain", ascending=False)
    imp.to_csv(Path(cfg.output_dir, "feature_importance.csv"), index=False)
    log.info("Top 20 features by gain:")
    log.info("\n" + imp.head(20).to_string(index=False))

    # ------------------------------------------------------------------ #
    # OUTPUT TABLES
    # ------------------------------------------------------------------ #
    pa_val = build_matchup_table(val, p_val_cal, p_val_raw)
    pa_te = build_matchup_table(test, p_te_cal, p_te_raw) if len(test) else pa_val.iloc[0:0]
    pa_all = pd.concat([pa_val, pa_te], ignore_index=True)
    pa_all.to_csv(Path(cfg.output_dir, "predictions_per_PA.csv"), index=False)

    game_agg = aggregate_pitcher_vs_lineup(pa_all)
    game_agg = game_agg.sort_values(["game_date", "pitcher_name"])
    game_agg.to_csv(Path(cfg.output_dir, "predictions_pitcher_vs_lineup.csv"), index=False)

    log.info(f"Wrote {cfg.output_dir}/predictions_per_PA.csv  ({len(pa_all)} rows)")
    log.info(f"Wrote {cfg.output_dir}/predictions_pitcher_vs_lineup.csv  ({len(game_agg)} rows)")

    # ------------------------------------------------------------------ #
    # SQL SERVER LOAD
    # ------------------------------------------------------------------ #
    if cfg.sql_load:
        try:
            load_to_sqlserver(pa_all, game_agg, cfg)
        except Exception as e:
            log.error(f"SQL Server load failed: {e}")
            log.error("CSVs are still on disk in the output dir.")
            raise
    else:
        log.info("sql_load=False, skipping SQL Server load.")

    log.info("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Train MLB pitcher strikeout model from SQL Server source table."
    )
    parser.add_argument("--out", default="outputs",
                        help="Local output directory for CSVs and the model artefact.")
    # SQL Server connection (used for both source READ and destination WRITE)
    parser.add_argument("--sql-server", default=None,
                        help="SQL Server hostname, e.g. 'sqlhost.example.com,1433'")
    parser.add_argument("--sql-database", default="mlb",
                        help="Destination database (where prediction tables are written).")
    parser.add_argument("--sql-schema", default="dbo",
                        help="Destination schema for prediction tables.")
    parser.add_argument("--sql-user", default=None)
    parser.add_argument("--sql-password", default=None)
    parser.add_argument("--sql-driver", default="ODBC Driver 18 for SQL Server")
    parser.add_argument("--sql-trusted", action="store_true",
                        help="Use Windows trusted (integrated) auth")
    # SQL Server SOURCE table (where features are read from)
    parser.add_argument("--sql-source-table",
                        default="fact_hitter_pitcher_matchup_model_featuresv2",
                        help="Source feature table name (no schema/db prefix).")
    parser.add_argument("--sql-source-schema", default="dbo",
                        help="Source schema (default: dbo).")
    parser.add_argument("--sql-source-database", default=None,
                        help="Source database (default: same as --sql-database).")
    parser.add_argument("--sql-source-where", default=None,
                        help="Optional WHERE clause body, e.g. 'season >= 2023'.")
    # Destination tables
    parser.add_argument("--sql-table-per-pa",
                        default="fact_pitcher_strikeout_predictions_per_pa")
    parser.add_argument("--sql-table-game",
                        default="fact_pitcher_strikeout_predictions_game")
    parser.add_argument("--sql-if-exists", default="replace",
                        choices=["replace", "append", "fail"])
    parser.add_argument("--no-sql", action="store_true",
                        help="Skip the SQL load of prediction tables (CSVs still written).")
    args = parser.parse_args()

    cfg = Config(
        output_dir=args.out,
        sql_server=args.sql_server,
        sql_database=args.sql_database,
        sql_schema=args.sql_schema,
        sql_user=args.sql_user,
        sql_password=args.sql_password,
        sql_driver=args.sql_driver,
        sql_trusted_connection=args.sql_trusted,
        sql_source_table=args.sql_source_table,
        sql_source_schema=args.sql_source_schema,
        sql_source_database=args.sql_source_database,
        sql_source_where=args.sql_source_where,
        sql_table_per_pa=args.sql_table_per_pa,
        sql_table_game=args.sql_table_game,
        sql_if_exists=args.sql_if_exists,
        sql_load=not args.no_sql,
    )
    main(cfg)
