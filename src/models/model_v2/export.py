"""
Export: write the final predictions table back to SQL Server.
"""
import logging
from typing import List

import numpy as np
import pandas as pd
import xgboost as xgb

from model_1_k_probability import config

try:
    from sql.sql_loader import execute_sql, write_dataframe
except ImportError:
    # Adjust to your sql_loader's actual helper names
    from sql.sql_loader import execute_sql
    write_dataframe = None

logger = logging.getLogger(__name__)


def build_predictions_dataframe(
    raw_df: pd.DataFrame,
    predictions: pd.DataFrame,
    model: xgb.Booster,
    feature_cols: List[str],
) -> pd.DataFrame:
    """
    Build the final export-ready DataFrame.

    Includes IDs, predictions, actuals (where known), key features for
    post-hoc analysis, and quality flags.
    """
    df = raw_df.copy()
    df["predicted_k_rate"] = predictions["predicted_k_rate"].values
    df["prediction_std"] = predictions["prediction_std"].values

    # Predicted count per matchup = rate × expected PAs
    # For backtesting we use actual PAs; for live predictions use pitcher's
    # avg BF per batter. Here we use actual when available, else estimate.
    expected_pa_col = "pitcher_avg_bf_last_10"
    lineup_slots = 9
    if expected_pa_col in df.columns:
        # Rough per-hitter expectation: pitcher's recent BF / lineup slots
        df["expected_plate_appearances"] = (
            df[expected_pa_col].fillna(25) / lineup_slots
        )
    else:
        df["expected_plate_appearances"] = np.nan

    # Use actual PAs where known, else expected
    df["effective_pa_for_scaling"] = np.where(
        df[config.LABEL_PA_COL].notna(),
        df[config.LABEL_PA_COL],
        df["expected_plate_appearances"],
    )

    df["predicted_k_count_per_matchup"] = (
        df["predicted_k_rate"] * df["effective_pa_for_scaling"]
    )

    # Prediction confidence tiers
    def confidence_tier(row):
        if pd.isna(row.get("h2h_career_pa")):
            h2h_pa = 0
        else:
            h2h_pa = row["h2h_career_pa"]
        std = row["prediction_std"]
        if h2h_pa >= 15 and std < 0.05:
            return "high"
        elif h2h_pa >= 5 or std < 0.07:
            return "medium"
        else:
            return "low"

    df["prediction_confidence_tier"] = df.apply(confidence_tier, axis=1)
    df["is_backtestable"] = df[config.LABEL_COUNT_COL].notna().astype(int)

    # Columns to keep in the export
    keep_cols = [
        # Identity
        "gamePk", "game_date", "season",
        "hitter_id", "hitter_name", "hitter_team_name", "hitter_position",
        "pitcher_id", "pitcher_name", "pitcher_team_name",
        # Context
        "pitcher_is_starter", "hitter_batting_order",
        "hitter_lineup_position",
        # Predictions
        "predicted_k_rate", "prediction_std",
        "predicted_k_count_per_matchup",
        "expected_plate_appearances",
        "prediction_confidence_tier",
        "is_backtestable",
        # Actuals
        "hitter_plate_appearances", "hitter_strikeouts",
        "hitter_walks", "hitter_hits", "hitter_home_runs",
        # Key features for post-hoc analysis
        "hitter_weighted_k_rate_last_10",
        "hitter_avg_whiff_rate_last_10",
        "hitter_weighted_ops_last_10",
        "pitcher_weighted_k_per_bf_last_10",
        "pitcher_weighted_whiff_rate_last_10",
        "pitcher_avg_pitches_last_10",
        "pitcher_avg_bf_last_10",
        "pitcher_avg_ip_last_10",
        "pitcher_weighted_k9_last_10",
        # H2H
        "h2h_career_pa", "h2h_career_k", "h2h_career_k_rate",
    ]

    existing_keep = [c for c in keep_cols if c in df.columns]
    export_df = df[existing_keep].copy()

    # Rename actuals for clarity in the output table
    rename_map = {
        "hitter_strikeouts": "actual_k_count",
        "hitter_plate_appearances": "actual_plate_appearances",
        "hitter_walks": "actual_walks",
        "hitter_hits": "actual_hits",
        "hitter_home_runs": "actual_home_runs",
    }
    export_df = export_df.rename(columns={
        k: v for k, v in rename_map.items() if k in export_df.columns
    })

    # Derive actual K rate from actuals
    if "actual_k_count" in export_df.columns and "actual_plate_appearances" in export_df.columns:
        export_df["actual_k_rate"] = (
            export_df["actual_k_count"] / export_df["actual_plate_appearances"]
        )
        # Signed prediction error for post-hoc analysis
        export_df["prediction_error"] = (
            export_df["predicted_k_rate"] - export_df["actual_k_rate"]
        )

    # Add pitcher-level aggregated prediction
    # Sum of predicted K counts across all hitters he faces in this game
    if {"gamePk", "pitcher_id", "predicted_k_count_per_matchup"}.issubset(export_df.columns):
        pitcher_game_totals = (
            export_df.groupby(["gamePk", "pitcher_id"])["predicted_k_count_per_matchup"]
            .sum()
            .rename("predicted_pitcher_total_ks")
            .reset_index()
        )
        export_df = export_df.merge(
            pitcher_game_totals, on=["gamePk", "pitcher_id"], how="left"
        )

    # Also add actual pitcher total Ks for backtesting
    if "actual_k_count" in export_df.columns:
        pitcher_actual_totals = (
            export_df.groupby(["gamePk", "pitcher_id"])["actual_k_count"]
            .sum()
            .rename("actual_pitcher_total_ks")
            .reset_index()
        )
        export_df = export_df.merge(
            pitcher_actual_totals, on=["gamePk", "pitcher_id"], how="left"
        )

    logger.info(f"Built predictions dataframe: {len(export_df):,} rows, "
                f"{len(export_df.columns)} columns")
    return export_df


def write_predictions_to_sql(df: pd.DataFrame, table_name: str = None) -> None:
    """
    Drop and recreate the predictions table in SQL Server, then insert rows.

    If your `sql_loader` provides a DataFrame writer, use it. Otherwise
    we generate CREATE TABLE + INSERT statements and batch them.
    """
    table_name = table_name or config.PREDICTION_TABLE

    logger.info(f"Writing {len(df):,} predictions to {table_name}")

    if write_dataframe is not None:
        # Preferred path: use the project's native bulk writer
        execute_sql(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};")
        write_dataframe(df, table_name, if_exists="replace")
        logger.info(f"Wrote predictions via write_dataframe")
        return

    # Fallback: manual CREATE + INSERT in batches
    logger.warning(
        "sql.sql_loader.write_dataframe not available; writing via manual "
        "CREATE TABLE + batched INSERT. This will be slow for large tables."
    )
    _manual_write(df, table_name)


def _sql_type(dtype) -> str:
    """Map pandas dtype to SQL Server type."""
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME2"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BIT"
    else:
        return "NVARCHAR(255)"


def _manual_write(df: pd.DataFrame, table_name: str, batch_size: int = 500) -> None:
    cols_sql = ",\n    ".join(
        f"[{c}] {_sql_type(df[c].dtype)}" for c in df.columns
    )
    create_sql = f"""
    IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};
    CREATE TABLE {table_name} (
        {cols_sql}
    );
    """
    execute_sql(create_sql)

    col_names = ", ".join(f"[{c}]" for c in df.columns)
    placeholders = ", ".join(["?"] * len(df.columns))
    insert_sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"

    # Batched insert — requires a lower-level cursor. Your sql_loader needs
    # to support this. If not, use write_dataframe path above.
    try:
        from sql.sql_loader import insert_many
        rows = df.where(df.notna(), None).values.tolist()
        for i in range(0, len(rows), batch_size):
            insert_many(insert_sql, rows[i:i + batch_size])
            if (i // batch_size) % 10 == 0:
                logger.info(f"  Inserted {i + batch_size:,}/{len(rows):,} rows")
    except ImportError:
        raise RuntimeError(
            "Neither write_dataframe nor insert_many is available in "
            "sql.sql_loader. Please add a bulk writer to your sql_loader "
            "module, or export to CSV and use bcp / Azure Data Studio import."
        )
    logger.info(f"Wrote {len(df):,} rows to {table_name}")
