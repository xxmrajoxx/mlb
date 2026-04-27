"""
Data loading for the strikeout model.

Reads the matchup feature table from SQL Server, does light cleaning,
and returns a DataFrame ready for feature engineering.
"""

import logging
import pandas as pd
from sqlalchemy import text

from sql_loader import get_engine
import config

logger = logging.getLogger(__name__)


def load_matchup_data(seasons: list[int] | None = None) -> pd.DataFrame:
    """
    Load matchup feature rows from SQL Server.

    Parameters
    ----------
    seasons : list[int] | None
        If provided, restricts the query to these seasons. Otherwise loads all.

    Returns
    -------
    pd.DataFrame
    """
    engine = get_engine()

    if seasons:
        season_list = ",".join(str(s) for s in seasons)
        query = f"""
            SELECT *
            FROM {config.SOURCE_TABLE}
            WHERE season IN ({season_list})
        """
    else:
        query = f"SELECT * FROM {config.SOURCE_TABLE}"

    logger.info(f"Loading matchup data: {query.strip()}")
    df = pd.read_sql(text(query), engine)
    logger.info(f"Loaded {len(df):,} rows, {df.shape[1]} columns")

    # The source data uses literal 'NULL' strings in some text columns -
    # convert them back to real NaN so pandas treats them properly.
    df = df.replace({"NULL": pd.NA})

    # Force key columns to the right dtype.
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    df["season"] = df["season"].astype(int)

    # Coerce the target. y_k_rate per PA is 0 or 1 (or fractional if a row
    # represents more than one PA, which it does in some game-level rows).
    df[config.TARGET_COL] = pd.to_numeric(df[config.TARGET_COL], errors="coerce")

    # Drop rows where we have no target (can't train on them).
    before = len(df)
    df = df.dropna(subset=[config.TARGET_COL])
    logger.info(f"Dropped {before - len(df):,} rows with null target")

    return df


def split_by_season(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Split into train / validation / test by season.

    No random shuffling - we want the model to be evaluated on *future* games
    relative to its training data, which is how it'll be used in real life.
    """
    train = df[df["season"].isin(config.TRAIN_SEASONS)].copy()
    val = df[df["season"] == config.VALIDATION_SEASON].copy()
    test = df[df["season"] == config.TEST_SEASON].copy()

    logger.info(
        f"Split sizes - train: {len(train):,}  "
        f"val: {len(val):,}  test: {len(test):,}"
    )
    return train, val, test
