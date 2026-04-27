"""
Data loader: pulls feature data from SQL Server using the project's
existing sql.sql_loader module, with local parquet caching so repeated
runs don't hammer the database.

Usage:
    from model_1_k_probability.data_loader import load_matchup_data
    df = load_matchup_data(seasons=[2023, 2024, 2025])
"""
import logging
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from model_1_k_probability import config

# Project uses this pattern elsewhere in the codebase
try:
    from sql.sql_loader import execute_query
except ImportError:
    # Fallback names — adjust if your sql_loader uses different function names
    try:
        from sql.sql_loader import read_sql as execute_query
    except ImportError:
        from sql.sql_loader import run_query as execute_query

logger = logging.getLogger(__name__)


def _cache_path(seasons: Iterable[int]) -> Path:
    tag = "_".join(str(s) for s in sorted(seasons))
    return config.CACHE_DIR / f"matchup_features_seasons_{tag}.parquet"


def load_matchup_data(
    seasons: Iterable[int],
    use_cache: bool = True,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """
    Load matchup features from SQL Server for the given seasons.

    Parameters
    ----------
    seasons : iterable of int
        Which seasons to pull (e.g. [2023, 2024]).
    use_cache : bool
        If True, cache to parquet and reuse on next call.
    force_refresh : bool
        If True, ignore cache and re-pull from SQL Server.

    Returns
    -------
    pd.DataFrame
    """
    seasons = list(seasons)
    cache_file = _cache_path(seasons)

    if use_cache and cache_file.exists() and not force_refresh:
        logger.info(f"Loading cached matchup data from {cache_file}")
        df = pd.read_parquet(cache_file)
        logger.info(f"Loaded {len(df):,} rows from cache")
        return df

    season_list = ",".join(str(s) for s in seasons)
    query = f"""
    SELECT *
    FROM {config.SOURCE_TABLE}
    WHERE season IN ({season_list})
    """

    logger.info(f"Pulling matchup features from SQL Server for seasons {seasons}")
    logger.info(f"Query: {query}")

    df = execute_query(query)

    logger.info(f"Pulled {len(df):,} rows, {len(df.columns)} columns")

    if use_cache:
        logger.info(f"Caching to {cache_file}")
        df.to_parquet(cache_file, index=False)

    return df


def summarize_data(df: pd.DataFrame) -> None:
    """Log a quick summary of the loaded data for sanity checking."""
    logger.info("=" * 70)
    logger.info("DATA SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Total rows: {len(df):,}")
    logger.info(f"Total columns: {len(df.columns)}")

    if "season" in df.columns:
        logger.info(f"Seasons present: {sorted(df['season'].unique().tolist())}")
        season_counts = df.groupby("season").size()
        for season, count in season_counts.items():
            logger.info(f"  Season {season}: {count:,} matchups")

    if config.LABEL_PA_COL in df.columns:
        logger.info(f"Total PAs across dataset: {df[config.LABEL_PA_COL].sum():,.0f}")
    if config.LABEL_COUNT_COL in df.columns:
        logger.info(f"Total strikeouts: {df[config.LABEL_COUNT_COL].sum():,.0f}")
    if config.TARGET_COL in df.columns:
        overall_k_rate = (
            df[config.LABEL_COUNT_COL].sum() / df[config.LABEL_PA_COL].sum()
        )
        logger.info(f"Overall K rate (weighted): {overall_k_rate:.4f}")
    logger.info("=" * 70)
